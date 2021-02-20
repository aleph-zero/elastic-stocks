/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.elastic.demo.stocks.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.mr.EsOutputFormat;

/**
 * Reads end-of-day stock prices from HDFS and batch load into Elasticsearch and
 * performs map-side join with stock sector data.
 *
 * Use the wrapper script ./bin/hdfs-load-join.sh to run.
 *
 * Create index mapping prior to running:
 *
 *  PUT stocks/_mapping
 *  {
 *     "properties": {
 *       "symbol":            { "type": "keyword" },
 *       "date":              { "type": "date" },
 *       "unadjusted_open":   { "type": "float" },
 *       "unadjusted_high":   { "type": "float" },
 *       "unadjusted_low":    { "type": "float" },
 *       "unadjusted_close":  { "type": "float" },
 *       "unadjusted_volume": { "type": "float" },
 *       "dividends":         { "type": "float" },
 *       "splits":            { "type": "float" },
 *       "adjusted_open":     { "type": "float" },
 *       "adjusted_high":     { "type": "float" },
 *       "adjusted_low":      { "type": "float" },
 *       "adjusted_close":    { "type": "float" },
 *       "adjusted_volume":   { "type": "float" },
 *       "name":              { "type": "keyword" },
 *       "country":           { "type": "keyword" },
 *       "ipo_year":          { "type": "integer" },
 *       "sector":            { "type": "keyword" },
 *       "industry":          { "type": "keyword" }
 *     }
 *  }
 *
 */
public class EndOfDayEquitiesLoader extends Configured implements Tool
{
    Logger logger = Logger.getLogger(EndOfDayEquitiesLoader.class);

    public static void main(String[] args) throws Exception
    {
        int result = ToolRunner.run(new Configuration(), new EndOfDayEquitiesLoader(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Options options = new Options();
        options.addOption("ef", "equities-file",       true, "path to equities file");
        options.addOption("sf", "sectors-file",        true, "path to sectors file");
        options.addOption("eh", "elasticsearch-hosts", true, "host:port pairs for elasticsearch");
        options.addOption("ei", "elasticsearch-index", true, "elasticsearch index");

        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cli = parser.parse(options, args);

            if (!cli.hasOption("ef") || !cli.hasOption("eh") || !cli.hasOption("ei") || !cli.hasOption("sf")) {
                formatter.printHelp("EndOfDayEquitiesLoader", options);
                System.exit(1);
            }

            Configuration config = this.getConf();

            // Speculative execution should be turned off when writing to Elasticsearch
            config.setBoolean("mapred.map.tasks.speculative.execution", false);
            config.setBoolean("mapred.reduce.tasks.speculative.execution", false);

            // Set the Elasticsearch node(s) and index to which we will write
            config.set("es.nodes", cli.getOptionValue("eh"));
            config.set("es.resource", cli.getOptionValue("ei"));

            // Load the stock sectors data file into the distributed cache so that we
            // can perform map-side with the stock prices
            DistributedCache.addCacheFile(
                    new URI(cli.getOptionValue("sf") + "#stock-sectors.csv"), config);

            Job job = Job.getInstance(config, "stocks-data-loader");

            job.setJarByClass(EndOfDayEquitiesLoader.class);
            job.setMapperClass(EquityDataLoaderMapper.class);
            job.setOutputFormatClass(EsOutputFormat.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(MapWritable.class);

            logger.info("Loading data file: " + cli.getOptionValue("ef"));
            FileInputFormat.setInputPaths(job, new Path(cli.getOptionValue("ef")));

            job.waitForCompletion(true);
        }
        catch (ParseException e) {
            formatter.printHelp("EndOfDayEquitiesLoader", options);
            System.exit(1);
        }

        return 0;
    }

    private static class EquityDataLoaderMapper extends Mapper<Object, Text, NullWritable, MapWritable>
    {
        private final Map<String, List<String>> sectors = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            // Build a map of [symbol -> sector] that we will use for our join
            // Sector data is organized as:
            //      symbol, name, last sale, net change, % change, market cap, country, IPO year, volume, sector, industry
            try (BufferedReader reader = new BufferedReader(new FileReader("./stock-sectors.csv")))
            {
                for (String line = reader.readLine(); line != null; line = reader.readLine())
                {
                    String[] split = line.split(",");
                    ArrayList<String> elements = new ArrayList<>(Arrays.asList(split));

                    if (split[0].equalsIgnoreCase("symbol")) {
                        continue; // skip header line
                    }

                    // sanity check for malformed line
                    if (elements.size() == 11) {
                        sectors.put(elements.get(0), elements);
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            MapWritable doc = new MapWritable();

            String[] split = value.toString().split(",");

            doc.put(new Text("symbol"),             new Text(split[0].trim()));
            doc.put(new Text("date"),               new Text(split[1].trim()));
            doc.put(new Text("unadjusted_open"),    new FloatWritable(getFloatValue(split[2])));
            doc.put(new Text("unadjusted_high"),    new FloatWritable(getFloatValue(split[3])));
            doc.put(new Text("unadjusted_low"),     new FloatWritable(getFloatValue(split[4])));
            doc.put(new Text("unadjusted_close"),   new FloatWritable(getFloatValue(split[5])));
            doc.put(new Text("unadjusted_volume"),  new FloatWritable(getFloatValue(split[6])));
            doc.put(new Text("dividends"),          new FloatWritable(getFloatValue(split[7])));
            doc.put(new Text("splits"),             new FloatWritable(getFloatValue(split[8])));
            doc.put(new Text("adjusted_open"),      new FloatWritable(getFloatValue(split[9])));
            doc.put(new Text("adjusted_high"),      new FloatWritable(getFloatValue(split[10])));
            doc.put(new Text("adjusted_low"),       new FloatWritable(getFloatValue(split[11])));
            doc.put(new Text("adjusted_close"),     new FloatWritable(getFloatValue(split[12])));
            doc.put(new Text("adjusted_volume"),    new FloatWritable(getFloatValue(split[13])));

            // join with stock sector data
            List<String> sector = sectors.get(split[0].trim());
            if (sector != null) {

                doc.put(new Text("name"),     new Text(sector.get(1)));
                doc.put(new Text("ipo_year"), new Text(sector.get(7)));
                doc.put(new Text("country"),  new Text(sector.get(6)));

                String s = sector.get(9);
                if (!s.isEmpty() && !s.equalsIgnoreCase("n/a")) {
                    doc.put(new Text("sector"), new Text(s));
                }

                String i = sector.get(10);
                if (!i.isEmpty() && !i.equalsIgnoreCase("n/a")) {
                    doc.put(new Text("industry"), new Text(i));
                }
            }

            context.write(NullWritable.get(), doc);
        }

        private float getFloatValue(final String value)
        {
            if (value == null || value.isEmpty()) {
                return 0.0f;
            }
            try {
                return Float.parseFloat(value.trim());
            }
            catch (NumberFormatException e) {
                return 0.0f;
            }
        }
    }
}
