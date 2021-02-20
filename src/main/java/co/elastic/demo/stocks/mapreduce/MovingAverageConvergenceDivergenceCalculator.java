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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.AbstractMap.SimpleImmutableEntry;

/**
 * Calculates the Moving Average Convergence Divergence (MACD) technical indicator.
 *
 * Use the wrapper script ./bin/calculate-macd.sh to run.
 */
public class MovingAverageConvergenceDivergenceCalculator extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        int result = ToolRunner.run(new Configuration(), new MovingAverageConvergenceDivergenceCalculator(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Options options = new Options();
        options.addOption("sd", "start-date",          true, "start date yyyy-mm-dd");
        options.addOption("ed", "end-date",            true, "end date yyyy-mm-dd");
        options.addOption("si", "source-index",        true, "source elasticsearch index");
        options.addOption("di", "destination-index",   true, "destination elasticsearch index");
        options.addOption("eh", "elasticsearch-hosts", true, "host:port pairs for elasticsearch");
        options.addOption("ss", "symbols",             true, "comma-separated list of stock symbols");

        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cli = parser.parse(options, args);

            if (!cli.hasOption("sd") || !cli.hasOption("ed") || !cli.hasOption("si") ||
                !cli.hasOption("di") || !cli.hasOption("eh") || !cli.hasOption("ss")) {
                formatter.printHelp("MovingAverageConvergenceDivergenceCalculator", options);
                System.exit(1);
            }

            Configuration config = this.getConf();

             // Speculative execution should be turned off when reading/writing to Elasticsearch.
            config.setBoolean("mapred.map.tasks.speculative.execution", false);
            config.setBoolean("mapred.reduce.tasks.speculative.execution", false);

            // Set the Elasticsearch nodes
            config.set("es.nodes", cli.getOptionValue("eh"));

            // Configure the indices for read/write
            config.set("es.resource.read", cli.getOptionValue("si"));
            config.set("es.resource.write", cli.getOptionValue("di"));

            // Provide the query DSL
            config.set("es.query", query(cli.getOptionValue("sd"), cli.getOptionValue("ed"), cli.getOptionValue("ss")));

            // No need to pull the entire doc from Elasticsearch, just the fields we need
            config.set("es.read.source.filter", "symbol, date, adjusted_close");

            // Configure the Hadoop connector to update existing records based on document id
            config.set("es.write.operation", "update");
            config.set("es.mapping.id", "docid");

            // Configure job
            Job job = Job.getInstance(config, "moving-average-convergence-divergence");
            job.setJarByClass(MovingAverageConvergenceDivergenceCalculator.class);
            job.setMapperClass(MovingAverageConvergenceDivergenceCalculatorMapper.class);
            job.setMapOutputKeyClass(SymbolDateKey.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setPartitionerClass(MovingAverageConvergenceDivergenceCalculatorPartitioner.class);
            job.setReducerClass(MovingAverageConvergenceDivergenceCalculatorReducer.class);
            job.setInputFormatClass(EsInputFormat.class);
            job.setOutputFormatClass(EsOutputFormat.class);

            job.waitForCompletion(true);
        }
        catch (ParseException e) {
            formatter.printHelp("MovingAverageConvergenceDivergenceCalculator", options);
            System.exit(1);
        }

        return 0;
    }

    private static String query(String start, String end, String symbols)
    {
        String q = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": [\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"date\": {\n" +
                "              \"gte\": \"" + start + "\",\n" +
                "              \"lte\": \"" + end + "\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"terms\" : {\n" +
                "            \"symbol\" : [%s]\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String _symbols = Arrays.stream(symbols.split(",")).collect(Collectors.joining("\",\"", "\"", "\""));
        return String.format(q, _symbols);
    }

    private static class MovingAverageConvergenceDivergenceCalculatorPartitioner extends Partitioner<SymbolDateKey, NullWritable>
    {
        @Override
        public int getPartition(SymbolDateKey symbolDateKey, NullWritable nullWritable, int numPartitions)
        {
            return symbolDateKey.symbol().hashCode() % numPartitions;
        }
    }

    private static class MovingAverageConvergenceDivergenceCalculatorMapper extends Mapper<Text, MapWritable, SymbolDateKey, DoubleWritable>
    {
        private static final Text symbol = new Text("symbol");
        private static final Text date   = new Text("date");
        private static final Text close  = new Text("adjusted_close");

        @Override
        protected void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException
        {
            SymbolDateKey sdk = new SymbolDateKey(value.get(symbol).toString(), value.get(date).toString(), key.toString());
            double _close = Double.parseDouble(value.get(close).toString());
            context.write(sdk, new DoubleWritable(_close));
        }
    }

    private static class MovingAverageConvergenceDivergenceCalculatorReducer extends Reducer<SymbolDateKey, DoubleWritable, NullWritable, MapWritable>
    {
        private static final Logger logger = Logger.getLogger(MovingAverageConvergenceDivergenceCalculatorReducer.class);

        private final static MathContext MATH_CONTEXT         = new MathContext(6, RoundingMode.HALF_UP);
        private static final double NINE_DAY_EMA_WEIGHT       = weight(9);
        private static final double TWELVE_DAY_EMA_WEIGHT     = weight(12);
        private static final double TWENTY_SIX_DAY_EMA_WEIGHT = weight(26);

        private static String currentSymbol = null;
        private static ArrayList<SimpleImmutableEntry<String, Double>> prices = new ArrayList<>();

        @Override
        protected void reduce(SymbolDateKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            if (symbolChanged(key.symbol().toString()))
            {
                logger.info("Processing ticker symbol: " + key.symbol().toString());

                for (MACD macd : calculate(currentSymbol, prices))
                {
                    MapWritable doc = new MapWritable();

                    doc.put(new Text("docid"),                   new Text(macd.docid));
                    doc.put(new Text("macd.twelve_day_ema"),     new DoubleWritable(macd.twelveDayEMA));
                    doc.put(new Text("macd.twenty_six_day_ema"), new DoubleWritable(macd.twentySixDayEMA));
                    doc.put(new Text("macd.macd"),               new DoubleWritable(macd.macd));
                    doc.put(new Text("macd.signal"),             new DoubleWritable(macd.nineDayEMA));
                    doc.put(new Text("macd.histogram"),          new DoubleWritable(macd.histogram));

                    // Update the document in Elasticsearch with the newly added fields.
                    context.write(NullWritable.get(), doc);
                }

                prices.clear();
                currentSymbol = key.symbol().toString();
            }

            prices.add(new SimpleImmutableEntry<>(key.docid().toString(), values.iterator().next().get()));
        }

        private static List<MACD> calculate(String symbol, List<SimpleImmutableEntry<String, Double>> prices)
        {
            ArrayList<MACD> results = new ArrayList<>();

            // Don't bother if we have less than 45 days' of closing prices.
            if (symbol == null || prices.size() < 45) {
                return results;
            }

            // Bootstrap EMA's w/simple moving averages
            double twelveDayEMA = sma(prices.subList(0, 12)
                    .stream().map(SimpleImmutableEntry::getValue).collect(Collectors.toList()));
            double twentySixDayEMA = sma(prices.subList(0, 26)
                    .stream().map(SimpleImmutableEntry::getValue).collect(Collectors.toList()));

            double nineDayEMA = 0.0;
            ArrayList<Double> macd = new ArrayList<>();

            for (int i = 0; i < prices.size(); i++) {

                String docid = prices.get(i).getKey();
                Double closingPrice = prices.get(i).getValue();

                if (i > 11) {
                    twelveDayEMA = ema(closingPrice, twelveDayEMA, TWELVE_DAY_EMA_WEIGHT);
                }

                if (i == 25) {
                    macd.add(twelveDayEMA - twentySixDayEMA);
                }

                if (i > 25) {
                    twentySixDayEMA = ema(closingPrice, twentySixDayEMA, TWENTY_SIX_DAY_EMA_WEIGHT);
                    macd.add(twelveDayEMA - twentySixDayEMA);

                    if (macd.size() == 9) {
                        nineDayEMA = sma(macd.subList(0, 9));
                    }
                    else if (macd.size() > 9) {
                        nineDayEMA = ema(macd.get(macd.size() - 1), nineDayEMA, NINE_DAY_EMA_WEIGHT);
                    }

                    double _macd = macd.get(macd.size() - 1);
                    results.add(new MACD(docid, twelveDayEMA, twentySixDayEMA, _macd, nineDayEMA, _macd - nineDayEMA));

                    logger.debug(
                            symbol +
                                    ", " + closingPrice +
                                    ", " + String.format("%.3f", twelveDayEMA) +
                                    ", " + String.format("%.3f", twentySixDayEMA) +
                                    ", " + String.format("%.3f", _macd) +              // 12 day - 26 day
                                    ", " + String.format("%.3f", nineDayEMA) +         // macd signal
                                    ", " + String.format("%.3f", _macd - nineDayEMA)   // macd histogram
                    );
                }
            }

            return results;
        }

        private static double ema(double closingPrice, double ema, double weight)
        {
            return (closingPrice - ema) * weight + ema;
        }

        private static double sma(List<Double> prices)
        {
            double sum = 0.0;
            for (Double price : prices) {
                sum += price;
            }
            return (new BigDecimal(sum).divide(new BigDecimal(prices.size()), MATH_CONTEXT)).doubleValue();
        }

        private static boolean symbolChanged(String symbol)
        {
            return !symbol.equalsIgnoreCase(currentSymbol);
        }

        private static double weight(int periods)
        {
            return new BigDecimal(2).divide(new BigDecimal(periods + 1), MATH_CONTEXT).doubleValue();
        }

        private static class MACD
        {
            final String docid;
            final double twelveDayEMA;
            final double twentySixDayEMA;
            final double macd;
            final double nineDayEMA;
            final double histogram;

            MACD(String docid, double twelveDayEMA, double twentySixDayEMA, double macd, double nineDayEMA, double histogram)
            {
                this.docid           = docid;
                this.twelveDayEMA    = twelveDayEMA;
                this.twentySixDayEMA = twentySixDayEMA;
                this.macd            = macd;
                this.nineDayEMA      = nineDayEMA;
                this.histogram       = histogram;
            }
        }
    }
}
