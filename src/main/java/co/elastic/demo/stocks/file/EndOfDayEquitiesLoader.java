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
package co.elastic.demo.stocks.file;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import org.apache.http.HttpHost;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Loads end-of-day stock prices from a file into elasticsearch.
 */
public class EndOfDayEquitiesLoader
{

    public static void main(String[] args)
    {
        Options options = new Options();
        options.addOption("ef", "equities-file",       true,  "path to equities file");
        options.addOption("eh", "elasticsearch-host",  true,  "elasticsearch host");
        options.addOption("ei", "elasticsearch-index", true,  "elasticsearch index");
        options.addOption("ci", "create-index",        false, "create index");

        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cli = parser.parse(options, args);

            if (!cli.hasOption("ef") || !cli.hasOption("eh") || !cli.hasOption("ei")) {
                formatter.printHelp("EndOfDayEquitiesLoader", options);
                System.exit(1);
            }

            BulkProcessor processor = init(cli.getOptionValue("eh"), cli.getOptionValue("ei"), cli.hasOption("ci"));
            load(cli.getOptionValue("ef"), cli.getOptionValue("ei"), processor);

            processor.awaitClose(10, TimeUnit.SECONDS);
            System.out.println("Processor closed. Quitting.");
            System.exit(0);
        }
        catch (ParseException e) {
            formatter.printHelp("EndOfDayEquitiesLoader", options);
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace(System.err);
        }
    }

    private static void load(String file, String index, BulkProcessor processor) throws IOException
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(file)))
        {
            for (String line = reader.readLine(); line != null; line = reader.readLine())
            {
                String[] splits = line.split(",");

                IndexRequest request = new IndexRequest(index).source(
                        XContentType.JSON,
                        "symbol",            splits[0],
                        "date",              splits[1],
                        "unadjusted_open",   splits[2],
                        "unadjusted_high",   splits[3],
                        "unadjusted_low",    splits[4],
                        "unadjusted_close",  splits[5],
                        "unadjusted_volume", splits[6],
                        "dividends",         splits[7],
                        "splits",            splits[8],
                        "adjusted_open",     splits[9],
                        "adjusted_high",     splits[10],
                        "adjusted_low",      splits[11],
                        "adjusted_close",    splits[12],
                        "adjusted_volume",   splits[13]
                );

                processor.add(request);
            }
        }
    }

    private static void mapping(RestHighLevelClient client, String index) throws IOException
    {
        CreateIndexRequest create = new CreateIndexRequest(index);
        create.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
        );
        client.indices().create(create, RequestOptions.DEFAULT);

        String json = "{\n" +
                "    \"properties\": {\n" +
                "      \"symbol\":            { \"type\": \"keyword\" },\n" +
                "      \"date\":              { \"type\": \"date\" },\n" +
                "      \"unadjusted_open\":   { \"type\": \"float\" },\n" +
                "      \"unadjusted_high\":   { \"type\": \"float\" },\n" +
                "      \"unadjusted_low\":    { \"type\": \"float\" },\n" +
                "      \"unadjusted_close\":  { \"type\": \"float\" },\n" +
                "      \"unadjusted_volume\": { \"type\": \"float\" },\n" +
                "      \"dividends\":         { \"type\": \"float\" },\n" +
                "      \"splits\":            { \"type\": \"float\" },\n" +
                "      \"adjusted_open\":     { \"type\": \"float\" },\n" +
                "      \"adjusted_high\":     { \"type\": \"float\" },\n" +
                "      \"adjusted_low\":      { \"type\": \"float\" },\n" +
                "      \"adjusted_close\":    { \"type\": \"float\" },\n" +
                "      \"adjusted_volume\":   { \"type\": \"float\" },\n" +
                "      \"name\":              { \"type\": \"keyword\" },\n" +
                "      \"country\":           { \"type\": \"keyword\" },\n" +
                "      \"ipo_year\":          { \"type\": \"integer\" },\n" +
                "      \"sector\":            { \"type\": \"keyword\" },\n" +
                "      \"industry\":          { \"type\": \"keyword\" }\n" +
                "    }\n" +
                "}\n";

        PutMappingRequest request = new PutMappingRequest(index);
        request.source(json, XContentType.JSON);
        client.indices().putMapping(request, RequestOptions.DEFAULT);
    }

    private static BulkProcessor init(String host, String index, boolean create) throws IOException
    {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, 9200)));

        if (create) {
            mapping(client, index);
        }

        BulkProcessor.Listener listener = new BulkProcessor.Listener()
        {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("Submitting bulk request of " + request.numberOfActions() + " items");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    System.err.println("Bulk failures: " + response.buildFailureMessage());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failure.printStackTrace(System.err);
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener);

        builder.setBulkActions(5000);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(3);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(2), 5));

        return builder.build();
    }
}











