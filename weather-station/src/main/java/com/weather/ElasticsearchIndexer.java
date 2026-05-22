package com.weather;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.elasticsearch.client.RestClient;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchIndexer {

    private static final String INDEX_NAME = "weather-data";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // 1. connect to Elasticsearch
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)
        ).build();

        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );

        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        // 2. load avro schema once
        InputStream schemaStream = ElasticsearchIndexer.class
                .getClassLoader()
                .getResourceAsStream("weather_message.avsc");
        Schema schema = new Schema.Parser().parse(schemaStream);

        // 3. find all parquet files under data/parquet/
        List<File> parquetFiles = findParquetFiles(new File("data/parquet"));

        // sort by path = chronological order per station
        // (dir.listFiles() has no guaranteed order)
        parquetFiles.sort((a, b) -> a.getAbsolutePath().compareTo(b.getAbsolutePath()));

        System.out.println("Found " + parquetFiles.size() + " parquet files");

        // lastSno shared across ALL files so dropped detection
        // is continuous across file boundaries
        Map<Long, Long> lastSno = new HashMap<>();

        // 4. read each file and index its records
        int totalIndexed = 0;
        for (File file : parquetFiles) {
            totalIndexed += indexFile(esClient, file, lastSno, schema); // ← schema passed in
        }

        System.out.println("Done! Indexed " + totalIndexed + " records total.");
        restClient.close();
    }

    private static int indexFile(ElasticsearchClient esClient,
                                 File file,
                                 Map<Long, Long> lastSno,
                                 Schema schema) throws Exception {
        System.out.println("Indexing: " + file.getPath());

        Path path = new Path(file.getAbsolutePath());
        Configuration conf = new Configuration();

        // force generic record reading, ignore WeatherMessage specific class
        AvroReadSupport.setAvroReadSchema(conf, schema);
        AvroReadSupport.setRequestedProjection(conf, schema);
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);

        int count = 0;

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(path)
                .withConf(conf)
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {

                long stationId = (long) record.get("station_id");
                long sNo       = (long) record.get("s_no");

                // detect gaps = dropped messages
                long previousSno  = lastSno.getOrDefault(stationId, sNo - 1L);
                long droppedCount = sNo - previousSno - 1;
                lastSno.put(stationId, sNo);

                ObjectNode doc = mapper.createObjectNode();
                doc.put("station_id",       stationId);
                doc.put("s_no",             sNo);
                doc.put("battery_status",   record.get("battery_status").toString());
                doc.put("status_timestamp", (long) record.get("status_timestamp"));
                doc.put("humidity",         (int)  record.get("humidity"));
                doc.put("temperature",      (int)  record.get("temperature"));
                doc.put("wind_speed",       (int)  record.get("wind_speed"));
                doc.put("dropped_before",   droppedCount);

                // unique doc ID = no duplicates on re-run
                String docId = stationId + "_" + sNo;
                esClient.index(i -> i
                        .index(INDEX_NAME)
                        .id(docId)
                        .document(doc)
                );

                count++;
            }
        }

        System.out.println("  Indexed " + count + " records from " + file.getName());
        return count;
    }

    // recursively find all .parquet files
    private static List<File> findParquetFiles(File dir) {
        List<File> result = new ArrayList<>();
        if (!dir.exists()) return result;

        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                result.addAll(findParquetFiles(f));
            } else if (f.getName().endsWith(".parquet")) {
                result.add(f);
            }
        }
        return result;
    }
}