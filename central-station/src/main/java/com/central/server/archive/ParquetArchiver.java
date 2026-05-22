package com.central.server.archive;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.client.RestClient;

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParquetArchiver {

    private static final int BATCH_SIZE = 10000;
    private static final String BASE_DIR = "data/parquet";
    private static final String ES_INDEX  = "weather-data";
    private final ExecutorService esExecutor = Executors.newSingleThreadExecutor();
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Schema schema;
    private final List<GenericRecord> buffer = new ArrayList<>();

    // Elasticsearch client — created once, reused for every batch
    private final ElasticsearchClient esClient;
    private final RestClient restClient;

    // track last s_no per station for dropped message detection
    private final Map<Long, Long> lastSno = new HashMap<>();

    public ParquetArchiver() throws Exception {
        // load avro schema
        InputStream schemaStream = getClass()
                .getClassLoader()
                .getResourceAsStream("weather_message.avsc");
        this.schema = new Schema.Parser().parse(schemaStream);
        new java.io.File(BASE_DIR).mkdirs();

        // connect to Elasticsearch once at startup
        this.restClient = RestClient.builder(
                new HttpHost("elasticsearch", 9200)
        ).build();

        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );
        this.esClient = new ElasticsearchClient(transport);

        System.out.println("Connected to Elasticsearch at localhost:9200");
    }

    public synchronized void add(String jsonMessage) throws Exception {
        JsonNode node = mapper.readTree(jsonMessage);

        long stationId = node.path("station_id").asLong();

        GenericRecord record = new GenericData.Record(schema);
        record.put("station_id",       stationId);
        record.put("s_no",             node.path("s_no").asLong());
        record.put("battery_status",   node.path("battery_status").asText());
        record.put("status_timestamp", node.path("status_timestamp").asLong());
        record.put("humidity",         node.path("weather").path("humidity").asInt());
        record.put("temperature",      node.path("weather").path("temperature").asInt());
        record.put("wind_speed",       node.path("weather").path("wind_speed").asInt());

        buffer.add(record);

        if (buffer.size() >= BATCH_SIZE) {
            flushAll();
        }
    }

    private void flushAll() throws Exception {
        if (buffer.isEmpty()) return;

        // group by day + station_id to handle midnight crossing
        Map<String, List<GenericRecord>> groups = new HashMap<>();

        for (GenericRecord record : buffer) {
            long stationId = (long) record.get("station_id");
            long timestamp = (long) record.get("status_timestamp");
            ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);

            String key = String.format("%d/%02d/%02d/%d",
                    dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), stationId);

            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
        }

        // write one parquet file per group + index to Elasticsearch
        for (Map.Entry<String, List<GenericRecord>> entry : groups.entrySet()) {
            List<GenericRecord> groupRecords = entry.getValue();

            long timestamp = (long) groupRecords.get(0).get("status_timestamp");
            long stationId = (long) groupRecords.get(0).get("station_id");

            // 1. write parquet file
            String filePath = buildFilePath(timestamp, stationId);
            new java.io.File(filePath).getParentFile().mkdirs();

            Path path = new Path(filePath);
            Configuration conf = new Configuration();

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(path)
                    .withSchema(schema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
                for (GenericRecord r : groupRecords) {
                    writer.write(r);
                }
            }

            System.out.println("Wrote " + groupRecords.size() +
                    " records to: " + filePath);

            // async - doesn't block
            final List<GenericRecord> recordsToIndex = new ArrayList<>(groupRecords);
            indexRecords(recordsToIndex);
        }

        buffer.clear();
    }

    private void indexRecords(List<GenericRecord> records) {
        int indexed = 0;
        for (GenericRecord record : records) {
            try {
                long stationId = (long) record.get("station_id");
                long sNo       = (long) record.get("s_no");

                // detect dropped messages via s_no gaps
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

                // unique ID = no duplicates on restart
                String docId = stationId + "_" + sNo;
                esClient.index(i -> i
                        .index(ES_INDEX)
                        .id(docId)
                        .document(doc)
                );

                indexed++;
            } catch (Exception e) {
                System.err.println("Failed to index record: " + e.getMessage());
            }
        }
        System.out.println("Indexed " + indexed + " records to Elasticsearch");
    }

    public synchronized void flush() throws Exception {
        flushAll();
    }

    public void close() throws Exception {
        esExecutor.shutdown();
        esExecutor.awaitTermination(10, TimeUnit.SECONDS); // wait for pending indexing
        restClient.close();
        System.out.println("Elasticsearch connection closed");
    }

    private String buildFilePath(long unixTimestamp, long stationId) {
        ZonedDateTime dt = Instant.ofEpochSecond(unixTimestamp)
                .atZone(ZoneOffset.UTC);

        String partition = String.format(
                "%s/year=%d/month=%02d/day=%02d/station_id=%d",
                BASE_DIR,
                dt.getYear(),
                dt.getMonthValue(),
                dt.getDayOfMonth(),
                stationId
        );

        String filename = "batch_" + System.currentTimeMillis() + ".parquet";
        return partition + "/" + filename;
    }
}