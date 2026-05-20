package com.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetArchiver {

    private static final int BATCH_SIZE = 5;
    private static final String BASE_DIR = "data/parquet";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Schema schema;

    // Bug fix 1: one buffer per station_id instead of one shared buffer
    private final Map<Long, List<GenericRecord>> buffers = new HashMap<>();

    public ParquetArchiver() throws Exception {
        InputStream schemaStream = getClass()
                .getClassLoader()
                .getResourceAsStream("weather_message.avsc");
        this.schema = new Schema.Parser().parse(schemaStream);
        new java.io.File(BASE_DIR).mkdirs();
    }

    // called for every message consumed from Kafka
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

        // get or create buffer for this specific station
        buffers.computeIfAbsent(stationId, k -> new ArrayList<>()).add(record);

        // flush only this station's buffer if it reached batch size
        if (buffers.get(stationId).size() >= BATCH_SIZE) {
            flushStation(stationId);
        }
    }

    // flush a specific station's buffer
    private void flushStation(long stationId) throws Exception {
        List<GenericRecord> buffer = buffers.get(stationId);
        if (buffer == null || buffer.isEmpty()) return;

        // Bug fix 2: group records by their own day before writing
        // so midnight-crossing batches go into correct partitions
        Map<String, List<GenericRecord>> byDay = new HashMap<>();

        for (GenericRecord record : buffer) {
            long timestamp = (long) record.get("status_timestamp");
            ZonedDateTime dt = Instant.ofEpochSecond(timestamp)
                    .atZone(ZoneOffset.UTC);

            String dayKey = String.format("%d/%02d/%02d",
                    dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());

            byDay.computeIfAbsent(dayKey, k -> new ArrayList<>()).add(record);
        }

        // write one parquet file per day group
        for (Map.Entry<String, List<GenericRecord>> entry : byDay.entrySet()) {
            List<GenericRecord> dayRecords = entry.getValue();

            // each record uses its OWN timestamp for the path
            long timestamp = (long) dayRecords.get(0).get("status_timestamp");
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

                for (GenericRecord record : dayRecords) {
                    writer.write(record);
                }
            }

            System.out.println("Wrote " + dayRecords.size() +
                    " records to: " + filePath);
        }

        buffer.clear();
    }

    // flush ALL stations' buffers (called on shutdown)
    public synchronized void flush() throws Exception {
        for (long stationId : buffers.keySet()) {
            flushStation(stationId);
        }
    }

    // partition path: data/parquet/year=.../month=.../day=.../station_id=.../
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