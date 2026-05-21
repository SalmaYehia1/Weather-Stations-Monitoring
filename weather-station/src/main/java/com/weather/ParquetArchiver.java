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

    private static final int BATCH_SIZE = 10_000;
    private static final String BASE_DIR = "data/parquet";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Schema schema;
    private final List<GenericRecord> buffer = new ArrayList<>();

    public ParquetArchiver() throws Exception {
        InputStream schemaStream = getClass()
                .getClassLoader()
                .getResourceAsStream("weather_message.avsc");
        this.schema = new Schema.Parser().parse(schemaStream);
        new java.io.File(BASE_DIR).mkdirs();
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

        // group by day + station_id
        Map<String, List<GenericRecord>> groups = new HashMap<>();

        for (GenericRecord record : buffer) {
            long stationId = (long) record.get("station_id");
            long timestamp = (long) record.get("status_timestamp");
            ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);

            String key = String.format("%d/%02d/%02d/%d",
                    dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), stationId);

            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
        }

        // write one parquet file per group
        for (Map.Entry<String, List<GenericRecord>> entry : groups.entrySet()) {
            List<GenericRecord> groupRecords = entry.getValue();

            long timestamp = (long) groupRecords.get(0).get("status_timestamp");
            long stationId = (long) groupRecords.get(0).get("station_id");

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

            System.out.println("Wrote " + groupRecords.size() + " records to: " + filePath);
        }

        buffer.clear();
    }

    public synchronized void flush() throws Exception {
        flushAll();
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