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
import java.util.List;

public class ParquetArchiver {

    private static final int BATCH_SIZE = 10_000;
    private static final String BASE_DIR = "data/parquet";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Schema schema;
    // buffer holding messages until batch is full
    private final List<GenericRecord> buffer = new ArrayList<>();

    public ParquetArchiver() throws Exception {
        // load avro schema from resources
        InputStream schemaStream = getClass()
                .getClassLoader()
                .getResourceAsStream("weather_message.avsc");
        this.schema = new Schema.Parser().parse(schemaStream);
        //  loads the Avro schema from resources,
        //  and creates the data/parquet/ directory on disk if it doesn't exist yet.
        new java.io.File(BASE_DIR).mkdirs();
    }

    // called for every message consumed from Kafka
    public void add(String jsonMessage) throws Exception {
        JsonNode node = mapper.readTree(jsonMessage);

        GenericRecord record = new GenericData.Record(schema);
        record.put("station_id",       node.path("station_id").asLong());
        record.put("s_no",             node.path("s_no").asLong());
        record.put("battery_status",   node.path("battery_status").asText());
        record.put("status_timestamp", node.path("status_timestamp").asLong());
        record.put("humidity",         node.path("weather").path("humidity").asInt());
        record.put("temperature",      node.path("weather").path("temperature").asInt());
        record.put("wind_speed",       node.path("weather").path("wind_speed").asInt());

        buffer.add(record);

        // flush when batch is full
        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    // write buffered records to a parquet file
    public void flush() throws Exception {
        if (buffer.isEmpty()) return;

        // use timestamp of first record for partitioning
        long timestamp = (long) buffer.get(0).get("status_timestamp");
        long stationId = (long) buffer.get(0).get("station_id");

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

            for (GenericRecord record : buffer) {
                writer.write(record);
            }
        }

        System.out.println("Wrote " + buffer.size() +
                " records to: " + filePath);
        buffer.clear();
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

        // unique filename per batch using current time
        String filename = "batch_" + System.currentTimeMillis() + ".parquet";
        return partition + "/" + filename;
    }
}