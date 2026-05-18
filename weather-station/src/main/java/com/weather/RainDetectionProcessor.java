package com.weather;
// kafka streams lets you write a program that reads from a Kafka topic,
// processes/transforms the data, and writes results to another topic
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class RainDetectionProcessor {

    static final String INPUT_TOPIC  = "weather-topic";
    static final String OUTPUT_TOPIC = "rain-alerts";

    // one shared Jackson JSON parser
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        // Configuration for the Kafka Streams app
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "rain-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // (Serializer/Deserializer) — Kafka sends everything as raw bytes.
        // You're telling it "both keys and values are Strings",
        // so it knows how to convert bytes ↔ String automatically
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Start reading from the weather-data topic
        builder.<String, String>stream(INPUT_TOPIC)
                .filter((key, value) -> {
                    try {
                        JsonNode msg = mapper.readTree(value);
                        int humidity = msg.path("weather").path("humidity").asInt(-1);
                        return humidity > 70; // only messages where humidity exceeds 70 pass through; the rest are dropped
                    } catch (Exception e) {
                        System.err.println("Skipping malformed message: " + e.getMessage());
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        JsonNode original = mapper.readTree(value);
                        ObjectNode alert = mapper.createObjectNode();
                        alert.put("station_id",       original.path("station_id").asLong());
                        alert.put("s_no",             original.path("s_no").asLong());
                        alert.put("status_timestamp", original.path("status_timestamp").asLong());
                        alert.put("humidity",         original.path("weather").path("humidity").asInt());
                        alert.put("alert",            "RAINING - Humidity exceeded 70%");
                        return mapper.writeValueAsString(alert); // mapper.writeValueAsString(alert) converts the object back to a JSON string to send to Kafka.
                    } catch (Exception e) {
                        return "{\"alert\":\"error processing message\"}";
                    }
                })
                .peek((key, value) ->
                        System.out.println("Rain alert sent: " + value)
                )
                .to(OUTPUT_TOPIC); // Publishes each transformed alert message to the rain-alert

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        System.out.println("Rain Detection Processor running.");
        System.out.println("Input  topic : " + INPUT_TOPIC);
        System.out.println("Output topic : " + OUTPUT_TOPIC);
    }
}