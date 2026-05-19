package com.central.kafka;

import com.central.bitcask.BitCaskEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaConsumerTask implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final BitCaskEngine engine;

    public KafkaConsumerTask(BitCaskEngine engine) {
        this.engine = engine;

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "central-station-bin-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList("weather-topic"));
    }

    @Override
    public void run() {
        System.out.println("Kafka Consumer listening...");
        Pattern idPattern = Pattern.compile("\"station_id\":\\s*(\\d+)");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String valueJson = record.value();
                    Matcher matcher = idPattern.matcher(valueJson);
                    String key = matcher.find() ? matcher.group(1) : "UNKNOWN";

                    engine.put(key, valueJson);
                    System.out.println("[BITCASK] Written key: " + key);
                }
            }
        } catch (IOException e) {
            System.err.println("BitCask write error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Stream error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}