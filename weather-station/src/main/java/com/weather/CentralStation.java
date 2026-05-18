package com.weather;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class CentralStation {

    public static void main(String[] args) throws Exception {

        ParquetArchiver archiver = new ParquetArchiver();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "central-station");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("weather-topic"));

        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down — flushing remaining records...");
                running.set(false);
                Thread.sleep(1500);
                archiver.flush();
                consumer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        System.out.println("Central Station running, consuming from weather-topic...");

        while (running.get()) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    archiver.add(record.value());
                } catch (Exception e) {
                    System.err.println("Failed to archive message: " + e.getMessage());
                }
            }
        }
    }
}