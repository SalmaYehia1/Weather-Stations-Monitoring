package com.central.kafka;

import com.central.bitcask.BitCaskEngine;
import com.central.bitcask.constant.NetworkConstants;
import com.central.bitcask.constant.StorageConstants;
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
    private volatile boolean running = true;

    public KafkaConsumerTask(BitCaskEngine engine) {
        this.engine = engine;

        Properties props = new Properties();
        props.put("bootstrap.servers", NetworkConstants.KAFKA_SERVER);
        props.put("group.id", NetworkConstants.CONSUMER_GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(NetworkConstants.WEATHER_TOPIC));
    }

    @Override
    public void run() {
        System.out.printf("[KAFKA] Consumer listening on topic '%s'...%n", NetworkConstants.WEATHER_TOPIC);
        Pattern idPattern = Pattern.compile("\"station_id\":\\s*(\\d+)");

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String valueJson = record.value();
                    Matcher matcher = idPattern.matcher(valueJson);
                    
                    String stationId = matcher.find() ? matcher.group(1) : "UNKNOWN";
                    String key = NetworkConstants.STATION_KEY_PREFIX + stationId;

                    byte[] valueBytes = valueJson.getBytes(StorageConstants.DEFAULT_CHARSET);

                    engine.put(key, valueBytes);
                    System.out.println("[BITCASK] Written key coordinate: " + key);
                }
            }
        } catch (IOException e) {
            System.err.println("[BITCASK] Storage disk write failure: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("[KAFKA] Stream runtime engine exception: " + e.getMessage());
        } finally {
            consumer.close();
            System.out.println("[KAFKA] Consumer connection released safely.");
        }
    }

    public void stop() {
        this.running = false;
    }
}