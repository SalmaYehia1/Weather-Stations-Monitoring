package com.central.server;

import com.central.bitcask.BitCaskEngine;
import com.central.bitcask.constant.NetworkConstants;
import com.central.bitcask.constant.StorageConstants;
import com.central.server.archive.ParquetArchiver;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CentralStation {

    public static void main(String[] args) {
        System.out.println("=== Launching Central Weather Storage Station Engine ===");
        
        BitCaskEngine engine = null;
        ParquetArchiver archiver = null;
        KafkaConsumer<String, String> consumer = null;

        try {
            // 1. Initialize custom Bitcask engine
            File storageDir = new File(StorageConstants.DATA_DIR_NAME);
            engine = new BitCaskEngine(storageDir);

            // 🔍 DIAGNOSTIC: Print KeyDir on startup recovery
            System.out.println("[DIAGNOSTIC] Checking recovered RAM index indices...");
            engine.printIndexState();

            // 2. Initialize columnar analytics Parquet archiver 
            archiver = new ParquetArchiver();

            // 3. Mount global Kafka consumer properties
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        NetworkConstants.KAFKA_SERVER);
            props.put(ConsumerConfig.GROUP_ID_CONFIG,                 NetworkConstants.CONSUMER_GROUP_ID);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(List.of(NetworkConstants.WEATHER_TOPIC));

            System.out.printf("[SYSTEM] Master loop running. Awaiting inputs on topic '%s'...%n", NetworkConstants.WEATHER_TOPIC);

            AtomicBoolean running = new AtomicBoolean(true);
            
            final BitCaskEngine finalEngine = engine;
            final ParquetArchiver finalArchiver = archiver;
            final KafkaConsumer<String, String> finalConsumer = consumer;

            // 4. Graceful shutdown hook execution sequence
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("\n[SHUTDOWN] Executing safe pipeline disconnect sequences...");
                    running.set(false);
                    
                    Thread.sleep(1500);
                    
                    if (finalArchiver != null) {
                        finalArchiver.flush();
                        finalArchiver.close();
                        System.out.println("[SHUTDOWN] Columnar Parquet blocks successfully committed.");
                    }
                    if (finalEngine != null) {
                        finalEngine.close();
                        System.out.println("[SHUTDOWN] Database storage file channels locked down safely.");
                    }
                    if (finalConsumer != null) {
                        finalConsumer.close();
                        System.out.println("[SHUTDOWN] Kafka broker connections released safely.");
                    }
                } catch (Exception e) {
                    System.err.println("Exception encountered during shutdown sequence: " + e.getMessage());
                }
            }));

            // Regex parsing pattern matching incoming metrics JSON layouts
            Pattern idPattern = Pattern.compile("\"station_id\":\\s*(\\d+)");

            // 5. Main Consumer Loop
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    String valueJson = record.value();

                    // Pipeline Layer A: Parquet Cold Archiving
                    try {
                        finalArchiver.add(valueJson);
                    } catch (Exception e) {
                        System.err.println("[ARCHIVER ERROR] Parquet buffer drop failure: " + e.getMessage());
                    }

                    // Pipeline Layer B: Bitcask O(1) Local Key Value Storage
                    try {
                        Matcher matcher = idPattern.matcher(valueJson);
                        String stationId = matcher.find() ? matcher.group(1) : "UNKNOWN";
                        
                        String bitcaskKey = NetworkConstants.STATION_KEY_PREFIX + stationId;
                        byte[] valueBytes = valueJson.getBytes(StorageConstants.DEFAULT_CHARSET);

                        finalEngine.put(bitcaskKey, valueBytes);
                        System.out.println("[BITCASK] Row written to binary storage channel for key: " + bitcaskKey);
                    } catch (IOException e) {
                        System.err.println("[BITCASK ERROR] Storage disk append file failure: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[FATAL] Failure during cluster initial server setup:");
            e.printStackTrace();
        }
    }
}
