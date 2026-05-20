package com.central;

import com.central.bitcask.BitCaskEngine;
import com.central.kafka.KafkaConsumerTask;

import java.io.File;
import java.io.IOException;

public class App {
    public static void main(String[] args) {
        System.out.println("=== Launching Central Weather Storage Station Engine ===");

        try {
            // Instantiate storage folder mapping using a java.io.File descriptor wrapper
            File storageDir = new File("data");
            BitCaskEngine engine = new BitCaskEngine(storageDir);

            // 🔍 PRINT KEYDIR IMMEDIATELY AFTER COLD-START RECOVERY RUNS
            System.out.println("[DIAGNOSTIC] Checking recovered RAM index indices...");
            engine.printIndexState();

            KafkaConsumerTask consumerTask = new KafkaConsumerTask(engine);
            Thread kafkaThread = new Thread(consumerTask);

            // Safe shutdown hook loop mechanics to avoid corrupting active file streams on exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[SHUTDOWN] Executing safe pipeline disconnect sequences...");
                consumerTask.stop();
                try {
                    kafkaThread.join(2000);
                    engine.close();
                    System.out.println("[SHUTDOWN] Database storage file channels locked down safely.");
                } catch (IOException | InterruptedException e) {
                    System.err.println("Exception encountered during shutdown sequence: " + e.getMessage());
                }
            }));

            kafkaThread.setName("kafka-consumer-worker");
            kafkaThread.start();

            System.out.println("[SYSTEM] Master loop running. Awaiting input metrics streams...");

        } catch (Exception e) {
            System.err.println("[FATAL] Failure during cluster initial server system boot verification setup:");
            e.printStackTrace();
        }
    }
}