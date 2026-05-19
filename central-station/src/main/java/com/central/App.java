package com.central;

import com.central.bitcask.BitCaskEngine;
import com.central.kafka.KafkaConsumerTask;
import java.io.IOException;

public class App {
    public static void main(String[] args) {
        System.out.println("Starting Central Station...");

        try {
            BitCaskEngine engine = new BitCaskEngine("data");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    engine.close();
                    System.out.println("BitCask closed.");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));

            KafkaConsumerTask consumerTask = new KafkaConsumerTask(engine);
            Thread kafkaThread = new Thread(consumerTask);
            kafkaThread.setName("kafka-consumer");
            kafkaThread.start();

            System.out.println("Central Station running. Waiting for Kafka messages...");

        } catch (Exception e) {
            System.err.println("Fatal error:");
            e.printStackTrace();
        }
    }
}