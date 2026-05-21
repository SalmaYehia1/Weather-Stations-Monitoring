package com.weather;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class WeatherProducer {

    public static void main(String[] args) throws Exception {

        long stationId = Long.parseLong(args[0]);
        WeatherStation station = new WeatherStation(stationId);
        Gson gson = new Gson();

        Properties properties = new Properties();
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        while (true) {
            WeatherMessage msg = station.generateMessage(); // called ONCE, always

            if (station.shouldDropMessage()) {
                System.out.println("Station " + stationId +
                        " -> Message Dropped (s_no=" + msg.s_no + ")");
            } else {
                String json = gson.toJson(msg);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                "weather-topic",
                                String.valueOf(msg.station_id),
                                json
                        );
                producer.send(record);
                System.out.println("Station " + stationId + " Sent: " + json);
            }

            Thread.sleep(1000);
        }
    }
}