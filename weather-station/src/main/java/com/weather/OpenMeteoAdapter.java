package com.weather;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.FileWriter;

public class OpenMeteoAdapter {

    private static final Location[] LOCATIONS = {
            new Location(-1L, "Cairo",     30.0444, 31.2357),
            new Location(-2L, "Alexandria", 31.2001, 29.9187),
            new Location(-3L, "London",    51.5072, -0.1276),
            new Location(-4L, "New York",  40.7128, -74.0060),
    };

    // Open-Meteo free endpoint — no API key required
    private static final String API_BASE =
            "https://api.open-meteo.com/v1/forecast" +
                    "?latitude=%f&longitude=%f" +
                    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m" +
                    "&wind_speed_unit=ms";   // m/s keeps units consistent with station data

//    temperature_2m → temperature
//    relative_humidity_2m → humidity
//    wind_speed_10m → wind_speed

    private static final String TOPIC        = "weather-data";
    private static final long   POLL_INTERVAL = 1000L; // 1 sec
    private static final long[] serialCounters = loadSerialCounters();

    // ------------------------------------------------------------------ //

    // Every second, fetches all 4 locations and publishes each as a Kafka message — exactly like real weather stations.

    public static void main(String[] args) throws Exception {

        HttpClient  http     = HttpClient.newHttpClient();
        Gson        gson     = new Gson();
        KafkaProducer<String, String> producer = buildProducer();

        System.out.println("[OpenMeteoAdapter] Starting — polling " +
                LOCATIONS.length + " location(s) every " +
                (POLL_INTERVAL / 1000) + "s");

        while (true) {
            for (int i = 0; i < LOCATIONS.length; i++) {
                Location loc = LOCATIONS[i];
                try {
                    WeatherMessage msg = fetchAndMap(http, loc,serialCounters[i]++);
                    saveCounter(i, serialCounters[i]); // persist after increment
                    String json = gson.toJson(msg);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(
                                    TOPIC,
                                    String.valueOf(msg.station_id), // key = station id
                                    json
                            );

                    producer.send(record, (metadata, ex) -> {
                        if (ex != null) {
                            System.err.println("[OpenMeteoAdapter] Send failed for " +
                                    loc.name + ": " + ex.getMessage());
                        } else {
                            System.out.println("[OpenMeteoAdapter] Published (" +
                                    loc.name + "): " + json);
                        }
                    });

                } catch (Exception e) {
                    System.err.println("[OpenMeteoAdapter] Error fetching " +
                            loc.name + ": " + e.getMessage());
                }
            }

            Thread.sleep(POLL_INTERVAL);
        }
    }




    private static WeatherMessage fetchAndMap(HttpClient http, Location loc,long sno)
            throws Exception {

        String url = String.format(API_BASE, loc.latitude, loc.longitude);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();


        // make HTTP GET request
        HttpResponse<String> response =
                http.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() +
                    " from Open-Meteo for " + loc.name);
        }

        // Parse response
        JsonObject root    = JsonParser.parseString(response.body()).getAsJsonObject();

        //  parse the JSON response
        JsonObject current = root.getAsJsonObject("current");
        double tempC    = current.get("temperature_2m").getAsDouble();
        int    humidity = current.get("relative_humidity_2m").getAsInt();
        double windMs   = current.get("wind_speed_10m").getAsDouble();

        // Map to station ranges (clamp to avoid out-of-range values)
        int mappedTemp = clamp((int) Math.round(tempC + 20), 0, 100); // shift: -20°C→0, 80°C→100
        int mappedWind = clamp((int) Math.round(windMs * 2), 0, 50);  // scale: 25 m/s → 50
        int mappedHumidity = clamp(humidity, 60, 100);

        Weather weather = new Weather(mappedHumidity, mappedTemp, mappedWind);

        return new WeatherMessage(
                loc.stationId,
                sno,
                "high",                          // API source — no battery degradation
                System.currentTimeMillis() / 1000,
                weather
        );
    }

    // ------------------------------------------------------------------ //
    //  Helpers                                                            //
    // ------------------------------------------------------------------ //


    private static KafkaProducer<String, String> buildProducer() {

        Properties props = new Properties();

        props.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "kafka:9092"
        );

        props.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // avoid duplicates on retries
        props.setProperty(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                "true"
        );

        return new KafkaProducer<>(props);
    }

    private static long[] loadSerialCounters() {

        long[] counters = new long[LOCATIONS.length];

        for (int i = 0; i < LOCATIONS.length; i++) {

            File f = new File(
                    "/data/adapter/adapter_station_" +
                            LOCATIONS[i].stationId +
                            "_sno.txt"
            );

            if (f.exists()) {

                try (BufferedReader br =
                             new BufferedReader(new FileReader(f))) {

                    counters[i] =
                            Long.parseLong(br.readLine().trim());

                } catch (Exception e) {
                    counters[i] = 0;
                }
            }
        }

        return counters;
    }

    private static void saveCounter(int index, long value) {
        try (PrintWriter pw = new PrintWriter(
                new FileWriter("/data/adapter/adapter_station_" + LOCATIONS[index].stationId + "_sno.txt"))) {
            pw.println(value);
        } catch (Exception e) {
            System.err.println("Failed to save counter: " + e.getMessage());
        }
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }


    static class Location {
        final long   stationId;
        final String name;
        final double latitude;
        final double longitude;

        Location(long stationId, String name, double latitude, double longitude) {
            this.stationId = stationId;
            this.name      = name;
            this.latitude  = latitude;
            this.longitude = longitude;
        }
    }
}