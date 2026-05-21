package com.weather;

import java.io.*;
import java.util.Random;

public class WeatherStation {

    private final long stationId;
    private long serialNo;
    private final Random random = new Random();
    private final String serialFile; // file to persist s_no

    public WeatherStation(long stationId) {
        this.stationId = stationId;
        this.serialFile = "station_" + stationId + "_sno.txt";
        this.serialNo = loadSerialNo(); // load from file on startup
    }

    // load last s_no from file, start from 1 if file doesn't exist
    private long loadSerialNo() {
        File f = new File(serialFile);
        if (!f.exists()) return 1;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            return Long.parseLong(br.readLine().trim());
        } catch (Exception e) {
            return 1;
        }
    }

    // save current s_no to file
    private void saveSerialNo() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(serialFile))) {
            pw.println(serialNo);
        } catch (Exception e) {
            System.err.println("Failed to save serial number: " + e.getMessage());
        }
    }

    public WeatherMessage generateMessage() {
        String battery = generateBatteryStatus();

        Weather weather = new Weather(
                random.nextInt(101),
                60 + random.nextInt(41),
                random.nextInt(51)
        );

        long currentSerialNo = serialNo++;
        saveSerialNo(); // persist after every increment

        return new WeatherMessage(
                stationId,
                currentSerialNo,
                battery,
                System.currentTimeMillis() / 1000,
                weather
        );
    }

    private String generateBatteryStatus() {
        int value = random.nextInt(100);
        if (value < 30) return "low";
        else if (value < 70) return "medium";
        return "high";
    }

    public boolean shouldDropMessage() {
        return random.nextInt(100) < 10;
    }
}