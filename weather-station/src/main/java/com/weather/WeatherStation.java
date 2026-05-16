package com.weather;

import java.util.Random;

public class WeatherStation {

    private final long stationId;
    private long serialNo = 1;

    private final Random random = new Random();

    public WeatherStation(long stationId) {
        this.stationId = stationId;
    }

    public WeatherMessage generateMessage() {

        String battery = generateBatteryStatus();

        Weather weather = new Weather(
                random.nextInt(101),          // humidity
                60 + random.nextInt(41),     // temperature
                random.nextInt(51)           // wind speed
        );

        WeatherMessage message = new WeatherMessage(
                stationId,
                serialNo++,
                battery,
                System.currentTimeMillis() / 1000,
                weather
        );

        return message;
    }

    private String generateBatteryStatus() {

        int value = random.nextInt(100);

        if (value < 30)
            return "low";

        else if (value < 70)
            return "medium";

        return "high";
    }

    public boolean shouldDropMessage() {

        return random.nextInt(100) < 10;
    }
}