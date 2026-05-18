package com.weather;

class Weather {
    int humidity;
    int temperature;
    int wind_speed;

    public Weather(int humidity, int temperature, int wind_speed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.wind_speed = wind_speed;
    }

    public String toJson() {
        return String.format(
                "{\"humidity\":%d,\"temperature\":%d,\"wind_speed\":%d}",
                humidity, temperature, wind_speed
        );
    }
}
