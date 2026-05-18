package com.weather;

public class WeatherMessage {

    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;
    Weather weather;

    public WeatherMessage(long station_id,
                          long s_no,
                          String battery_status,
                          long status_timestamp,
                          Weather weather) {

        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = weather;
    }

}