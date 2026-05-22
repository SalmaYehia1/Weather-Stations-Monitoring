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
    
    public String toJson() {
        return String.format(
                "{\"station_id\":%d,\"s_no\":%d,\"battery_status\":\"%s\",\"status_timestamp\":%d,\"weather\":%s}",
                station_id, s_no, battery_status, status_timestamp, weather.toJson()
        );
    }

}