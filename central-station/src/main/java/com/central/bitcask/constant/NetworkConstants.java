package com.central.bitcask.constant;

public final class NetworkConstants {

    private NetworkConstants() {}

    public static final String KAFKA_SERVER = "localhost:9092";
    public static final String WEATHER_TOPIC = "weather-topic";
    public static final String CONSUMER_GROUP_ID = "central-station-group";

    public static final String STATION_KEY_PREFIX = "station_id:";
}