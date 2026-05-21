#!/bin/bash
echo "=== Starting 10 Weather Producers ==="
cd "$(dirname "$0")/../weather-station"
for i in $(seq 1 10)
do
    java -cp target/weather-station-1.0-SNAPSHOT.jar com.weather.WeatherProducer $i &
    echo "Started station $i (PID: $!)"
done
echo "=== All 10 stations running ==="
wait
