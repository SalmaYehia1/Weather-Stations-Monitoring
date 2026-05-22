#!/bin/bash
echo "=== Starting 10 Standalone Weather Station Emulators ==="

# Move up TWO levels to leave 'scripts/run_scripts/' and find 'weather-station' at the root
cd "$(dirname "$0")/../../weather-station"

for i in $(seq 1 10)
do
    java -cp target/weather-station-1.0-SNAPSHOT.jar com.weather.WeatherProducer $i &
    echo "Started Telemetry Station $i (System PID: $!)"
done

echo "=== All 10 telemetry nodes running actively ==="
wait
