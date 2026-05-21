#!/bin/bash
echo "=== Starting Central Station with JFR ==="
cd "$(dirname "$0")/../central-station"
rm -f central_station.jfr
java -XX:StartFlightRecording=duration=60s,filename=central_station.jfr,settings=profile \
     -cp "target/central-station-1.0-SNAPSHOT.jar:target/deps/*" com.central.App
echo "=== Printing JFR Results ==="
jfr print --events jdk.GCPhasePause central_station.jfr
jfr print --events jdk.FileRead,jdk.FileWrite central_station.jfr
jfr summary central_station.jfr
