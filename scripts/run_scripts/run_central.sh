#!/bin/bash
echo "=== Starting Central Station ==="
cd "$(dirname "$0")/../central-station"
java -jar target/central-station-1.0-SNAPSHOT.jar