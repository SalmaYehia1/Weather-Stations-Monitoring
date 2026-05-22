#!/bin/bash
echo "=== Starting Central Ingestion Core Pipeline ==="

# Move up TWO levels to leave 'scripts/run_scripts/' and land in the root project directory
cd "$(dirname "$0")/../../central-station"

# Launch the Ingestion Engine in the background
java -jar target/central-station-1.0-SNAPSHOT.jar &
CENTRAL_PID=$!
echo "Ingestion Pipeline Engine started (PID: ${CENTRAL_PID})"

# Give the engine 2 seconds to initialize file systems before binding ports
sleep 2

echo "=== Starting Independent BitCask API Server ==="
java -cp target/central-station-1.0-SNAPSHOT.jar com.central.server.BitcaskServer &
SERVER_ID=$!
echo "BitCask Client REST endpoints active on Port 8080 (PID: ${SERVER_ID})"

# Await both parallel backend tracking structures
wait ${CENTRAL_PID} ${SERVER_ID}
