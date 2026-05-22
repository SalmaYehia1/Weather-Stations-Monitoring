#!/bin/bash
# BitCask-only test: seed sample keys, then start the HTTP API (no Kafka).
set -e

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/central-station"

if [ ! -f target/central-station-1.0-SNAPSHOT.jar ]; then
    echo "Building central-station..."
    mvn -q package -DskipTests
fi

JAR=target/central-station-1.0-SNAPSHOT.jar

echo "=== [1/2] Seeding BitCask (in-process put/get) ==="
java -cp "$JAR" com.central.bitcask.BitcaskDemo

echo ""
echo "=== [2/2] Starting BitCask HTTP API on :8080 (Ctrl+C to stop) ==="
java -cp "$JAR" com.central.server.BitcaskServer
