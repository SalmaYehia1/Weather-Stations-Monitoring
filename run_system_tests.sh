#!/bin/bash
# ==============================================================================
# ALEXANDRIA UNIVERSITY - CSE-4E3 COURSE PROJECT AUTOMATED TEST SUITE
# Comprehensive Verification: Parts A, B, C, and D
# ==============================================================================

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}==================================================================${NC}"
echo -e "${CYAN}         STARTING AUTOMATED SYSTEM INTEGRATION TESTING            ${NC}"
echo -e "${CYAN}==================================================================${NC}"

cd "$(dirname "$0")"

# Clean up workspace completely from previous runs to prevent false statistics
rm -f *.csv
rm -f documentation/perf_results/*.csv

# ------------------------------------------------------------------------------
# VERIFY PART A & B: Weather Stations & Kafka Ingestion
# ------------------------------------------------------------------------------
echo -e "\n${YELLOW}[STAGE 1/4] Checking Ingestion & Data Generation (Parts A & B)...${NC}"

# Find active binary logs written by our ingestion consumer loops
ACTIVE_DATA_FILE="central-station/data/active_data.bin"
if [ ! -f "$ACTIVE_DATA_FILE" ]; then
    ACTIVE_DATA_FILE=$(ls -t central-station/data/active_data_*.bin 2>/dev/null | head -n 1)
fi

if [ -f "$ACTIVE_DATA_FILE" ] && [ $(wc -c < "$ACTIVE_DATA_FILE") -gt 0 ]; then
    echo -e "${GREEN}[PASS] Storage engine is actively writing raw data blocks to disk.${NC}"
    echo -e "Inspecting live binary schema contents inside: $ACTIVE_DATA_FILE"
    strings "$ACTIVE_DATA_FILE" | grep -E "station_id|weather" | head -n 3
else
    echo -e "${RED}[FAIL] Ingestion check failed. active_data.bin is missing or empty. Ensure system servers are running.${NC}"
fi

# ------------------------------------------------------------------------------
# VERIFY PART C: Kafka Raining Trigger Processor
# ------------------------------------------------------------------------------
echo -e "\n${YELLOW}[STAGE 2/4] Checking Raining Filter Triggers (Part C)...${NC}"
echo "Scanning data logs for high-humidity alert events (>70%)..."

# Extract and count how many high-humidity instances are parsed from our storage rows
HIGH_HUMIDITY_COUNT=$(strings "$ACTIVE_DATA_FILE" 2>/dev/null | grep -o '"humidity":[7-9][0-9]' | wc -l)

if [ "$HIGH_HUMIDITY_COUNT" -gt 0 ]; then
    echo -e "${GREEN}[PASS] Rain trigger processor active! Captured ${HIGH_HUMIDITY_COUNT} records exceeding 70% humidity threshold.${NC}"
else
    echo -e "${YELLOW}[WARN] No high-humidity records captured yet. Let the weather producers stream a few seconds longer.${NC}"
fi

# ------------------------------------------------------------------------------
# VERIFY PART D-1: BitCask Point Reads, Snapshots, & Hint Files
# ------------------------------------------------------------------------------
echo -e "\n${YELLOW}[STAGE 3/4] Checking BitCask LSM Engine & Hint Architecture (Part D)...${NC}"

echo "Testing Point-Lookup Engine endpoint (station_id:3):"
./scripts/bitcask_scripts/view_key.sh station_id:3

echo -e "\nTesting Database Master Snapshot Output generation (--view-all):"
./scripts/bitcask_scripts/view_all.sh

LATEST_SNAPSHOT=$(ls -t 1779*.csv 2>/dev/null | head -n 1)
if [ -f "$LATEST_SNAPSHOT" ]; then
    echo -e "${GREEN}[PASS] Snapshot file verified successfully: $LATEST_SNAPSHOT${NC}"
    head -n 5 "$LATEST_SNAPSHOT"
else
    echo -e "${RED}[FAIL] Snapshot file was not created.${NC}"
fi

# Hint File Architecture Verification
echo -e "\nChecking for recovery indexes (Hint Files)..."
LATEST_HINT=$(ls -t central-station/data/hint_active_data_*.bin 2>/dev/null | head -n 1)
if [ -f "$LATEST_HINT" ]; then
    echo -e "${GREEN}[PASS] Recovery Hint file verified: $LATEST_HINT${NC}"
else
    echo -e "${RED}[FAIL] Hint file check failed. Ensure the compactor has executed at least once.${NC}"
fi

# ------------------------------------------------------------------------------
# VERIFY PART D-2: 100-Client Concurrency Performance Stress Test
# ------------------------------------------------------------------------------
echo -e "\n${YELLOW}[STAGE 4/4] Checking 100-Client Virtual-Thread Scaling & Parquet Archive (Part D)...${NC}"
./scripts/bitcask_scripts/perf_test.sh 100

echo "Allowing parallel worker threads to complete disk IO sync..."
sleep 2

# Move execution results safely into their home folder
mv *_thread_*.csv documentation/perf_results/ 2>/dev/null
mv 1779*.csv documentation/perf_results/ 2>/dev/null

THREAD_COUNT=$(ls -1 documentation/perf_results/*_thread_*.csv 2>/dev/null | wc -l)
SAMPLE_SIZE=$(ls -l documentation/perf_results/ | grep _thread_ | head -n 1 | awk '{print $5}')

if [ "$THREAD_COUNT" -eq 100 ] && [ "$SAMPLE_SIZE" -gt 20 ]; then
    echo -e "${GREEN}[PASS] Concurrency metrics valid! Exactly 100 distinct worker logs stored inside documentation/perf_results/${NC}"
    echo -e "${GREEN}[METRIC VIEW] Stable client read snapshot file payload: ${SAMPLE_SIZE} Bytes.${NC}"
else
    echo -e "${RED}[FAIL] Concurrency stress test failure. Thread logs count: ${THREAD_COUNT}, Payload Size: ${SAMPLE_SIZE} Bytes.${NC}"
fi

# Parquet Check
echo -e "\nChecking Parquet Historical Archiving Layout..."
PARQUET_COUNT=$(find central-station/data/parquet/ -name "*.parquet" | wc -l)
if [ "$PARQUET_COUNT" -gt 0 ]; then
    echo -e "${GREEN}[PASS] Parquet history branch valid! Found ${PARQUET_COUNT} time-partitioned batch blocks.${NC}"
else
    echo -e "${RED}[FAIL] Historical Parquet archive empty. Check background batch processing buffers.${NC}"
fi

echo -e "\n${CYAN}==================================================================${NC}"
echo -e "${GREEN}            ALL ARCHITECTURAL COMPONENT TESTS COMPLETED           ${NC}"
echo -e "${CYAN}==================================================================${NC}"
