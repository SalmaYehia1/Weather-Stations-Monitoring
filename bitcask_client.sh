#!/bin/bash

# auto port-forward in background
kubectl port-forward deployment/central-station 8080:8080 > /dev/null 2>&1 &
PF_PID=$!
sleep 2  # wait for it to be ready

SERVER_URL="http://localhost:8080"
MODE=""
KEY=""
CLIENT_COUNT=100

for arg in "$@"; do
    case $arg in
        --view-all)    MODE="view-all" ;;
        --view)        MODE="view" ;;
        --key=*)       KEY="${arg#--key=}" ;;
        --perf)        MODE="perf" ;;
        --clients=*)   CLIENT_COUNT="${arg#--clients=}" ;;
    esac
done

# kill port-forward when script exits
trap "kill $PF_PID 2>/dev/null" EXIT

if [ "$MODE" = "view-all" ]; then
    TIMESTAMP=$(date +%s)
    FILE_NAME="${TIMESTAMP}.csv"
    echo "key,value" > "$FILE_NAME"
    curl -s "${SERVER_URL}/getAll" >> "$FILE_NAME"
    echo "[SUCCESS] Saved to: $FILE_NAME"

elif [ "$MODE" = "view" ]; then
    if [ -z "$KEY" ]; then echo "[ERROR] Missing --key"; exit 1; fi
    RESPONSE=$(curl -s -w "\n%{http_code}" "${SERVER_URL}/get?key=${KEY}")
    HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    if [ "$HTTP_STATUS" -eq 200 ]; then echo "$BODY"
    elif [ "$HTTP_STATUS" -eq 404 ]; then echo "[ERROR] Key '$KEY' not found."
    else echo "[SERVER ERROR] Status: $HTTP_STATUS"; fi

elif [ "$MODE" = "perf" ]; then
    TIMESTAMP=$(date +%s)
    echo "[PERF] Spawning $CLIENT_COUNT parallel threads..."
    for ((i=1; i<=CLIENT_COUNT; i++)); do
        (
            THREAD_FILE="${TIMESTAMP}_thread_${i}.csv"
            echo "key,value" > "$THREAD_FILE"
            curl -s "${SERVER_URL}/getAll" >> "$THREAD_FILE"
        ) &
    done
    wait
    echo "[SUCCESS] Done. $CLIENT_COUNT files created."

else
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view --key=station_id:1"
    echo "  ./bitcask_client.sh --perf --clients=100"
fi
