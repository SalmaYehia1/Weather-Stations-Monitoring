#!/bin/bash

SERVER_URL="http://localhost:8080"

case "$1" in
    --view-all)
        TIMESTAMP=$(date +%s)
        FILE_NAME="${TIMESTAMP}.csv"
        
        echo "key,value" > "$FILE_NAME"
        curl -s "${SERVER_URL}/getAll" >> "$FILE_NAME"
        
        echo "[SUCCESS] Dumped all active records directly to snapshot file: $FILE_NAME"
        ;;
        
    --view)
        if [[ $2 =~ --key=(.*) ]]; then
            KEY="${BASH_REMATCH[1]}"
            RESPONSE=$(curl -s -w "\n%{http_code}" "${SERVER_URL}/get?key=${KEY}")
            HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
            BODY=$(echo "$RESPONSE" | sed '$d')

            if [ "$HTTP_STATUS" -eq 200 ]; then
                echo "$BODY"
            elif [ "$HTTP_STATUS" -eq 404 ]; then
                echo "[ERROR] Key '${KEY}' not found inside active database indices."
            else
                echo "[SERVER ERROR] Status code: $HTTP_STATUS"
            fi
        else
            echo "Usage: ./bitcask_client.sh --view --key=station_id:1"
        fi
        ;;
        
    --perf)
        if [[ $2 =~ --clients=(.*) ]]; then
            CLIENT_COUNT="${BASH_REMATCH[1]}"
            TIMESTAMP=$(date +%s)
            
            echo "[PERF TEST] Dispatching $CLIENT_COUNT benchmarking parallel threads..."
            
            for ((i=1; i<=CLIENT_COUNT; i++)); do
                (
                    THREAD_FILE="${TIMESTAMP}_thread_${i}.csv"
                    echo "key,value" > "$THREAD_FILE"
                    curl -s "${SERVER_URL}/getAll" >> "$THREAD_FILE"
                ) &
            done
            
            wait
            echo "[SUCCESS] Multi-client test complete. All $CLIENT_COUNT files compiled cleanly."
        else
            echo "Usage: ./bitcask_client.sh --perf --clients=100"
        fi
        ;;
        
    *)
        echo "Usage: ./bitcask_client.sh {--view-all | --view --key=KEY | --perf --clients=NUM}"
        exit 1
        ;;
esac
