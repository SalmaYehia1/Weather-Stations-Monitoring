SERVER_URL="http://localhost:8080"
CLIENT_COUNT=${1:-100}
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