SERVER_URL="http://localhost:8080"
TIMESTAMP=$(date +%s)
FILE_NAME="${TIMESTAMP}.csv"

echo "key,value" > "$FILE_NAME"
curl -s "${SERVER_URL}/getAll" >> "$FILE_NAME"

echo "[SUCCESS] Dumped all active records directly to snapshot file: $FILE_NAME"