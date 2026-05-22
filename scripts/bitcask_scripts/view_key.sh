SERVER_URL="http://localhost:8080"

if [ -z "$1" ]; then
    echo "Usage: ./view_key.sh station_id:1"
    exit 1
fi

RESPONSE=$(curl -s -w "\n%{http_code}" "${SERVER_URL}/get?key=$1")
HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "$BODY"
elif [ "$HTTP_STATUS" -eq 404 ]; then
    echo "[ERROR] Key '$1' not found inside active database indices."
else
    echo "[SERVER ERROR] Status code: $HTTP_STATUS"
fi