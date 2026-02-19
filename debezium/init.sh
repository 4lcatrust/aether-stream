#!/usr/bin/env sh
set -e
CONNECT_URL="http://kafka-connect:8083"
CONNECTOR_NAME="aether-connector"
CONNECTOR_FILE="/debezium/aether-connector.json"
echo "Waiting for Kafka Connect..."
until curl -s "${CONNECT_URL}/" > /dev/null; do
  sleep 3
done
echo "Kafka Connect is up."
echo "Registering (or updating) connector via PUT..."
curl -s -X PUT \
  -H "Content-Type: application/json" \
  --data @"${CONNECTOR_FILE}" \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"
echo "Connector registration completed."