#!/bin/sh
set -e
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
echo "Waiting for MinIO to be ready..."
until mc alias set local "${MINIO_ENDPOINT}" "${MINIO_USER}" "${MINIO_PASS}"; do
  sleep 2
done
echo "Connected to MinIO"
mc mb --ignore-existing local/bronze
mc mb --ignore-existing local/flink-checkpoints
mc mb --ignore-existing local/flink-savepoints
mc mb --ignore-existing local/warehouse
mc anonymous set download local/bronze || true
mc anonymous set download local/flink-checkpoints || true
mc anonymous set download local/flink-savepoints || true
mc anonymous set download local/warehouse || true
echo "MinIO init complete!"