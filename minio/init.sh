#!/bin/sh
set -e
echo "Waiting for MinIO to be ready..."
until mc alias set local http://aether_minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  sleep 2
done
echo "Connected to MinIO"

mc mb --ignore-existing local/aether-bucket
mc mb --ignore-existing local/aether-raw
mc mb --ignore-existing local/aether-archive
mc policy set public local/aether-bucket
mc policy set none local/aether-raw
mc policy set none local/aether-archive
echo "MinIO init complete!"
