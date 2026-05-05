#!/bin/sh
set -e

until mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  echo "Waiting for MinIO..."
  sleep 2
done

echo "MinIO ready. Creating buckets..."

for bucket in raw-html raw-json parsed-snapshots llm-assist exports; do
  if mc mb --ignore-existing "local/$bucket"; then
    echo "Bucket ready: $bucket"
  else
    echo "Warning: could not ensure bucket $bucket" >&2
  fi
done

echo "MinIO bootstrap complete."
