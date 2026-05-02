#!/bin/sh
set -eu

until mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo "Waiting for MinIO to accept bootstrap connections..."
  sleep 2
done

for bucket in raw-html raw-json parsed-snapshots llm-assist exports; do
  mc mb --ignore-existing "local/$bucket"
  mc version enable "local/$bucket"
done
