#!/bin/sh
set -eu

mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in raw-html raw-json parsed-snapshots llm-assist exports; do
  mc mb --ignore-existing "local/$bucket"
  mc version enable "local/$bucket"
done
