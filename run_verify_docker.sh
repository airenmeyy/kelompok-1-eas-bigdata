#!/bin/bash
docker run --rm \
  --network kelompok-1-eas-bigdata_kecamatras-net \
  -e HADOOP_USER_NAME=hadoop \
  -v "$(pwd)":/app \
  -w /app \
  python:3.12-slim \
  bash -c "apt-get update >/dev/null 2>&1 && apt-get install -y default-jre >/dev/null 2>&1 && pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.0.0 >/dev/null 2>&1 && python verifikasi/verify_silver.py"
