#!/bin/bash
set -e

NET=$(python3 -c "import subprocess, json; print(list(json.loads(subprocess.check_output(['docker', 'inspect', 'kecamatras-kafka']).decode())[0]['NetworkSettings']['Networks'].keys())[0])")
IMG="kecamatras-pipeline"
APP_DIR="$(pwd)"

echo "================================================================="
echo "KECAMATRAS Full Pipeline Execution"
echo "================================================================="

echo "[0/4] Ingestion Layer (1 Cycle)..."
docker run --rm --network $NET \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e PYTHONUNBUFFERED=1 \
  -v "$APP_DIR:/app" \
  -w /app $IMG \
  python3 00_ingestion_api.py --broker kafka:29092 --max-cycles 1

echo "[1/4] Bronze Layer - Kafka Stream ke HDFS Raw..."
docker run --rm --network $NET \
  -e HADOOP_USER_NAME=hadoop \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e HDFS_NAMENODE=hdfs://namenode:8020 \
  -e PYTHONUNBUFFERED=1 \
  -v "$APP_DIR:/app" \
  -w /app $IMG \
  python3 01_bronze.py --stream

echo "[2/4] Silver Layer - Text Preprocessing dan Geo-Parsing..."
docker run --rm --network $NET \
  -e HADOOP_USER_NAME=hadoop \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e HDFS_NAMENODE=hdfs://namenode:8020 \
  -e PYTHONUNBUFFERED=1 \
  -v "$APP_DIR:/app" \
  -w /app $IMG \
  python3 02_silver.py

echo "[3/4] Gold Layer - Spark MLlib LDA dan Index Normalization..."
docker run --rm --network $NET \
  -e HADOOP_USER_NAME=hadoop \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e HDFS_NAMENODE=hdfs://namenode:8020 \
  -e PYTHONUNBUFFERED=1 \
  -v "$APP_DIR:/app" \
  -w /app $IMG \
  python3 03_gold.py

echo "[4/4] Export Gold Layer ke Dashboard JSON..."
docker run --rm --network $NET \
  -e HADOOP_USER_NAME=hadoop \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e HDFS_NAMENODE=hdfs://namenode:8020 \
  -e PYTHONUNBUFFERED=1 \
  -v "$APP_DIR:/app" \
  -w /app $IMG \
  python3 export_to_json.py

echo "✅ Pipeline Selesai. Data berhasil diekspor ke Dashboard!"
