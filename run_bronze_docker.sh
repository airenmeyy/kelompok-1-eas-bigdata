#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
# KECAMATRAS — Bronze Layer Docker Runner
# ═══════════════════════════════════════════════════════════════════
# Script ini menjalankan 01_bronze.py dari DALAM network Docker.
# Ini memastikan PySpark dapat mengakses HDFS (Namenode & Datanode)
# tanpa terhalang isu networking host (UnknownHostException / Connection Refused).
# ═══════════════════════════════════════════════════════════════════

echo "🚀 Menjalankan Bronze Layer Engine di dalam Docker Network (kecamatras-net)..."
echo "📦 Meng-install dependencies & menjalankan Spark (proses ini butuh beberapa detik)..."

# Gunakan image bitnami/spark:3.5.1 atau bde2020 sebagai container ephemeral
docker run --rm \
  --network kelompok-1-eas-bigdata_kecamatras-net \
  -e HADOOP_USER_NAME=hadoop \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -v "$(pwd)":/app \
  -w /app \
  python:3.12-slim \
  bash -c "
    echo '⚙️ Menginstall dependensi (PySpark, Delta, Kafka)...'
    pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.0.0 kafka-python-ng bs4 feedparser > /dev/null 2>&1
    
    echo '🔥 Menjalankan 01_bronze.py...'
    # Gunakan JDK dari PySpark environment jika diperlukan, atau Python murni
    # Karena delta-spark butuh Java, kita install default-jre
    apt-get update > /dev/null 2>&1 && apt-get install -y default-jre > /dev/null 2>&1
    
    python 01_bronze.py \"\$@\"
  " _ "$@"

echo "✅ Eksekusi selesai!"
