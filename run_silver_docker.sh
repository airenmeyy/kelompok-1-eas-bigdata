#!/bin/bash
# Script untuk menjalankan 02_silver.py di dalam Docker Network (kecamatras-net)
# Ini mengatasi limitasi akses port acak dari HDFS Datanode ke host WSL.

echo "🚀 Menjalankan Silver Layer Engine di dalam Docker Network (kecamatras-net)..."
echo "📦 Meng-install dependencies & menjalankan Spark (proses ini butuh beberapa detik)..."

# Gunakan image bitnami/spark:3.5.1 atau bde2020 sebagai container ephemeral
# Di sini kita pakai python:3.12-slim yang di-inject Spark dan Hadoop
docker run --rm \
  --network kelompok-1-eas-bigdata_kecamatras-net \
  -e HADOOP_USER_NAME=hadoop \
  -v "$(pwd)":/app \
  -w /app \
  python:3.12-slim \
  /bin/bash -c "
    echo '⚙️ Menginstall dependensi (Java, PySpark, Delta)...'
    apt-get update && apt-get install -y default-jre > /dev/null 2>&1
    pip install pyspark==3.5.1 delta-spark==3.0.0 > /dev/null 2>&1
    
    echo '🔥 Menjalankan 02_silver.py...'
    python 02_silver.py \"\$@\"
  " _ "$@"

echo "✅ Eksekusi selesai!"
