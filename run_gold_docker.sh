#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
# KECAMATRAS — Gold Layer Docker Runner
# ═══════════════════════════════════════════════════════════════════

echo "🚀 Menjalankan Gold Layer Engine di dalam Docker Network (kecamatras-net)..."
echo "📦 Meng-install dependencies & menjalankan Spark (proses ini butuh beberapa detik)..."

docker run --rm \
  --network kelompok-1-eas-bigdata_kecamatras-net \
  -e HADOOP_USER_NAME=hadoop \
  -v "$(pwd)":/app \
  -w /app \
  python:3.12-slim \
  bash -c "
    echo '⚙️ Menginstall dependensi (Java, PySpark, Delta)...'
    apt-get update > /dev/null 2>&1 && apt-get install -y default-jre > /dev/null 2>&1
    pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.0.0 numpy setuptools > /dev/null 2>&1
    
    echo '🔥 Menjalankan 03_gold.py...'
    python 03_gold.py \"\$@\"
  " _ "$@"

echo "✅ Eksekusi selesai!"
