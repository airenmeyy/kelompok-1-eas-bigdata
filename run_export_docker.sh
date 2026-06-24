#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
# KECAMATRAS — Gold Data Exporter Docker Runner
# ═══════════════════════════════════════════════════════════════════

echo "🚀 Menjalankan Data Exporter di dalam Docker Network..."
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
    
    echo '🔥 Menjalankan export_to_json.py...'
    python export_to_json.py \"\$@\"
  " _ "$@"

echo "✅ Eksekusi ekspor data selesai!"
