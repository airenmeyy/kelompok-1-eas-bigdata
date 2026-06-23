#!/bin/bash
# =============================================================
# setup_bronze.sh — Setup Virtual Environment untuk KECAMATRAS
# Jalankan di WSL Ubuntu dari root proyek:
#   bash setup_bronze.sh
# =============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=================================================="
echo "  KECAMATRAS — Setup Environment Bronze Layer"
echo "  Directory: $PROJECT_DIR"
echo "=================================================="

# 1. Cek Java
echo ""
echo "[1/4] Mengecek Java..."
if command -v java &> /dev/null; then
    java -version 2>&1 | head -1
    echo "      ✅ Java terdeteksi."
else
    echo "      ❌ Java tidak ditemukan. Menginstall..."
    sudo apt update && sudo apt install -y openjdk-11-jdk
fi

# Set JAVA_HOME jika belum ada
if [ -z "$JAVA_HOME" ]; then
    JAVA_CANDIDATE="/usr/lib/jvm/java-11-openjdk-amd64"
    if [ ! -d "$JAVA_CANDIDATE" ]; then
        # Cari Java yang ada
        JAVA_CANDIDATE=$(dirname $(dirname $(readlink -f $(which java))))
    fi
    export JAVA_HOME="$JAVA_CANDIDATE"
    echo "      JAVA_HOME=$JAVA_HOME"
fi

# 2. Buat Virtual Environment
echo ""
echo "[2/4] Membuat Python Virtual Environment (.venv)..."
if [ -d ".venv" ]; then
    echo "      .venv sudah ada, melewati pembuatan."
else
    python3 -m venv .venv
    echo "      ✅ .venv berhasil dibuat."
fi

# 3. Aktivasi & Install Dependencies
echo ""
echo "[3/4] Menginstall PySpark + Delta Lake di .venv..."
source .venv/bin/activate

pip install --upgrade pip --quiet
pip install 'pyspark>=3.3.0' 'delta-spark>=2.3.0' 'pandas>=1.5.0' 'requests>=2.28.0' --quiet

echo "      ✅ Semua dependencies terinstall."

# 4. Verifikasi
echo ""
echo "[4/4] Verifikasi instalasi..."
python3 -c "import pyspark; print(f'      PySpark version: {pyspark.__version__}')"
python3 -c "import delta; print(f'      Delta-Spark: OK')"
python3 -c "import pandas; print(f'      Pandas version: {pandas.__version__}')"

# Cari spark-submit di venv
SPARK_SUBMIT=$(find .venv -name "spark-submit" -type f 2>/dev/null | head -1)
if [ -n "$SPARK_SUBMIT" ]; then
    echo "      spark-submit: $SPARK_SUBMIT"
else
    echo "      ⚠️  spark-submit tidak ditemukan di .venv, gunakan 'python3 01_bronze.py'"
fi

echo ""
echo "=================================================="
echo "  ✅ SETUP SELESAI!"
echo ""
echo "  Cara menjalankan 01_bronze.py:"
echo ""
echo "  # Opsi A: Langsung dengan Python (DIREKOMENDASIKAN)"
echo "  source .venv/bin/activate"
echo "  python3 01_bronze.py"
echo ""
echo "  # Opsi B: Dengan spark-submit"
echo "  source .venv/bin/activate"
echo "  spark-submit --packages io.delta:delta-spark_2.12:3.0.0 01_bronze.py"
echo "=================================================="
