import os
import sys
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Setup Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("DataExporter")

HDFS_NAMENODE = "hdfs://namenode:8020"
GOLD_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/gold"
SILVER_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/silver"

TBL_INDEX_KRIMINALITAS = f"{GOLD_DIR}/tbl_index_kriminalitas"
TBL_INDEX_KESEHATAN = f"{GOLD_DIR}/tbl_index_kesehatan"
TBL_CLEAN_NEWS = f"{SILVER_DIR}/tbl_clean_news"

OUTPUT_DIR = "/app/dashboard/data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "kecamatras_data.json")

def main():
    logger.info("=" * 70)
    logger.info("  KECAMATRAS — Gold Data Exporter to JSON")
    logger.info("  Tim Anti Gravity | Institut Teknologi Sepuluh Nopember")
    logger.info("=" * 70)

    try:
        # Inisialisasi SparkSession
        spark = (
            SparkSession.builder
            .appName("Kecamatras-Exporter")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("[INIT] SparkSession berhasil dibuat.")
    except Exception as e:
        logger.error(f"[INIT] ❌ Gagal membuat SparkSession: {e}")
        sys.exit(1)

    try:
        # Cek ketersediaan direktori output
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 1. Baca Indeks Kriminalitas
        logger.info("[LOAD] Membaca data tbl_index_kriminalitas...")
        df_krim = spark.read.format("delta").load(TBL_INDEX_KRIMINALITAS)
        # Ambil kolom yang relevan, urutkan berdasarkan indeks tertinggi
        df_krim_selected = df_krim.select(
            "kecamatan", 
            "total_kasus_kriminal", 
            "crime_rate", 
            "indeks_kriminalitas"
        ).orderBy(desc("indeks_kriminalitas"))
        
        kriminalitas_data = [row.asDict() for row in df_krim_selected.collect()]
        logger.info(f"[LOAD] Sukses memuat {len(kriminalitas_data)} data kecamatan (Kriminalitas).")

        # 2. Baca Indeks Kesehatan
        logger.info("[LOAD] Membaca data tbl_index_kesehatan...")
        df_kes = spark.read.format("delta").load(TBL_INDEX_KESEHATAN)
        # Ambil kolom yang relevan, urutkan berdasarkan indeks tertinggi
        df_kes_selected = df_kes.select(
            "kecamatan", 
            "total_kasus_wabah", 
            "incidence_rate", 
            "hfr", 
            "indeks_kesehatan"
        ).orderBy(desc("indeks_kesehatan"))
        
        kesehatan_data = [row.asDict() for row in df_kes_selected.collect()]
        logger.info(f"[LOAD] Sukses memuat {len(kesehatan_data)} data kecamatan (Kesehatan).")

        # 3. Baca Berita Terkini (Silver Layer)
        logger.info("[LOAD] Membaca data berita tbl_clean_news...")
        news_data = []
        if not spark._jsparkSession.catalog().tableExists(f"delta.`{TBL_CLEAN_NEWS}`"):
            logger.warning("[LOAD] Tabel tbl_clean_news belum ada di HDFS. Lewati berita.")
        else:
            df_news = spark.read.format("delta").load(TBL_CLEAN_NEWS)
            # Filter berita yang kecamatan_terdeteksi-nya valid (bukan unknown), batasi 50 berita terbaru
            df_news_filtered = df_news.filter(col("kecamatan_terdeteksi") != "unknown") \
                                      .select("id_berita", "judul", "link", "tanggal_publikasi", "sumber", "kategori", "kecamatan_terdeteksi") \
                                      .orderBy(desc("tanggal_publikasi")) \
                                      .limit(50)
            for row in df_news_filtered.collect():
                row_dict = row.asDict()
                # Convert datetime to string ISO format
                val = row_dict.get("tanggal_publikasi")
                if val is not None:
                    if hasattr(val, "isoformat"):
                        row_dict["tanggal_publikasi"] = val.isoformat()
                    else:
                        row_dict["tanggal_publikasi"] = str(val)
                news_data.append(row_dict)
            logger.info(f"[LOAD] Sukses memuat {len(news_data)} berita terbaru dengan kecamatan terdeteksi.")

        # Gabungkan semua data
        export_payload = {
            "last_updated": datetime.now().isoformat(),
            "kriminalitas": kriminalitas_data,
            "kesehatan": kesehatan_data,
            "berita": news_data
        }

        # Simpan ke JSON local
        logger.info(f"[WRITE] Menyimpan data ke {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(export_payload, f, indent=2, ensure_ascii=False)
            
        logger.info("[WRITE] ✅ Ekspor data JSON selesai!")

    except Exception as e:
        logger.error(f"[ERROR] Proses ekspor gagal: {e}")
    finally:
        spark.stop()
        logger.info("[SHUTDOWN] SparkSession dihentikan.")

if __name__ == "__main__":
    main()
