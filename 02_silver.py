import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType
)
from pyspark.sql.functions import (
    col, current_timestamp, lit, from_json, lower, regexp_replace, trim, initcap, when
)
from delta import configure_spark_with_delta_pip

# ═══════════════════════════════════════════════════════════════════════════
# KONFIGURASI GLOBAL
# ═══════════════════════════════════════════════════════════════════════════

HDFS_NAMENODE = "hdfs://namenode:8020"

# Path Bronze (Input)
BRONZE_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/bronze"
TBL_RAW_FASKES = f"{BRONZE_DIR}/tbl_raw_faskes_baseline"
TBL_RAW_DISEASE = f"{BRONZE_DIR}/tbl_raw_disease_baseline"
TBL_RAW_CRIME = f"{BRONZE_DIR}/tbl_static_crime_baseline"
TBL_RAW_NEWS = f"{BRONZE_DIR}/tbl_raw_news"

# Path Silver (Output)
SILVER_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/silver"
TBL_CLEAN_FASKES = f"{SILVER_DIR}/tbl_clean_faskes"
TBL_CLEAN_DISEASE = f"{SILVER_DIR}/tbl_clean_disease"
TBL_CLEAN_CRIME = f"{SILVER_DIR}/tbl_clean_crime_baseline"
TBL_CLEAN_NEWS = f"{SILVER_DIR}/tbl_clean_news"

# Checkpoint streaming
CHECKPOINT_DIR = f"{HDFS_NAMENODE}/kecamatras/_checkpoints/silver_news"

# Daftar 31 Kecamatan Surabaya (untuk Geo-parsing)
KECAMATAN_SURABAYA = [
    "asemrowo", "benowo", "bubutan", "bulak", "dukuh pakis", 
    "gayungan", "genteng", "gubeng", "gunung anyar", "jambangan", 
    "karang pilang", "kenjeran", "krembangan", "lakarsantri", 
    "mulyorejo", "pabean cantian", "pakal", "rungkut", "sambikerep", 
    "sawahan", "semampir", "simokerto", "sukolilo", "sukomanunggal", 
    "tambaksari", "tandes", "tegalsari", "tenggilis mejoyo", 
    "wiyung", "wonocolo", "wonokromo"
]

# Skema JSON Berita (dari Layer Ingestion Dinamis)
NEWS_JSON_SCHEMA = StructType([
    StructField("id_berita", StringType(), True),
    StructField("judul", StringType(), True),
    StructField("link", StringType(), True),
    StructField("tanggal_publikasi", StringType(), True),
    StructField("sumber", StringType(), True),
    StructField("kategori", StringType(), True),
    StructField("deskripsi_mentah", StringType(), True)
])

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SilverLayer")

# ═══════════════════════════════════════════════════════════════════════════
# KELAS UTAMA: SilverLayerProcessor
# ═══════════════════════════════════════════════════════════════════════════
class SilverLayerProcessor:
    def __init__(self):
        logger.info("=" * 70)
        logger.info("  KECAMATRAS — Silver Layer Engine (Cleaned & Conformed Data)")
        logger.info("  Tim Anti Gravity | Institut Teknologi Sepuluh Nopember")
        logger.info("=" * 70)

        try:
            builder = (
                SparkSession.builder
                .appName("KECAMATRAS-SilverLayer")
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
                .config("spark.sql.shuffle.partitions", "4")
            )

            self.spark: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")

            logger.info("[INIT] SparkSession berhasil dibuat.")
            logger.info(f"[INIT] Silver Target : {SILVER_DIR}")

        except Exception as e:
            logger.error(f"[INIT] ❌ Gagal membuat SparkSession: {e}")
            sys.exit(1)

    # ═══════════════════════════════════════════════════════════════════
    # BATCH PROCESSING
    # ═══════════════════════════════════════════════════════════════════
    def process_faskes(self):
        """
        Membersihkan data fasilitas kesehatan:
        - Drop duplicates
        - Standardisasi nama kecamatan (Title Case, Trim)
        """
        logger.info("-" * 60)
        logger.info("[SILVER-BATCH] Memproses Data Fasilitas Kesehatan...")
        try:
            df_bronze = self.spark.read.format("delta").load(TBL_RAW_FASKES)
            
            df_clean = (
                df_bronze
                .dropDuplicates()
                .filter(col("kecamatan").isNotNull() & col("nama_faskes").isNotNull())
                .withColumn("kecamatan", trim(initcap(col("kecamatan"))))
                .withColumn("silver_processed_at", current_timestamp())
            )

            df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(TBL_CLEAN_FASKES)
            logger.info(f"[SILVER-BATCH] ✅ tbl_clean_faskes berhasil ditulis ({df_clean.count()} baris).")
        except Exception as e:
            logger.error(f"[SILVER-BATCH] ❌ Gagal memproses Faskes: {e}")

    def process_disease(self):
        """
        Membersihkan data penyakit:
        - Drop duplicates
        - Casting tipe data (jumlah_kasus -> Integer)
        - Standardisasi nama kecamatan (Title Case, Trim)
        """
        logger.info("-" * 60)
        logger.info("[SILVER-BATCH] Memproses Data Kasus Penyakit...")
        try:
            df_bronze = self.spark.read.format("delta").load(TBL_RAW_DISEASE)
            
            df_clean = (
                df_bronze
                .dropDuplicates()
                .filter(col("kecamatan").isNotNull() & col("jenis_penyakit").isNotNull())
                .withColumn("kecamatan", trim(initcap(col("kecamatan"))))
                .withColumn("jumlah_kasus", col("jumlah_kasus").cast(IntegerType()))
                .withColumn("silver_processed_at", current_timestamp())
            )

            df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(TBL_CLEAN_DISEASE)
            logger.info(f"[SILVER-BATCH] ✅ tbl_clean_disease berhasil ditulis ({df_clean.count()} baris).")
        except Exception as e:
            logger.error(f"[SILVER-BATCH] ❌ Gagal memproses Disease: {e}")

    def process_crime(self):
        """
        Membersihkan data kriminalitas:
        - Drop duplicates
        - Casting tipe data (Integer, Float)
        - Standardisasi nama kecamatan (Title Case, Trim)
        """
        logger.info("-" * 60)
        logger.info("[SILVER-BATCH] Memproses Data Kriminalitas...")
        try:
            df_bronze = self.spark.read.format("delta").load(TBL_RAW_CRIME)
            
            df_clean = (
                df_bronze
                .dropDuplicates()
                .filter(col("kecamatan").isNotNull())
                .withColumn("kecamatan", trim(initcap(col("kecamatan"))))
                .withColumn("jumlah_kasus", col("jumlah_kasus").cast(IntegerType()))
                .withColumn("jumlah_penduduk", col("jumlah_penduduk").cast(IntegerType()))
                .withColumn("crime_rate", col("crime_rate").cast(FloatType()))
                .withColumn("silver_processed_at", current_timestamp())
            )

            df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(TBL_CLEAN_CRIME)
            logger.info(f"[SILVER-BATCH] ✅ tbl_clean_crime_baseline berhasil ditulis ({df_clean.count()} baris).")
        except Exception as e:
            logger.error(f"[SILVER-BATCH] ❌ Gagal memproses Crime: {e}")

    # ═══════════════════════════════════════════════════════════════════
    # STREAM PROCESSING
    # ═══════════════════════════════════════════════════════════════════
    def process_news_stream(self, block=True):
        """
        Membaca streaming data berita dari Bronze Layer:
        - Extract JSON payload
        - Text cleansing (lowercase, hapus tanda baca)
        - Geo-parsing (ekstraksi nama kecamatan) tanpa library NLP eksternal.
        - Tulis secara append ke Silver Layer.
        """
        logger.info("-" * 60)
        logger.info("[SILVER-STREAM] Memulai Stream Processing Data Berita...")
        
        try:
            # Pastikan tabel bronze news ada (bisa jadi belum dibuat jika tidak ada aliran data)
            if not self.spark._jsparkSession.catalog().tableExists(f"delta.`{TBL_RAW_NEWS}`"):
                logger.warning(f"[SILVER-STREAM] ⚠️ Tabel {TBL_RAW_NEWS} belum ada di HDFS. Stream dilewati.")
                return None

            df_stream = self.spark.readStream.format("delta").load(TBL_RAW_NEWS)

            # Ekstraksi JSON payload
            df_parsed = df_stream.withColumn("data", from_json(col("raw_value"), NEWS_JSON_SCHEMA))

            # Membongkar field JSON menjadi kolom terpisah
            df_flat = df_parsed.select(
                col("data.id_berita").alias("id_berita"),
                col("data.judul").alias("judul"),
                col("data.link").alias("link"),
                col("data.tanggal_publikasi").cast(TimestampType()).alias("tanggal_publikasi"),
                col("data.sumber").alias("sumber"),
                col("data.kategori").alias("kategori"),
                col("data.deskripsi_mentah").alias("deskripsi_mentah"),
                col("kafka_timestamp")
            )

            # Text Cleansing
            df_clean = (
                df_flat
                .withColumn("judul_clean", lower(regexp_replace(col("judul"), "[^a-zA-Z\\s]", " ")))
                .withColumn("deskripsi_clean", lower(regexp_replace(col("deskripsi_mentah"), "[^a-zA-Z\\s]", " ")))
            )

            # Logika Geo-Parsing Statis menggunakan Regex `rlike`
            geo_expr = None
            for kec in KECAMATAN_SURABAYA:
                # Pola regex `\b` untuk mendeteksi batas kata yang tepat (exact word match)
                pattern = f"\\b{kec}\\b"
                condition = col("deskripsi_clean").rlike(pattern) | col("judul_clean").rlike(pattern)
                
                if geo_expr is None:
                    geo_expr = when(condition, lit(kec.title()))
                else:
                    geo_expr = geo_expr.when(condition, lit(kec.title()))
            
            geo_expr = geo_expr.otherwise(lit("unknown"))

            # Terapkan geo-parsing
            df_geo = (
                df_clean
                .withColumn("kecamatan_terdeteksi", geo_expr)
                .withColumn("silver_processed_at", current_timestamp())
                # Drop kolom mentah yang sudah tidak dipakai (agar rapi)
                .drop("judul_clean", "deskripsi_clean")
            )

            # Tulis stream ke Silver Layer
            query = (
                df_geo.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_DIR)
                .option("mergeSchema", "true")
                .start(TBL_CLEAN_NEWS)
            )

            logger.info("[SILVER-STREAM] ✅ Kafka streaming query (Silver) AKTIF.")
            
            if block:
                query.awaitTermination()
            else:
                return query

        except Exception as e:
            logger.error(f"[SILVER-STREAM] ❌ Gagal memproses Stream News: {e}")
            return None

    def run_all(self):
        logger.info("[RUN] Memulai eksekusi seluruh pipeline Silver Layer...")
        # 1. Jalankan Batch
        self.process_faskes()
        self.process_disease()
        self.process_crime()
        
        # 2. Jalankan Stream (Non-blocking) agar tidak menghalangi jika dijalankan via shell script
        # Biasanya di production, stream dijalankan terpisah atau blocking.
        # Kita buat blocking agar log dapat terpantau, kecuali user menginterupsi.
        self.process_news_stream(block=False)
        
        logger.info("[RUN] Eksekusi batch Selesai. Spark akan dihentikan sebentar lagi jika tidak ada stream blocking.")

    def stop(self):
        if self.spark:
            logger.info("[SHUTDOWN] Menghentikan SparkSession...")
            self.spark.stop()
            logger.info("[SHUTDOWN] SparkSession dihentikan. Selesai.")


if __name__ == "__main__":
    processor = SilverLayerProcessor()
    try:
        # Jalankan semua secara bertahap
        processor.process_faskes()
        processor.process_disease()
        processor.process_crime()
        
        # Stream di-run blocking agar container tetap menyala untuk stream processing
        logger.info("Semua batch job telah selesai. Beralih ke mode Stream Processing (tekan Ctrl+C untuk berhenti).")
        processor.process_news_stream(block=True)
    except KeyboardInterrupt:
        logger.info("Interupsi diterima. Menghentikan...")
    finally:
        processor.stop()
