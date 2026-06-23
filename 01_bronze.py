"""
=============================================================================
 01_bronze.py — KECAMATRAS Bronze Layer Engine
=============================================================================
 Project  : KECAMATRAS (Kecamatan Metrics & Anomaly Tracking of Surabaya)
 Team     : Anti Gravity — Institut Teknologi Sepuluh Nopember (ITS)
 Layer    : Bronze (Raw Data Ingestion — Medallion Architecture)
 Storage  : HDFS (Hadoop Distributed File System) — Pilar 2
 Engine   : Apache Spark (PySpark) + Delta Lake
 ───────────────────────────────────────────────────────────────────────────
 Deskripsi:
   Script ini bertanggung jawab memuat data mentah (raw) ke dalam
   Data Lakehouse tanpa transformasi bisnis apa pun (prinsip Bronze Layer).
   Terdapat 3 subsistem ingestion:
     1. STREAMING  — Membaca topik Kafka `kecamatras-stream` secara real-time
                     dan menyimpannya sebagai Delta Table (append-only).
     2. BATCH CSV  — Membaca file CSV statis (Faskes & Penyakit) dengan
                     separator semikolon (;) dan menyimpannya sebagai Delta.
     3. STATIC     — Menghasilkan DataFrame kriminalitas baseline 2025
                     dari data hardcoded (sumber: Crime_rate_1.png &
                     Crime_rate_2.png) dan menyimpannya sebagai Delta.
 ───────────────────────────────────────────────────────────────────────────
 Storage Target:
   HDFS URI: hdfs://namenode:8020/kecamatras/delta/bronze/
   (Bukan local filesystem — sesuai PRD Section 6.1 & Pilar 2 Hadoop)

 Cara Menjalankan (dari root proyek, di dalam WSL):
   1. Pastikan Docker Compose aktif: docker compose up -d
   2. Jalankan: spark-submit --packages io.delta:delta-spark_2.12:3.0.0 01_bronze.py
 =============================================================================
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import (
    col, current_timestamp, lit, from_json, expr
)
from delta import configure_spark_with_delta_pip

# ═══════════════════════════════════════════════════════════════════════════
# KONFIGURASI GLOBAL
# ═══════════════════════════════════════════════════════════════════════════
# Base directory: root proyek (lokasi script ini berada)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path input raw data (Local Filesystem -> tambahkan file:// agar Spark tidak mencarinya di HDFS)
RAW_FASKES_CSV  = os.path.join(BASE_DIR, "raw_data", "kesehatan", "new-faskes_kecamatan_2023_2026.csv")
RAW_DISEASE_CSV = os.path.join(BASE_DIR, "raw_data", "kesehatan", "new-penyakit_puskesmas_2022_2026-1.csv")
RAW_CRIME_CSV   = os.path.join(BASE_DIR, "raw_data", "kriminalitas", "new-kriminalitas_kecamatan_2023_2026.csv")

# ── HDFS Configuration (Pilar 2: Hadoop Distributed File System) ──
# Semua output Delta Lake disimpan ke HDFS, BUKAN local filesystem.
# URI format: hdfs://<namenode-host>:<rpc-port>/<path>
HDFS_NAMENODE           = "hdfs://namenode:8020"
HDFS_BRONZE_DIR         = f"{HDFS_NAMENODE}/kecamatras/delta/bronze"

# Path output Delta Lake di HDFS (Bronze Layer)
TBL_RAW_NEWS            = f"{HDFS_BRONZE_DIR}/tbl_raw_news"
TBL_RAW_FASKES          = f"{HDFS_BRONZE_DIR}/tbl_raw_faskes_baseline"
TBL_RAW_DISEASE         = f"{HDFS_BRONZE_DIR}/tbl_raw_disease_baseline"
TBL_STATIC_CRIME        = f"{HDFS_BRONZE_DIR}/tbl_static_crime_baseline"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = "kecamatras-stream"
CHECKPOINT_DIR          = f"{HDFS_NAMENODE}/kecamatras/_checkpoints/bronze_news_stream"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KECAMATRAS.Bronze")


# ═══════════════════════════════════════════════════════════════════════════
# CLASS: BronzeLayerEngine
# ═══════════════════════════════════════════════════════════════════════════
class BronzeLayerEngine:
    """
    Engine utama Bronze Layer KECAMATRAS.
    Menginisialisasi SparkSession dengan dukungan Delta Lake,
    lalu menjalankan 3 subsistem ingestion secara berurutan.
    """

    def __init__(self):
        """Inisialisasi SparkSession dengan konfigurasi Delta Lake."""
        logger.info("=" * 70)
        logger.info("  KECAMATRAS — Bronze Layer Engine (Medallion Architecture)")
        logger.info("  Tim Anti Gravity | Institut Teknologi Sepuluh Nopember")
        logger.info("=" * 70)

        try:
            # ── Builder SparkSession dengan paket Delta Lake ──
            builder = (
                SparkSession.builder
                .appName("KECAMATRAS-BronzeLayer")
                # Paket Delta Lake & Kafka untuk Spark 3.x
                .config(
                    "spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
                )
                # Ekstensi SQL Delta Lake
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension"
                )
                # Katalog Delta Lake
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                # ── Hadoop HDFS Configuration (Pilar 2) ──
                .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
                # Konfigurasi tambahan untuk performa
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.memory", "2g")
            )

            # Gunakan configure_spark_with_delta_pip untuk auto-resolve dependency
            self.spark: SparkSession = (
                configure_spark_with_delta_pip(
                    builder,
                    extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
                ).getOrCreate()
            )

            # Set log level Spark agar tidak terlalu verbose
            self.spark.sparkContext.setLogLevel("WARN")

            logger.info("[INIT] SparkSession berhasil dibuat.")
            logger.info(f"[INIT] Spark Version : {self.spark.version}")
            logger.info(f"[INIT] HDFS Namenode : {HDFS_NAMENODE}")
            logger.info(f"[INIT] Delta Bronze  : {HDFS_BRONZE_DIR}")

        except Exception as e:
            logger.error(f"[INIT] GAGAL menginisialisasi SparkSession: {e}")
            sys.exit(1)

    # ═══════════════════════════════════════════════════════════════════
    # SUBSISTEM 1: Streaming Ingestion (Kafka → Delta)
    # ═══════════════════════════════════════════════════════════════════
    def ingest_streaming_news(self) -> None:
        """
        Membaca data streaming dari Kafka topic `kecamatras-stream`
        dan menulisnya secara append-only ke Delta Table `tbl_raw_news`.
        
        Data mentah dari Kafka berisi kolom bawaan:
          - key (binary)   : Kunci pesan Kafka
          - value (binary) : Isi pesan (JSON/Teks berita)
          - topic (string) : Nama topik
          - partition (int) : Nomor partisi
          - offset (long)  : Offset pesan
          - timestamp (ts) : Waktu pesan diterima Kafka
        
        Kita menyimpan kolom `value` yang di-cast ke string beserta
        metadata Kafka lainnya. Tidak ada transformasi bisnis di sini
        (murni raw ingestion sesuai prinsip Bronze Layer).
        """
        logger.info("-" * 60)
        logger.info("[STREAM] Memulai Kafka Streaming Ingestion...")
        logger.info(f"[STREAM] Broker   : {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"[STREAM] Topic    : {KAFKA_TOPIC}")
        logger.info(f"[STREAM] Output   : {TBL_RAW_NEWS}")
        logger.info(f"[STREAM] Checkpoint: {CHECKPOINT_DIR}")

        try:
            # Membaca stream dari Kafka
            df_stream = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                # Mulai dari offset terlama jika belum ada checkpoint
                .option("startingOffsets", "earliest")
                # Fail-safe: jangan gagal jika data loss terjadi
                .option("failOnDataLoss", "false")
                .load()
            )

            # Transformasi minimal: cast value binary → string, tambah timestamp ingestion
            df_raw_news = (
                df_stream
                .selectExpr(
                    "CAST(key AS STRING)   AS kafka_key",
                    "CAST(value AS STRING) AS raw_value",
                    "topic                 AS kafka_topic",
                    "partition             AS kafka_partition",
                    "offset                AS kafka_offset",
                    "timestamp             AS kafka_timestamp"
                )
                # Tambahkan metadata waktu ingestion Bronze Layer
                .withColumn("bronze_ingested_at", current_timestamp())
            )

            # Tulis ke Delta Lake secara append-only (streaming)
            query = (
                df_raw_news.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_DIR)
                .option("mergeSchema", "true")
                .start(TBL_RAW_NEWS)
            )

            logger.info("[STREAM] ✅ Kafka streaming query AKTIF.")
            logger.info("[STREAM] Tekan Ctrl+C untuk menghentikan streaming.")
            logger.info("[STREAM] Stream akan berjalan di background...")

            # Menunggu terminasi (blocking) — dapat di-interrupt dengan Ctrl+C
            query.awaitTermination()

        except Exception as e:
            # Kafka belum aktif atau konfigurasi salah — bukan error fatal
            logger.warning(f"[STREAM] ⚠️  Kafka streaming TIDAK aktif: {e}")
            logger.warning("[STREAM] Pastikan Kafka broker berjalan di Docker.")
            logger.warning("[STREAM] Melanjutkan ke batch ingestion...")

    def start_streaming_background(self) -> None:
        """
        Memulai streaming Kafka di background (non-blocking).
        Cocok saat menjalankan batch ingestion secara bersamaan.
        Streaming akan berhenti otomatis saat SparkSession dihentikan.
        """
        logger.info("-" * 60)
        logger.info("[STREAM] Memulai Kafka Streaming (Background / Non-blocking)...")

        try:
            df_stream = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
            )

            df_raw_news = (
                df_stream
                .selectExpr(
                    "CAST(key AS STRING)   AS kafka_key",
                    "CAST(value AS STRING) AS raw_value",
                    "topic                 AS kafka_topic",
                    "partition             AS kafka_partition",
                    "offset                AS kafka_offset",
                    "timestamp             AS kafka_timestamp"
                )
                .withColumn("bronze_ingested_at", current_timestamp())
            )

            query = (
                df_raw_news.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_DIR)
                .option("mergeSchema", "true")
                .start(TBL_RAW_NEWS)
            )

            logger.info("[STREAM] ✅ Kafka streaming query AKTIF (background).")
            return query

        except Exception as e:
            logger.warning(f"[STREAM] ⚠️  Kafka streaming TIDAK aktif: {e}")
            logger.warning("[STREAM] Melanjutkan ke batch ingestion...")
            return None

    # ═══════════════════════════════════════════════════════════════════
    # SUBSISTEM 2: Batch Ingestion (CSV Statis → Delta)
    # ═══════════════════════════════════════════════════════════════════
    def ingest_batch_faskes(self) -> None:
        """
        Membaca data CSV Fasilitas Kesehatan (HFR baseline) dari file:
          raw_data/kesehatan/new-faskes_kecamatan_2023_2026.csv
        
        Skema CSV (separator = semikolon ';'):
          kode_provinsi; nama_provinsi; kode_kota; nama_kota;
          kode_kecamatan_kemendagri; kode_kecamatan_bps; kecamatan;
          jenis_faskes; penyelenggara_faskes; nama_faskes; periode; tahun
        
        Disimpan secara OVERWRITE ke Delta Table:
          delta/bronze/tbl_raw_faskes_baseline
        """
        logger.info("-" * 60)
        logger.info("[BATCH] Ingesting Data Fasilitas Kesehatan (HFR Baseline)...")
        logger.info(f"[BATCH] Source : {RAW_FASKES_CSV}")
        logger.info(f"[BATCH] Target : {TBL_RAW_FASKES}")

        try:
            # Validasi keberadaan file
            if not os.path.exists(RAW_FASKES_CSV):
                raise FileNotFoundError(
                    f"File CSV Faskes tidak ditemukan: {RAW_FASKES_CSV}"
                )

            # Baca CSV dengan separator semikolon, header, dan inferSchema
            df_faskes: DataFrame = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .option("encoding", "UTF-8")
                .csv(f"file://{RAW_FASKES_CSV}")
            )

            # Tambahkan kolom metadata ingestion
            df_faskes = (
                df_faskes
                .withColumn("bronze_ingested_at", current_timestamp())
                .withColumn("bronze_source_file", lit("new-faskes_kecamatan_2023_2026.csv"))
            )

            # Tulis ke Delta Lake (overwrite karena data baseline historis)
            (
                df_faskes.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(TBL_RAW_FASKES)
            )

            row_count = df_faskes.count()
            col_count = len(df_faskes.columns)
            logger.info(f"[BATCH] ✅ tbl_raw_faskes_baseline berhasil ditulis.")
            logger.info(f"[BATCH]    Jumlah baris  : {row_count:,}")
            logger.info(f"[BATCH]    Jumlah kolom  : {col_count}")
            logger.info(f"[BATCH]    Kolom: {df_faskes.columns}")

            # Preview 5 baris pertama
            logger.info("[BATCH]    Preview data:")
            df_faskes.show(5, truncate=False)

        except FileNotFoundError as fnf:
            logger.error(f"[BATCH] ❌ {fnf}")
        except Exception as e:
            logger.error(f"[BATCH] ❌ Gagal ingest data Faskes: {e}")
            raise

    def ingest_batch_disease(self) -> None:
        """
        Membaca data CSV Kasus Penyakit per Puskesmas (IR baseline) dari file:
          raw_data/kesehatan/new-penyakit_puskesmas_2022_2026-1.csv
        
        Skema CSV (separator = semikolon ';'):
          kode_provinsi; nama_provinsi; kode_kota; nama_kota;
          kode_kecamatan_kemendagri; kode_kecamatan_bps; kecamatan;
          nama_faskes; jenis_penyakit; jumlah_kasus; periode; tahun
        
        Disimpan secara OVERWRITE ke Delta Table:
          delta/bronze/tbl_raw_disease_baseline
        """
        logger.info("-" * 60)
        logger.info("[BATCH] Ingesting Data Kasus Penyakit (IR Baseline)...")
        logger.info(f"[BATCH] Source : {RAW_DISEASE_CSV}")
        logger.info(f"[BATCH] Target : {TBL_RAW_DISEASE}")

        try:
            # Validasi keberadaan file
            if not os.path.exists(RAW_DISEASE_CSV):
                raise FileNotFoundError(
                    f"File CSV Penyakit tidak ditemukan: {RAW_DISEASE_CSV}"
                )

            # Baca CSV dengan separator semikolon, header, dan inferSchema
            df_disease: DataFrame = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .option("encoding", "UTF-8")
                .csv(f"file://{RAW_DISEASE_CSV}")
            )

            # Tambahkan kolom metadata ingestion
            df_disease = (
                df_disease
                .withColumn("bronze_ingested_at", current_timestamp())
                .withColumn("bronze_source_file", lit("new-penyakit_puskesmas_2022_2026-1.csv"))
            )

            # Tulis ke Delta Lake (overwrite karena data baseline historis)
            (
                df_disease.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(TBL_RAW_DISEASE)
            )

            row_count = df_disease.count()
            col_count = len(df_disease.columns)
            logger.info(f"[BATCH] ✅ tbl_raw_disease_baseline berhasil ditulis.")
            logger.info(f"[BATCH]    Jumlah baris  : {row_count:,}")
            logger.info(f"[BATCH]    Jumlah kolom  : {col_count}")
            logger.info(f"[BATCH]    Kolom: {df_disease.columns}")

            # Preview 5 baris pertama
            logger.info("[BATCH]    Preview data:")
            df_disease.show(5, truncate=False)

        except FileNotFoundError as fnf:
            logger.error(f"[BATCH] ❌ {fnf}")
        except Exception as e:
            logger.error(f"[BATCH] ❌ Gagal ingest data Penyakit: {e}")
            raise

    # ═══════════════════════════════════════════════════════════════════
    # SUBSISTEM 3: Static / Hardcoded Crime Baseline (→ Delta)
    # ═══════════════════════════════════════════════════════════════════
    def ingest_static_crime_baseline(self) -> None:
        """
        Membangun DataFrame kriminalitas baseline tahun 2025 secara
        hardcoded. Data ini bersumber dari tabel di file gambar:
          - Crime_rate_1.png (9 kecamatan — Surabaya Selatan/Timur)
          - Crime_rate_2.png (22 kecamatan — Surabaya Barat/Utara/Pusat)
        
        Kolom yang dihasilkan:
          - kecamatan (string)      : Nama kecamatan di Kota Surabaya
          - jumlah_kasus (int)      : Total kasus kriminalitas yang dilaporkan
          - jumlah_penduduk (int)   : Jumlah penduduk kecamatan
          - crime_rate (float)      : Crime Rate per 100.000 penduduk
          - tahun_baseline (int)    : Tahun baseline (2025)
          - sumber_data (string)    : Keterangan asal data
        
        Seluruh 31 kecamatan di Kota Surabaya dicakup.
        Disimpan secara OVERWRITE ke Delta Table:
          delta/bronze/tbl_static_crime_baseline
        """
        logger.info("-" * 60)
        logger.info("[STATIC] Membangun Data Kriminalitas Baseline 2025...")
        logger.info(f"[STATIC] Target : {TBL_STATIC_CRIME}")

        try:
            # ── Skema DataFrame ──
            crime_schema = StructType([
                StructField("kecamatan",        StringType(),  False),
                StructField("jumlah_kasus",     IntegerType(), False),
                StructField("jumlah_penduduk",  IntegerType(), False),
                StructField("crime_rate",       FloatType(),   False),
                StructField("tahun_baseline",   IntegerType(), False),
                StructField("sumber_data",      StringType(),  False),
            ])

            # ── Data Hardcoded: 31 Kecamatan Kota Surabaya ──
            # Sumber: Crime_rate_1.png & Crime_rate_2.png (penelitian referensi)
            # Catatan: Angka jumlah_kasus & penduduk diambil dari tabel di gambar.
            #          Crime rate = (kasus / penduduk) × 100.000
            #          Tahun baseline diset ke 2025 sesuai PRD Sec.5.2.
            crime_data = [
                # ─── Crime_rate_1.png (Surabaya Selatan & Timur) ───
                ("Karang Pilang",    77,  75503,  102.0, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Jambangan",        89,  54212,  164.2, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Gayungan",         50,  43846,  114.0, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Wonocolo",         15,  80034,   18.7, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Tenggilis Mejoyo", 89,  58932,  151.0, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Gunung Anyar",     92,  62342,  147.6, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Rungkut",          39, 123653,   31.5, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Sukolilo",          0, 115913,    0.0, 2025, "Crime_rate_1.png — Referensi Penelitian"),
                ("Mulyorejo",       100,  88214,  113.4, 2025, "Crime_rate_1.png — Referensi Penelitian"),

                # ─── Crime_rate_2.png (Surabaya Barat, Utara, Pusat) ───
                ("Gubeng",            6, 132382,    4.5, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Wonokromo",        75, 153563,   48.8, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Dukuh Pakis",      75,  59345,  126.4, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Wiyung",            0,  76501,    0.0, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Lakarsantri",      23,  65013,   35.4, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Sambikerep",       33,  69076,   47.8, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Tandes",          175,  91784,  190.8, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Sukomanunggal",    82, 104166,   78.7, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Sawahan",         167, 198516,   84.1, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Tegalsari",       144,  97511,  147.6, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Genteng",          70,  58216,  120.3, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Tambaksari",       19,  97511,   19.5, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Kenjeran",        104,  58216,  178.6, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Bulak",           163, 227025,   71.8, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Simokerto",        71, 185294,   38.3, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Semampir",         78,  47839,  163.1, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Pabean Cantian",   71,  73931,   96.0, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Bubutan",          23,  96704,   23.8, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Krembangan",       72, 114866,   62.7, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Asemrowo",         84,  48841,  172.0, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Benowo",           92,  74933,  122.8, 2025, "Crime_rate_2.png — Referensi Penelitian"),
                ("Pakal",            61,  64515,   94.6, 2025, "Crime_rate_2.png — Referensi Penelitian"),
            ]

            # Bangun DataFrame dari data hardcoded
            df_crime: DataFrame = self.spark.createDataFrame(
                data=crime_data,
                schema=crime_schema
            )

            # Tambahkan kolom metadata ingestion
            df_crime = (
                df_crime
                .withColumn("bronze_ingested_at", current_timestamp())
            )

            # Tulis ke Delta Lake (overwrite — data baseline statis)
            (
                df_crime.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(TBL_STATIC_CRIME)
            )

            row_count = df_crime.count()
            logger.info(f"[STATIC] ✅ tbl_static_crime_baseline berhasil ditulis.")
            logger.info(f"[STATIC]    Jumlah kecamatan : {row_count}")
            logger.info(f"[STATIC]    Tahun baseline   : 2025")
            logger.info(f"[STATIC]    Kolom: {df_crime.columns}")

            # Preview data
            logger.info("[STATIC]    Preview data:")
            df_crime.show(10, truncate=False)

        except Exception as e:
            logger.error(f"[STATIC] ❌ Gagal membangun data Crime Baseline: {e}")
            raise

    # ═══════════════════════════════════════════════════════════════════
    # UTILITAS: Verifikasi Delta Tables
    # ═══════════════════════════════════════════════════════════════════
    def verify_delta_tables(self) -> None:
        """
        Membaca ulang seluruh Delta Table Bronze untuk memverifikasi
        bahwa data berhasil ditulis dengan benar.
        """
        logger.info("=" * 60)
        logger.info("[VERIFY] Memverifikasi Delta Tables di Bronze Layer...")

        tables = {
            "tbl_raw_faskes_baseline":   TBL_RAW_FASKES,
            "tbl_raw_disease_baseline":  TBL_RAW_DISEASE,
            "tbl_static_crime_baseline": TBL_STATIC_CRIME,
        }

        # Gunakan Hadoop FileSystem API untuk cek keberadaan path di HDFS
        # (os.path.exists TIDAK bisa digunakan untuk path HDFS)
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            self.spark._jvm.java.net.URI(HDFS_NAMENODE), hadoop_conf
        )

        for table_name, path in tables.items():
            try:
                hdfs_path = self.spark._jvm.org.apache.hadoop.fs.Path(path)
                if fs.exists(hdfs_path):
                    df = self.spark.read.format("delta").load(path)
                    count = df.count()
                    logger.info(
                        f"[VERIFY] ✅ {table_name:35s} | "
                        f"{count:>8,} baris | {len(df.columns)} kolom"
                    )
                else:
                    logger.warning(f"[VERIFY] ⚠️  {table_name} — belum dibuat di HDFS.")
            except Exception as e:
                logger.error(f"[VERIFY] ❌ {table_name} — error: {e}")

        # Verifikasi streaming table (mungkin belum ada jika Kafka off)
        try:
            hdfs_news_path = self.spark._jvm.org.apache.hadoop.fs.Path(TBL_RAW_NEWS)
            if fs.exists(hdfs_news_path):
                df_news = self.spark.read.format("delta").load(TBL_RAW_NEWS)
                count = df_news.count()
                logger.info(
                    f"[VERIFY] ✅ {'tbl_raw_news':35s} | "
                    f"{count:>8,} baris | {len(df_news.columns)} kolom"
                )
            else:
                logger.info(
                    f"[VERIFY] ℹ️  tbl_raw_news — belum ada data di HDFS "
                    f"(Kafka streaming belum dijalankan)."
                )
        except Exception as e:
            logger.warning(f"[VERIFY] ⚠️  tbl_raw_news — {e}")

        logger.info("=" * 60)

    # ═══════════════════════════════════════════════════════════════════
    # MAIN RUNNER
    # ═══════════════════════════════════════════════════════════════════
    def run_batch_ingestion(self) -> None:
        """
        Menjalankan seluruh pipeline batch ingestion Bronze Layer:
          1. Ingest CSV Faskes (HFR)
          2. Ingest CSV Penyakit (IR)
          3. Ingest Static Crime Baseline
          4. Verifikasi semua Delta Table
        
        Catatan: Kafka streaming TIDAK dijalankan di sini secara default.
        Untuk menjalankan streaming, panggil `ingest_streaming_news()`
        atau `start_streaming_background()` secara terpisah.
        """
        start_time = datetime.now()
        logger.info(f"[RUN] Memulai Bronze Layer Batch Ingestion @ {start_time}")
        logger.info(f"[RUN] Base Directory : {BASE_DIR}")
        logger.info(f"[RUN] HDFS Target    : {HDFS_BRONZE_DIR}")

        # Catatan: Direktori HDFS dibuat otomatis oleh Spark saat menulis.
        # Tidak perlu os.makedirs() untuk path HDFS.

        # ── Eksekusi 3 Subsistem Batch ──
        self.ingest_batch_faskes()          # CSV → Delta (Faskes/HFR)
        self.ingest_batch_disease()         # CSV → Delta (Penyakit/IR)
        self.ingest_static_crime_baseline() # Hardcoded → Delta (Crime)

        # ── Verifikasi ──
        self.verify_delta_tables()

        elapsed = datetime.now() - start_time
        logger.info(f"[RUN] ✅ Bronze Layer Batch Ingestion SELESAI.")
        logger.info(f"[RUN]    Durasi total: {elapsed}")

    def run_full_pipeline(self) -> None:
        """
        Menjalankan pipeline lengkap (Batch + Streaming).
        Streaming berjalan secara blocking (awaitTermination).
        Gunakan Ctrl+C untuk menghentikan streaming.
        """
        # Jalankan batch terlebih dahulu
        self.run_batch_ingestion()

        # Kemudian jalankan streaming (blocking)
        logger.info("[RUN] Memulai Kafka Streaming setelah batch selesai...")
        self.ingest_streaming_news()

    def stop(self) -> None:
        """Menghentikan SparkSession dengan aman."""
        if self.spark:
            logger.info("[SHUTDOWN] Menghentikan SparkSession...")
            self.spark.stop()
            logger.info("[SHUTDOWN] SparkSession dihentikan. Selesai.")


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    """
    Cara menjalankan:
    ─────────────────────────────────────────────────────────────────
    MODE 1 — Batch Only (tanpa Kafka):
      spark-submit --packages io.delta:delta-spark_2.12:3.0.0 01_bronze.py
    
    MODE 2 — Batch + Streaming (Kafka harus aktif):
      spark-submit --packages io.delta:delta-spark_2.12:3.0.0 \
        01_bronze.py --stream
    
    MODE 3 — Streaming Only (Kafka harus aktif):
      spark-submit --packages io.delta:delta-spark_2.12:3.0.0 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        01_bronze.py --stream-only
    ─────────────────────────────────────────────────────────────────
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="KECAMATRAS Bronze Layer Engine — Medallion Architecture"
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Jalankan batch ingestion LALU streaming Kafka (blocking)."
    )
    parser.add_argument(
        "--stream-only",
        action="store_true",
        help="Jalankan HANYA streaming Kafka (tanpa batch)."
    )
    args = parser.parse_args()

    engine = BronzeLayerEngine()

    try:
        if args.stream_only:
            # Mode 3: Streaming saja
            logger.info("[MAIN] Mode: Streaming Only")
            engine.ingest_streaming_news()
        elif args.stream:
            # Mode 2: Batch + Streaming
            logger.info("[MAIN] Mode: Batch + Streaming")
            engine.run_full_pipeline()
        else:
            # Mode 1: Batch saja (default)
            logger.info("[MAIN] Mode: Batch Only (default)")
            engine.run_batch_ingestion()
    except KeyboardInterrupt:
        logger.info("\n[MAIN] ⛔ Dihentikan oleh pengguna (Ctrl+C).")
    except Exception as e:
        logger.error(f"[MAIN] ❌ Fatal error: {e}")
        sys.exit(1)
    finally:
        engine.stop()
