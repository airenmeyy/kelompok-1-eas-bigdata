import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.WARN)

HDFS_NAMENODE = "hdfs://namenode:8020"
GOLD_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/gold"

spark = (
    SparkSession.builder
    .appName("Kecamatras-VerifyGold")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    .getOrCreate()
)

print("\n" + "="*80)
print("🏆 VERIFIKASI DATA GOLD LAYER: INDEKS KRIMINALITAS (0 - 100) 🏆")
print("="*80)
df_krim = spark.read.format("delta").load(f"{GOLD_DIR}/tbl_index_kriminalitas")
# Tampilkan 10 teratas berdasarkan indeks tertinggi
df_krim.orderBy("indeks_kriminalitas", ascending=False).show(10, truncate=False)

print("\n" + "="*80)
print("🏥 VERIFIKASI DATA GOLD LAYER: INDEKS KESEHATAN LINGKUNGAN (0 - 100) 🏥")
print("="*80)
df_kes = spark.read.format("delta").load(f"{GOLD_DIR}/tbl_index_kesehatan")
# Tampilkan 10 teratas berdasarkan indeks tertinggi
df_kes.orderBy("indeks_kesehatan", ascending=False).show(10, truncate=False)
print("="*80 + "\n")
