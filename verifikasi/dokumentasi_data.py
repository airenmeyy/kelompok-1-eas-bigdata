import logging
from pyspark.sql import SparkSession

# Sembunyikan log INFO Spark agar terminal bersih untuk screenshot dokumentasi
logging.getLogger("py4j").setLevel(logging.ERROR)

HDFS_NAMENODE = "hdfs://namenode:8020"
BRONZE_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/bronze"
SILVER_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/silver"
GOLD_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/gold"

spark = (
    SparkSession.builder
    .appName("Kecamatras-Docs")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "█"*80)
print(" 🥉 DOKUMENTASI BRONZE LAYER (RAW DATA) 🥉")
print("█"*80)
print("Path HDFS: hdfs://namenode:8020/kecamatras/delta/bronze/tbl_raw_news")
try:
    df_bronze = spark.read.format("delta").load(f"{BRONZE_DIR}/tbl_raw_news")
    df_bronze.select("kafka_key", "kafka_topic", "kafka_timestamp").show(5, truncate=False)
    print(f"Total Data Streaming Masuk: {df_bronze.count()} baris\n")
except Exception as e:
    print(f"Tabel Bronze belum ada atau HDFS belum siap. Error: {str(e)}\n")


print("█"*80)
print(" 🥈 DOKUMENTASI SILVER LAYER (CLEANED & PARSED DATA) 🥈")
print("█"*80)
print("Path HDFS: hdfs://namenode:8020/kecamatras/delta/silver/tbl_clean_news")
try:
    df_silver = spark.read.format("delta").load(f"{SILVER_DIR}/tbl_clean_news")
    
    print("Contoh Data Kriminalitas:")
    df_silver.filter(df_silver.kategori == "Kriminalitas").select("id_berita", "kategori", "kecamatan_terdeteksi").show(3, truncate=False)
    
    print("Contoh Data Kesehatan (Wabah/Penyakit):")
    df_silver.filter(df_silver.kategori == "Kesehatan").select("id_berita", "kategori", "kecamatan_terdeteksi").show(3, truncate=False)
    
    print("Contoh Data dengan Kecamatan Terdeteksi:")
    df_silver.filter(df_silver.kecamatan_terdeteksi != "unknown").select("id_berita", "kategori", "kecamatan_terdeteksi").show(3, truncate=False)
    
    print(f"Total Keseluruhan Berita Terekstrak: {df_silver.count()} baris\n")
except Exception as e:
    print("Tabel Silver belum ada.\n")


print("█"*80)
print(" 🥇 DOKUMENTASI GOLD LAYER (FINAL BUSINESS INDEX) 🥇")
print("█"*80)
print("Path HDFS: hdfs://namenode:8020/kecamatras/delta/gold/tbl_index_kriminalitas")
try:
    df_gold_krim = spark.read.format("delta").load(f"{GOLD_DIR}/tbl_index_kriminalitas")
    df_gold_krim.select("kecamatan", "crime_rate", "indeks_kriminalitas").orderBy("indeks_kriminalitas", ascending=False).show(5, truncate=False)
    
    print("Path HDFS: hdfs://namenode:8020/kecamatras/delta/gold/tbl_index_kesehatan")
    df_gold_kes = spark.read.format("delta").load(f"{GOLD_DIR}/tbl_index_kesehatan")
    df_gold_kes.select("kecamatan", "incidence_rate", "hfr", "indeks_kesehatan").orderBy("indeks_kesehatan", ascending=False).show(5, truncate=False)
except Exception as e:
    print("Tabel Gold belum ada.\n")

print("✅ Pengambilan Data Selesai.")
spark.stop()
