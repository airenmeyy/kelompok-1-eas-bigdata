from pyspark.sql import SparkSession

HDFS_NAMENODE = "hdfs://namenode:8020"
SILVER_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/silver"
TBL_CLEAN_NEWS = f"{SILVER_DIR}/tbl_clean_news"

spark = (
    SparkSession.builder
    .appName("Verify-Silver")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    .getOrCreate()
)

print("\n\n" + "="*80)
print("📊 VERIFIKASI DATA SILVER LAYER (CLEAN NEWS)")
print("="*80)

try:
    df = spark.read.format("delta").load(TBL_CLEAN_NEWS)
    print(f"Total Baris Berita: {df.count()}")
    df.show(5, truncate=100)
    print("="*80 + "\n\n")
except Exception as e:
    print(f"Error: {e}")

spark.stop()
