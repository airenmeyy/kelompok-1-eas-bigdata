import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat_ws, lower, udf, max as spark_max, min as spark_min, when, lit, sum as spark_sum, current_timestamp
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import LDA
import os

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("GoldLayer")

# URI HDFS
HDFS_NAMENODE = "hdfs://namenode:8020"
SILVER_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/silver"
GOLD_DIR = f"{HDFS_NAMENODE}/kecamatras/delta/gold"

# Tabel Input
TBL_CLEAN_NEWS = f"{SILVER_DIR}/tbl_clean_news"
TBL_CLEAN_FASKES = f"{SILVER_DIR}/tbl_clean_faskes"
TBL_CLEAN_DISEASE = f"{SILVER_DIR}/tbl_clean_disease"
TBL_CLEAN_CRIME = f"{SILVER_DIR}/tbl_clean_crime_baseline"

# Tabel Output
TBL_INDEX_KRIMINALITAS = f"{GOLD_DIR}/tbl_index_kriminalitas"
TBL_INDEX_KESEHATAN = f"{GOLD_DIR}/tbl_index_kesehatan"

# Daftar Stopwords Bahasa Indonesia
INDONESIAN_STOPWORDS = [
    "yang", "di", "ke", "dari", "pada", "dalam", "untuk", "dengan", "dan", "atau", "ini", "itu", 
    "juga", "sudah", "saya", "anda", "dia", "mereka", "kita", "kami", "akan", "bisa", "ada", 
    "tidak", "bukan", "belum", "sangat", "paling", "lebih", "karena", "sebab", "oleh", "seperti", 
    "sebagai", "saat", "ketika", "setelah", "sebelum", "jika", "kalau", "apabila", "hingga", 
    "sampai", "tentang", "bagi", "menurut", "antara", "terhadap", "kepada", "namun", "tetapi", 
    "tapi", "sedangkan", "lalu", "kemudian", "maka", "jadi", "saja", "lagi", "terus", "pun", 
    "baru", "pernah", "selalu", "sering", "kadang", "banyak", "beberapa", "semua", "setiap", 
    "seluruh", "sebagian", "hal", "hari", "tahun", "bulan", "waktu", "orang", "saudara", 
    "bapak", "ibu", "anak", "surabaya", "kota", "kecamatan", "jawa", "timur"
]

class GoldLayerProcessor:
    def __init__(self):
        logger.info("=" * 70)
        logger.info("  KECAMATRAS — Gold Layer Engine (Business Aggregation & ML)")
        logger.info("  Tim Anti Gravity | Institut Teknologi Sepuluh Nopember")
        logger.info("=" * 70)

        self.spark = (
            SparkSession.builder
            .appName("Kecamatras-Gold")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
            .getOrCreate()
        )
        logger.info("[INIT] SparkSession berhasil dibuat.")

    def run_ml_pipeline(self):
        logger.info("[ML-LDA] Membaca data tbl_clean_news dari Silver Layer...")
        if not self.spark._jsparkSession.catalog().tableExists(f"delta.`{TBL_CLEAN_NEWS}`"):
            logger.warning("[ML-LDA] Tabel tbl_clean_news belum ada di HDFS. Membuat DataFrame kosong...")
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            schema = StructType([
                StructField("id_berita", StringType(), True),
                StructField("judul", StringType(), True),
                StructField("link", StringType(), True),
                StructField("tanggal_publikasi", TimestampType(), True),
                StructField("sumber", StringType(), True),
                StructField("kategori", StringType(), True),
                StructField("deskripsi_mentah", StringType(), True),
                StructField("kafka_timestamp", TimestampType(), True),
                StructField("kecamatan_terdeteksi", StringType(), True),
                StructField("silver_processed_at", TimestampType(), True)
            ])
            df_news = self.spark.createDataFrame([], schema)
        else:
            df_news = self.spark.read.format("delta").load(TBL_CLEAN_NEWS)
        
        # Filter berita yang kecamatannya terdeteksi
        df_filtered = df_news.filter(col("kecamatan_terdeteksi") != "unknown")
        logger.info(f"[ML-LDA] Jumlah berita valid untuk NLP: {df_filtered.count()} baris.")

        if df_filtered.count() == 0:
            logger.warning("[ML-LDA] Tidak ada berita dengan kecamatan terdeteksi. Melewati NLP.")
            return None, None

        # Gabungkan judul dan deskripsi sebagai input dokumen
        df_docs = df_filtered.withColumn("text", concat_ws(" ", col("judul"), col("deskripsi_mentah")))

        logger.info("[ML-LDA] Membangun Spark ML Pipeline (Tokenizer, StopWords, TF-IDF)...")
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        words_data = tokenizer.transform(df_docs)

        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=INDONESIAN_STOPWORDS)
        filtered_data = remover.transform(words_data)

        cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=5000, minDF=2.0)
        cv_model = cv.fit(filtered_data)
        featurized_data = cv_model.transform(filtered_data)
        vocab = cv_model.vocabulary

        idf = IDF(inputCol="raw_features", outputCol="features")
        idf_model = idf.fit(featurized_data)
        rescaled_data = idf_model.transform(featurized_data)

        logger.info("[ML-LDA] Menjalankan Latent Dirichlet Allocation (k=2, seed=42)...")
        lda = LDA(k=2, maxIter=10, seed=42, featuresCol="features")
        lda_model = lda.fit(rescaled_data)

        # ---------------------------------------------------------
        # Model Evaluation (Log-Likelihood & Perplexity)
        # ---------------------------------------------------------
        log_likelihood = lda_model.logLikelihood(rescaled_data)
        log_perplexity = lda_model.logPerplexity(rescaled_data)
        logger.info("[ML-LDA] Evaluasi Model:")
        logger.info(f"   => Log-Likelihood : {log_likelihood:.4f}")
        logger.info(f"   => Perplexity     : {log_perplexity:.4f}")

        # ---------------------------------------------------------
        # Dynamic Topic Labelling (Heuristic Mapping)
        # ---------------------------------------------------------
        logger.info("[ML-LDA] Melakukan Ekstraksi Topik (Dynamic Labelling)...")
        topics = lda_model.describeTopics(maxTermsPerTopic=15)
        topics_collected = topics.collect()

        kriminal_keywords = ["begal", "polisi", "curanmor", "pelaku", "korban", "narkoba", "ditangkap", "pencurian", "sajam", "maling", "curas", "kriminal", "tersangka", "tahanan", "pidana"]
        sehat_keywords = ["pasien", "dbd", "wabah", "diare", "nyamuk", "rsud", "puskesmas", "infeksi", "kesehatan", "kasus", "sakit", "medis", "rumah", "rawat", "penyakit"]

        topic_mapping = {}
        for row in topics_collected:
            topic_idx = row['topic']
            term_indices = row['termIndices']
            top_words = [vocab[idx] for idx in term_indices]
            
            kriminal_score = len(set(top_words).intersection(kriminal_keywords))
            sehat_score = len(set(top_words).intersection(sehat_keywords))
            
            logger.info(f"   => Topic {topic_idx} Top Words: {', '.join(top_words)}")
            logger.info(f"   => Score Kriminal: {kriminal_score} | Score Sehat: {sehat_score}")
            
            if kriminal_score >= sehat_score:
                topic_mapping[topic_idx] = "Kriminalitas"
            else:
                topic_mapping[topic_idx] = "Kesehatan"

        logger.info(f"[ML-LDA] Pemetaan Klaster Final: {topic_mapping}")

        # Prediksi/Transform
        predictions = lda_model.transform(rescaled_data)
        
        # Ekstrak Argmax dari vector topicDistribution
        def get_dominant_topic(v):
            return int(max(range(len(v)), key=v.__getitem__))
        
        argmax_udf = udf(get_dominant_topic, IntegerType())
        df_pred = predictions.withColumn("dominant_topic", argmax_udf(col("topicDistribution")))

        # Map ke string kategori
        def map_topic_name(topic_idx):
            return topic_mapping.get(topic_idx, "Unknown")
            
        map_topic_udf = udf(map_topic_name)
        df_pred = df_pred.withColumn("kategori_ml", map_topic_udf(col("dominant_topic")))

        # ---------------------------------------------------------
        # Agregasi Berita
        # ---------------------------------------------------------
        logger.info("[ML-LDA] Mengagregasi jumlah berita Kriminalitas dan Kesehatan per Kecamatan...")
        
        df_agg_kriminal = df_pred.filter(col("kategori_ml") == "Kriminalitas") \
                                 .groupBy(col("kecamatan_terdeteksi").alias("kecamatan")) \
                                 .agg(spark_sum(lit(1)).alias("total_kasus_berita_kriminal"))

        df_agg_sehat = df_pred.filter(col("kategori_ml") == "Kesehatan") \
                              .groupBy(col("kecamatan_terdeteksi").alias("kecamatan")) \
                              .agg(spark_sum(lit(1)).alias("total_kasus_berita_wabah"))

        return df_agg_kriminal, df_agg_sehat

    def calculate_indexes(self, df_kriminal_news, df_sehat_news):
        logger.info("[INDEX] Memuat Data Silver Baseline...")
        
        # Load Silver Baselines
        df_crime_base = self.spark.read.format("delta").load(TBL_CLEAN_CRIME)
        df_disease_base = self.spark.read.format("delta").load(TBL_CLEAN_DISEASE)
        df_faskes_base = self.spark.read.format("delta").load(TBL_CLEAN_FASKES)

        # Prepare Baseline
        # Asumsikan df_crime_base memiliki 'kecamatan', 'jumlah_kasus', 'jumlah_penduduk'
        df_base = df_crime_base.select("kecamatan", col("jumlah_kasus").alias("kasus_kriminal_baseline"), "jumlah_penduduk")
        
        df_disease_agg = df_disease_base.groupBy("kecamatan").agg(spark_sum("jumlah_kasus").alias("kasus_wabah_baseline"))
        df_faskes_agg = df_faskes_base.groupBy("kecamatan").agg(spark_sum(lit(1)).alias("total_faskes"))

        # ---------------------------------------------------------
        # Analytical Joins (CRIME)
        # ---------------------------------------------------------
        logger.info("[INDEX] Menghitung Indeks Kriminalitas (Analytical Joins)...")
        df_crime_joined = df_base.join(df_kriminal_news, on="kecamatan", how="left")
        df_crime_joined = df_crime_joined.fillna({"total_kasus_berita_kriminal": 0, "kasus_kriminal_baseline": 0})
        
        # TC = kasus_kriminal_baseline + total_kasus_berita_kriminal
        # CR = (TC / jumlah_penduduk) * 100000
        df_crime_calc = df_crime_joined.withColumn(
            "total_kasus_kriminal", 
            col("kasus_kriminal_baseline") + col("total_kasus_berita_kriminal")
        ).withColumn(
            "crime_rate", 
            (col("total_kasus_kriminal") / col("jumlah_penduduk")) * 100000
        )

        # Min-Max Normalization (0 - 100)
        min_cr = df_crime_calc.select(spark_min("crime_rate")).collect()[0][0]
        max_cr = df_crime_calc.select(spark_max("crime_rate")).collect()[0][0]
        
        # Cegah division by zero
        diff_cr = max_cr - min_cr if (max_cr - min_cr) > 0 else 1.0

        df_idx_kriminal = df_crime_calc.withColumn(
            "indeks_kriminalitas",
            ((col("crime_rate") - min_cr) / diff_cr) * 100
        ).withColumn("gold_processed_at", current_timestamp())

        # ---------------------------------------------------------
        # Analytical Joins (HEALTH)
        # ---------------------------------------------------------
        logger.info("[INDEX] Menghitung Indeks Kesehatan (Analytical Joins)...")
        df_health_joined = df_base.select("kecamatan", "jumlah_penduduk") \
                                  .join(df_disease_agg, on="kecamatan", how="left") \
                                  .join(df_sehat_news, on="kecamatan", how="left") \
                                  .join(df_faskes_agg, on="kecamatan", how="left")
        
        df_health_joined = df_health_joined.fillna({
            "kasus_wabah_baseline": 0, 
            "total_kasus_berita_wabah": 0,
            "total_faskes": 0
        })

        # IR = (kasus_wabah_baseline + total_kasus_berita_wabah) / jumlah_penduduk * 100000
        # HFR = total_faskes / jumlah_penduduk * 10000
        df_health_calc = df_health_joined.withColumn(
            "total_kasus_wabah",
            col("kasus_wabah_baseline") + col("total_kasus_berita_wabah")
        ).withColumn(
            "incidence_rate",
            (col("total_kasus_wabah") / col("jumlah_penduduk")) * 100000
        ).withColumn(
            "hfr",
            (col("total_faskes") / col("jumlah_penduduk")) * 10000
        )

        # Min-Max Normalization for IR and HFR
        min_ir = df_health_calc.select(spark_min("incidence_rate")).collect()[0][0]
        max_ir = df_health_calc.select(spark_max("incidence_rate")).collect()[0][0]
        diff_ir = max_ir - min_ir if (max_ir - min_ir) > 0 else 1.0

        min_hfr = df_health_calc.select(spark_min("hfr")).collect()[0][0]
        max_hfr = df_health_calc.select(spark_max("hfr")).collect()[0][0]
        diff_hfr = max_hfr - min_hfr if (max_hfr - min_hfr) > 0 else 1.0

        df_idx_kesehatan = df_health_calc.withColumn(
            "norm_ir", ((col("incidence_rate") - min_ir) / diff_ir) * 100
        ).withColumn(
            "norm_hfr", ((col("hfr") - min_hfr) / diff_hfr) * 100
        ).withColumn(
            "indeks_kesehatan",
            (0.7 * col("norm_ir")) + (0.3 * (100 - col("norm_hfr")))
        ).withColumn("gold_processed_at", current_timestamp())

        return df_idx_kriminal, df_idx_kesehatan

    def save_to_gold(self, df_kriminal, df_kesehatan):
        logger.info("[GOLD] Menyimpan tbl_index_kriminalitas ke Delta Lake HDFS...")
        df_kriminal.write.format("delta").mode("overwrite").save(TBL_INDEX_KRIMINALITAS)
        
        logger.info("[GOLD] Menyimpan tbl_index_kesehatan ke Delta Lake HDFS...")
        df_kesehatan.write.format("delta").mode("overwrite").save(TBL_INDEX_KESEHATAN)

        logger.info("[GOLD] ✅ Penyimpanan Selesai.")

    def export_to_dashboard(self, df_kriminal, df_kesehatan):
        logger.info("[EXPORT] Mengambil data berita dari HDFS Silver...")
        try:
            # Collect data
            kriminal_list = [
                {
                    "kecamatan": row["kecamatan"],
                    "total_kasus_kriminal": int(row["total_kasus_kriminal"]),
                    "crime_rate": float(row["crime_rate"]),
                    "indeks_kriminalitas": float(row["indeks_kriminalitas"])
                }
                for row in df_kriminal.orderBy(col("indeks_kriminalitas").desc()).collect()
            ]
            
            kesehatan_list = [
                {
                    "kecamatan": row["kecamatan"],
                    "total_kasus_wabah": int(row["total_kasus_wabah"]),
                    "incidence_rate": float(row["incidence_rate"]),
                    "hfr": float(row["hfr"]),
                    "indeks_kesehatan": float(row["indeks_kesehatan"])
                }
                for row in df_kesehatan.orderBy(col("indeks_kesehatan").desc()).collect()
            ]

            if not self.spark._jsparkSession.catalog().tableExists(f"delta.`{TBL_CLEAN_NEWS}`"):
                logger.warning("[EXPORT] Tabel tbl_clean_news belum ada di HDFS. Menggunakan list berita kosong.")
                berita_list = []
            else:
                df_news = self.spark.read.format("delta").load(TBL_CLEAN_NEWS)
                # Ambil berita yang valid (kategori Kesehatan atau Kriminalitas) dan urutkan berdasarkan tanggal publikasi desc
                df_news_filtered = df_news.filter(col("kecamatan_terdeteksi") != "unknown") \
                                          .orderBy(col("tanggal_publikasi").desc()) \
                                          .limit(100)
                berita_list = [
                    {
                        "id_berita": row["id_berita"],
                        "judul": row["judul"],
                        "link": row["link"],
                        "tanggal_publikasi": row["tanggal_publikasi"].isoformat() if row["tanggal_publikasi"] and hasattr(row["tanggal_publikasi"], "isoformat") else (str(row["tanggal_publikasi"]) if row["tanggal_publikasi"] else None),
                        "sumber": row["sumber"],
                        "kategori": row["kategori"],
                        "kecamatan_terdeteksi": row["kecamatan_terdeteksi"],
                        "deskripsi_mentah": row["deskripsi_mentah"]
                    }
                    for row in df_news_filtered.collect()
                ]
            
            import json
            from datetime import datetime
            
            dashboard_json = {
                "last_updated": datetime.now().isoformat() + "Z",
                "kriminalitas": kriminal_list,
                "kesehatan": kesehatan_list,
                "berita": berita_list
            }
            
            os.makedirs("dashboard/data", exist_ok=True)
            export_path = "dashboard/data/kecamatras_data.json"
            
            with open(export_path, "w", encoding="utf-8") as f:
                json.dump(dashboard_json, f, indent=2, ensure_ascii=False)
                
            logger.info(f"[EXPORT] ✅ Dashboard data berhasil diexport ke {export_path}")
            
        except Exception as e:
            logger.error(f"[EXPORT] ❌ Gagal mengeksport data dashboard: {e}")

if __name__ == "__main__":
    processor = GoldLayerProcessor()
    df_kriminal_news, df_sehat_news = processor.run_ml_pipeline()
    
    if df_kriminal_news is not None and df_sehat_news is not None:
        df_idx_kriminal, df_idx_kesehatan = processor.calculate_indexes(df_kriminal_news, df_sehat_news)
        processor.save_to_gold(df_idx_kriminal, df_idx_kesehatan)
        processor.export_to_dashboard(df_idx_kriminal, df_idx_kesehatan)
    else:
        logger.error("Gagal mengeksekusi Gold Layer karena ML Pipeline tidak mengembalikan DataFrames valid.")
