# Pertemuan 6: Spark MLlib — Machine Learning Terdistribusi dengan Apache Spark

|                           |                                                                                                |
| ------------------------- | ---------------------------------------------------------------------------------------------- |
| **Mata Kuliah**     | Big Data dan Data Lakehouse                                                                    |
| **Pertemuan**       | 6 (Minggu 6)                                                                                   |
| **Durasi**          | 120 menit                                                                                      |
| **CPMK**            | CPMK-2, CPMK-4                                                                                 |
| **Kemampuan Akhir** | Mahasiswa mampu menerapkan Spark MLlib untuk membangun pipeline machine learning terdistribusi |
| **Metode**          | 📖 Ceramah → 🔧 Praktikum → 🏆 Challenge → 📋 Penugasan Kelompok                            |

---

## Agenda Di Kelas

| Waktu          | Durasi | Kegiatan                                     | Keterangan                                     |
| -------------- | ------ | -------------------------------------------- | ---------------------------------------------- |
| 00:00 – 00:25 | 25 min | 📖 Materi: Ekosistem MLlib & ML Pipeline     | Ceramah interaktif, tanya-jawab                |
| 00:25 – 00:40 | 15 min | 🔧 Lab 1: Feature Engineering dengan MLlib   | VectorAssembler, StringIndexer, StandardScaler |
| 00:40 – 01:00 | 20 min | 🔧 Lab 2: Klasifikasi — Logistic Regression | Train/test split, fit, transform, evaluasi     |
| 01:00 – 01:20 | 20 min | 🔧 Lab 3: Regresi — Linear Regression       | Prediksi nilai kontinu, RMSE, R²              |
| 01:20 – 01:50 | 30 min | 🏆 Challenge — 10 Soal                      | Kerjakan mandiri, dikumpulkan di akhir         |
| 01:50 – 02:00 | 10 min | 📋 Penutup & Preview Pertemuan 7             | Recap kelompok, preview Data Lakehouse         |

> 📊 **Komposisi waktu:** ~20% materi, ~45% hands-on lab, ~25% challenge, ~10% penutup

---

# 📖 BAGIAN 1: MATERI (Di Kelas — 25 menit)

---

## 1.1 Mengapa Machine Learning Butuh Spark?

**Masalah machine learning tradisional (Scikit-learn, R):**

```
┌─────────────────────────────────────────────────────────────────┐
│          MASALAH ML TRADISIONAL vs SPARK MLlib                  │
│                                                                 │
│  Scikit-learn / R:                                              │
│  ┌──────────────────┐                                           │
│  │  Data (20 GB)    │ → RAM satu mesin hanya 16 GB → ❌ CRASH  │
│  └──────────────────┘                                           │
│                                                                 │
│  Spark MLlib:                                                   │
│  ┌──────┐  ┌──────┐  ┌──────┐                                  │
│  │ 7 GB │  │ 7 GB │  │ 6 GB │  ← Data dipartisi ke 3 worker   │
│  └──────┘  └──────┘  └──────┘                                  │
│       Diproses PARALEL → ✅ Sukses                              │
└─────────────────────────────────────────────────────────────────┘
```

> 🏋️ **Analogi: Gym vs. Olimpiade**
>
> - **Scikit-learn** = Gym personal: sempurna untuk latihan harian (dataset kecil–menengah)
> - **Spark MLlib** = Fasilitas olimpiade: dirancang untuk beban sangat besar, ratusan atlet (data) secara bersamaan

| Aspek                       | Scikit-learn              | Spark MLlib                                  |
| --------------------------- | ------------------------- | -------------------------------------------- |
| **Skala data**        | Ratusan MB – beberapa GB | Ratusan GB – TB                             |
| **Komputasi**         | Single machine            | Distributed cluster                          |
| **API**               | Python native             | DataFrame-based Pipeline                     |
| **Model**             | Sangat lengkap            | Subset utama (klasifikasi, regresi, kluster) |
| **Training iteratif** | Baik untuk dataset kecil  | Sangat baik (data di RAM, iterasi cepat)     |

---

## 1.2 Ekosistem Spark MLlib

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPARK MLlib ECOSYSTEM                        │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   spark.ml (DataFrame API)              │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │   │
│  │  │  Transformer  │  │  Estimator   │  │   Pipeline   │  │   │
│  │  │ (transform()) │  │   (fit())    │  │ (chaining)   │  │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Algoritma yang tersedia:                                       │
│  • Klasifikasi: Logistic Regression, Decision Tree, RF, GBT    │
│  • Regresi: Linear Regression, Decision Tree, GBT              │
│  • Kluster: K-Means, Bisecting K-Means, GMM                   │
│  • Reduksi Dimensi: PCA                                        │
│  • Feature Engineering: VectorAssembler, StandardScaler, dll.  │
└─────────────────────────────────────────────────────────────────┘
```

### Konsep Utama MLlib

| Konsep                | Deskripsi                                              | Analogi                              |
| --------------------- | ------------------------------------------------------ | ------------------------------------ |
| **Transformer** | Mengubah DataFrame → DataFrame baru (tidak belajar)   | Timbangan: selalu mengukur sama      |
| **Estimator**   | Belajar dari data (fit), lalu menghasilkan Transformer | Dokter: diagnosa berdasar pengalaman |
| **Pipeline**    | Rangkaian Transformer + Estimator menjadi satu alur    | Lini produksi pabrik                 |
| **Evaluator**   | Mengukur performa model                                | Juri kompetisi                       |

---

## 1.3 ML Pipeline — Alur Kerja Standar

```
┌─────────────────────────────────────────────────────────────────┐
│                     ML PIPELINE STANDAR                         │
│                                                                 │
│  Raw Data                                                       │
│     │                                                           │
│     ▼                                                           │
│  ┌──────────────────┐                                           │
│  │ Feature Eng.     │  ← StringIndexer, VectorAssembler        │
│  │ (Transformers)   │      StandardScaler, OneHotEncoder        │
│  └────────┬─────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │ Train/Test Split │  ← randomSplit([0.8, 0.2])               │
│  └────────┬─────────┘                                           │
│           │                                                     │
│     ┌─────┴─────┐                                               │
│     ▼           ▼                                               │
│  Training     Testing                                           │
│  Data (80%)   Data (20%)                                        │
│     │                                                           │
│     ▼                                                           │
│  ┌──────────────────┐                                           │
│  │  Model Training  │  ← pipeline.fit(train)                   │
│  │  (Estimator)     │                                           │
│  └────────┬─────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │  Prediksi &      │  ← model.transform(test)                 │
│  │  Evaluasi        │      BinaryClassificationEvaluator        │
│  └──────────────────┘      RegressionEvaluator                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1.4 Feature Engineering — Fondasi ML

> 💡 **Key point:** Spark MLlib mengharuskan semua fitur numerik **digabung menjadi satu kolom vektor** sebelum masuk ke algoritma. Inilah tugas `VectorAssembler`.

```python
# Semua fitur → satu kolom "features" (vektor)
# [ipk, angkatan, nilai_tes] → DenseVector([3.8, 2022, 85.0])

assembler = VectorAssembler(
    inputCols=["ipk", "angkatan", "nilai_tes"],
    outputCol="features"
)
```

### Transformers Wajib Diketahui

| Transformer         | Fungsi                          | Contoh Input → Output                     |
| ------------------- | ------------------------------- | ------------------------------------------ |
| `StringIndexer`   | Teks → angka                   | `"Informatika"` → `0.0`               |
| `OneHotEncoder`   | Angka kategori → vektor biner  | `0.0` → `[1, 0, 0]`                   |
| `VectorAssembler` | Beberapa kolom → satu vektor   | `[3.8, 22, 85]` → vektor                |
| `StandardScaler`  | Normalisasi skala fitur         | `[3.8, 2022, 85]` → `[0.2, 1.1, 0.7]` |
| `Binarizer`       | Angka → 0/1 berdasar threshold | `3.8` → `1` (jika threshold = 3.5)    |

---

## 1.5 Potensi Penggunaan Spark MLlib di Industri

> 🌍 **Spark MLlib digunakan di mana saja ada data besar + kebutuhan prediksi/pengelompokan secara otomatis.**

### Peta Penggunaan per Sektor

| Sektor                               | Masalah                                         | Solusi dengan MLlib                                         | Model yang Dipakai               |
| ------------------------------------ | ----------------------------------------------- | ----------------------------------------------------------- | -------------------------------- |
| **🛒 E-Commerce**              | Produk apa yang akan dibeli pelanggan?          | Rekomendasi produk dari riwayat transaksi jutaan user       | ALS (Collaborative Filtering)    |
| **🏦 Keuangan & Perbankan**    | Apakah transaksi ini penipuan?                  | Deteksi fraud real-time dari miliaran transaksi             | Random Forest, GBT               |
| **📡 Telekomunikasi**          | Pelanggan mana yang akan berhenti berlangganan? | Prediksi churn pelanggan setiap bulan                       | Logistic Regression, GBT         |
| **🏥 Kesehatan**               | Apakah pasien ini berisiko tinggi?              | Klasifikasi risiko penyakit dari rekam medis skala nasional | Decision Tree, Random Forest     |
| **🚗 Transportasi & Logistik** | Berapa lama perjalanan ini?                     | Prediksi durasi perjalanan / ETA dari GPS + cuaca           | Linear Regression, GBT           |
| **🎬 Media & Streaming**       | Film apa yang cocok untuk user ini?             | Personalisasi konten dari ratusan juta penonton             | ALS, K-Means                     |
| **🏭 Manufaktur**              | Kapan mesin ini akan rusak?                     | Predictive maintenance dari sensor IoT                      | Linear Regression, Random Forest |
| **🌾 Pertanian**               | Area mana yang butuh irigasi?                   | Segmentasi lahan dari data satelit & cuaca                  | K-Means, Bisecting K-Means       |
| **📰 Media Sosial**            | Konten mana yang relevan untuk user?            | Klasifikasi sentimen & topik dari miliaran post             | Logistic Regression, NaiveBayes  |
| **⚡ Energi**                  | Berapa kebutuhan listrik besok?                 | Prediksi konsumsi energi dari data historis                 | Linear Regression, GBT           |

### Contoh: Mengapa MLlib, Bukan Scikit-learn?

```
┌─────────────────────────────────────────────────────────────────┐
│           KAPAN MEMILIH MLlib vs SCIKIT-LEARN?                  │
│                                                                 │
│  Dataset < 10 GB, satu server          → Scikit-learn ✅       │
│  Dataset > 10 GB, butuh cluster        → Spark MLlib ✅        │
│  Prototype / eksperimen cepat          → Scikit-learn ✅       │
│  Production pipeline, data terus tumbuh → Spark MLlib ✅      │
│  Tim data kecil, infrastruktur sederhana → Scikit-learn ✅     │
│  Integrasi dengan HDFS / Data Lakehouse  → Spark MLlib ✅      │
└─────────────────────────────────────────────────────────────────┘
```

> 💡 **Tidak ada konkurensi:** Banyak perusahaan menggunakan **keduanya** — Scikit-learn untuk prototyping cepat, lalu migrasi ke MLlib ketika data sudah terlalu besar.

---

## 1.6 Katalog Model Spark MLlib

### 🔵 Klasifikasi — Memprediksi Kategori

> **Kapan dipakai:** Output adalah **label diskrit** (ya/tidak, kategori A/B/C, spam/bukan spam)

| Model                                 | Kelebihan                                  | Kekurangan                        | Cocok untuk                                |
| ------------------------------------- | ------------------------------------------ | --------------------------------- | ------------------------------------------ |
| **Logistic Regression**         | Cepat, mudah diinterpretasi, probabilistik | Linier, lemah untuk pola kompleks | Churn prediction, deteksi spam             |
| **Decision Tree**               | Mudah divisualisasi, tidak perlu scaling   | Mudah overfit, tidak stabil       | Klasifikasi sederhana, interpretable model |
| **Random Forest**               | Akurat, tahan overfit, feature importance  | Lambat, sulit diinterpretasi      | Fraud detection, klasifikasi medis         |
| **Gradient Boosted Tree (GBT)** | Sangat akurat, menang di banyak kompetisi  | Butuh tuning, lambat di training  | Churn, fraud, CTR prediction               |
| **Linear SVM**                  | Efektif untuk data high-dimensional        | Tidak ada probabilitas, linier    | Text classification, sentiment             |
| **Naive Bayes**                 | Sangat cepat, cocok untuk teks             | Asumsi fitur independen           | Spam filter, klasifikasi teks              |
| **Multilayer Perceptron (MLP)** | Bisa tangkap pola non-linier               | Butuh banyak data, lambat         | Pattern recognition kompleks               |

### 🟠 Regresi — Memprediksi Nilai Kontinu

> **Kapan dipakai:** Output adalah **angka** (harga, durasi, suhu, konsumsi energi)

| Model                                    | Kelebihan                                | Kekurangan             | Cocok untuk                          |
| ---------------------------------------- | ---------------------------------------- | ---------------------- | ------------------------------------ |
| **Linear Regression**              | Cepat, interpretable, baseline yang kuat | Hanya pola linier      | Prediksi harga, ETA, konsumsi energi |
| **Decision Tree Regressor**        | Non-linier, tidak perlu scaling          | Mudah overfit          | Prediksi dengan interaksi fitur      |
| **Random Forest Regressor**        | Akurat, robust terhadap outlier          | Sulit diinterpretasi   | Prediksi harga properti, cuaca       |
| **GBT Regressor**                  | Sangat akurat                            | Butuh tuning, lambat   | Prediksi kompleks: saham, demand     |
| **Generalized Linear Model (GLM)** | Fleksibel (Poisson, Gamma, dll.)         | Lebih kompleks dari LR | Count data, distribusi non-normal    |
| **Isotonic Regression**            | Menjamin output monoton                  | Terbatas hanya monoton | Kalibrasi probabilitas               |

### 🟢 Kluster — Mengelompokkan Tanpa Label

> **Kapan dipakai:** **Tidak ada label**, ingin menemukan pola/kelompok tersembunyi dalam data

| Model                                       | Kelebihan                                   | Kekurangan                         | Cocok untuk                                 |
| ------------------------------------------- | ------------------------------------------- | ---------------------------------- | ------------------------------------------- |
| **K-Means**                           | Cepat, sederhana, skalabel                  | Harus tentukan K, sensitif outlier | Segmentasi pelanggan, pengelompokan dokumen |
| **Bisecting K-Means**                 | Lebih baik dari K-Means untuk kluster besar | Hasil hierarki, lebih kompleks     | Hierarki segmentasi                         |
| **Gaussian Mixture Model (GMM)**      | Soft assignment (probabilistik)             | Lambat, butuh banyak data          | Deteksi anomali, segmentasi halus           |
| **LDA (Latent Dirichlet Allocation)** | Khusus topic modeling                       | Hanya untuk teks                   | Mengelompokkan topik dari artikel/berita    |

### 🟣 Rekomendasi

> **Kapan dipakai:** Menemukan item (produk, film, lagu) yang cocok untuk user tertentu

| Model                                     | Kelebihan                               | Cocok untuk                                |
| ----------------------------------------- | --------------------------------------- | ------------------------------------------ |
| **ALS (Alternating Least Squares)** | Efisien untuk sparse matrix skala besar | Sistem rekomendasi (e-commerce, streaming) |

### ⚪ Reduksi Dimensi

> **Kapan dipakai:** Terlalu banyak fitur, ingin **kompres** tanpa kehilangan informasi penting

| Model                                        | Kelebihan                     | Cocok untuk                                |
| -------------------------------------------- | ----------------------------- | ------------------------------------------ |
| **PCA (Principal Component Analysis)** | Mengurangi fitur, visualisasi | Pre-processing sebelum klasifikasi/kluster |

### Panduan Cepat: Pilih Model yang Tepat

```
┌─────────────────────────────────────────────────────────────────┐
│              DECISION TREE: PILIH MODEL MLlib                   │
│                                                                 │
│  Apakah ada label/target?                                       │
│  ├─ YA → Output berupa kategori?                                │
│  │        ├─ YA → KLASIFIKASI                                   │
│  │        │        ├─ Butuh interpretasi?  → Logistic Reg / DT  │
│  │        │        ├─ Butuh akurasi tinggi? → Random Forest/GBT │
│  │        │        └─ Data teks?           → Naive Bayes / SVM  │
│  │        └─ TIDAK → REGRESI                                    │
│  │                   ├─ Pola linier?       → Linear Regression  │
│  │                   └─ Pola kompleks?     → GBT Regressor      │
│  │                                                              │
│  └─ TIDAK → CLUSTERING                                          │
│              ├─ Jumlah kluster diketahui? → K-Means             │
│              ├─ Butuh probabilitas?       → GMM                  │
│              └─ Data teks/topik?          → LDA                 │
└─────────────────────────────────────────────────────────────────┘
```

---

# 🔧 BAGIAN 2: HANDS-ON LAB (Di Kelas)

> **Platform:** Google Colab (recommended)
>
> Setup di Colab:
>
> ```python
> !pip install pyspark
> ```

---

## 🔧 Lab 1: Feature Engineering dengan MLlib (15 menit)

### Tujuan

Mempersiapkan data mentah menjadi format yang siap digunakan oleh algoritma MLlib.

### Task 1.1: Setup & Dataset

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("Lab Pertemuan 6 - MLlib") \
    .master("local[*]") \
    .getOrCreate()

# Dataset mahasiswa dengan fitur tambahan untuk ML
data = [
    ("Budi",    "Informatika",      3.80, 2022, 85, "Lulus"),
    ("Ani",     "Sistem Informasi", 3.50, 2022, 78, "Lulus"),
    ("Citra",   "Informatika",      3.90, 2021, 92, "Lulus"),
    ("Dedi",    "Sistem Informasi", 3.20, 2023, 65, "Tidak Lulus"),
    ("Eka",     "Informatika",      3.60, 2021, 80, "Lulus"),
    ("Fajar",   "Data Science",     3.70, 2022, 88, "Lulus"),
    ("Gita",    "Data Science",     3.40, 2023, 70, "Tidak Lulus"),
    ("Hadi",    "Informatika",      2.90, 2021, 55, "Tidak Lulus"),
    ("Indah",   "Sistem Informasi", 3.80, 2022, 87, "Lulus"),
    ("Joko",    "Data Science",     3.10, 2023, 60, "Tidak Lulus"),
    ("Kartika", "Informatika",      3.95, 2021, 95, "Lulus"),
    ("Lukman",  "Data Science",     3.60, 2022, 82, "Lulus"),
    ("Maya",    "Sistem Informasi", 3.30, 2023, 68, "Tidak Lulus"),
    ("Nadia",   "Informatika",      3.70, 2022, 89, "Lulus"),
    ("Omar",    "Data Science",     3.85, 2021, 91, "Lulus"),
    ("Putri",   "Informatika",      2.80, 2023, 52, "Tidak Lulus"),
    ("Rudi",    "Data Science",     3.55, 2022, 76, "Lulus"),
    ("Sari",    "Sistem Informasi", 3.45, 2021, 73, "Lulus"),
    ("Tono",    "Informatika",      3.15, 2023, 63, "Tidak Lulus"),
    ("Udin",    "Data Science",     3.75, 2022, 86, "Lulus"),
]

columns = ["nama", "jurusan", "ipk", "angkatan", "nilai_tes", "status"]
df = spark.createDataFrame(data, columns)
df.show()
```

### Task 1.2: StringIndexer — Encode Fitur Kategorik

```python
# Encode kolom "jurusan" (teks) menjadi angka
jurusan_indexer = StringIndexer(inputCol="jurusan", outputCol="jurusan_idx")

# Encode kolom "status" (label target)
label_indexer = StringIndexer(inputCol="status", outputCol="label")

# Fit dan transform
df_indexed = jurusan_indexer.fit(df).transform(df)
df_indexed = label_indexer.fit(df_indexed).transform(df_indexed)

df_indexed.select("nama", "jurusan", "jurusan_idx", "status", "label").show()
```

### Task 1.3: VectorAssembler — Gabungkan Fitur ke Satu Vektor

```python
# Gabungkan semua fitur numerik menjadi satu kolom "features"
assembler = VectorAssembler(
    inputCols=["ipk", "angkatan", "nilai_tes", "jurusan_idx"],
    outputCol="features_raw"
)
df_assembled = assembler.transform(df_indexed)
df_assembled.select("nama", "features_raw", "label").show(5, truncate=False)
```

### Task 1.4: StandardScaler — Normalisasi Skala

```python
# Normalisasi fitur agar semua skala seragam
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)
df_scaled = scaler.fit(df_assembled).transform(df_assembled)
df_scaled.select("nama", "features", "label").show(5, truncate=False)
```

> 💡 **Mengapa StandardScaler penting?**
> `angkatan` bernilai ~2022, sedangkan `ipk` bernilai ~3.5. Tanpa normalisasi, fitur berskala besar akan mendominasi dan merusak model.

---

## 🔧 Lab 2: Klasifikasi Logistic Regression (20 menit)

### Tujuan

Membangun model klasifikasi untuk memprediksi status kelulusan mahasiswa.

### Task 2.1: Bangun Pipeline Klasifikasi

```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# --- 1. Definisikan tahapan pipeline ---
jurusan_indexer = StringIndexer(inputCol="jurusan", outputCol="jurusan_idx")
label_indexer   = StringIndexer(inputCol="status",  outputCol="label")

assembler = VectorAssembler(
    inputCols=["ipk", "angkatan", "nilai_tes", "jurusan_idx"],
    outputCol="features_raw"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100
)

# --- 2. Rangkai pipeline ---
pipeline = Pipeline(stages=[
    jurusan_indexer,
    label_indexer,
    assembler,
    scaler,
    lr
])
```

### Task 2.2: Train/Test Split & Training

```python
# Split data: 80% train, 20% test (seed untuk reproducibility)
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Jumlah data training : {train_df.count()}")
print(f"Jumlah data testing  : {test_df.count()}")

# Latih model
model = pipeline.fit(train_df)
print("✅ Model berhasil dilatih!")
```

### Task 2.3: Prediksi & Evaluasi

```python
# Prediksi pada data test
predictions = model.transform(test_df)
predictions.select("nama", "status", "label", "prediction", "probability").show(truncate=False)

# Evaluasi model
evaluator_auc = BinaryClassificationEvaluator(
    labelCol="label",
    metricName="areaUnderROC"
)
evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

auc = evaluator_auc.evaluate(predictions)
acc = evaluator_acc.evaluate(predictions)

print(f"AUC  (Area Under ROC) : {auc:.4f}")
print(f"Akurasi               : {acc:.4f}")
```

### Task 2.4: Interpretasi Koefisien

```python
# Lihat bobot koefisien model (feature importance)
lr_model = model.stages[-1]
print("Koefisien (bobot) per fitur:")
print(lr_model.coefficients)
print(f"\nIntercept: {lr_model.intercept:.4f}")
```

> 📝 **Interpretasi:** Koefisien positif → fitur meningkatkan probabilitas "Lulus".
> Fitur dengan koefisien terbesar (absolut) = fitur paling berpengaruh.

---

## 🔧 Lab 3: Regresi Linear (20 menit)

### Tujuan

Membangun model regresi untuk memprediksi nilai tes berdasarkan IPK dan angkatan.

### Task 3.1: Bangun Pipeline Regresi

```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Kita memprediksi "nilai_tes" dari "ipk" dan "angkatan"
assembler_reg = VectorAssembler(
    inputCols=["ipk", "angkatan"],
    outputCol="features_reg"
)

scaler_reg = StandardScaler(
    inputCol="features_reg",
    outputCol="features_scaled_reg",
    withMean=True,
    withStd=True
)

lr_reg = LinearRegression(
    featuresCol="features_scaled_reg",
    labelCol="nilai_tes",
    maxIter=100,
    regParam=0.1
)

pipeline_reg = Pipeline(stages=[assembler_reg, scaler_reg, lr_reg])
```

### Task 3.2: Training & Prediksi

```python
train_reg, test_reg = df.randomSplit([0.8, 0.2], seed=42)

model_reg = pipeline_reg.fit(train_reg)
print("✅ Model regresi berhasil dilatih!")

pred_reg = model_reg.transform(test_reg)
pred_reg.select("nama", "ipk", "angkatan", "nilai_tes", "prediction").show()
```

### Task 3.3: Evaluasi Regresi

```python
evaluator_reg = RegressionEvaluator(
    labelCol="nilai_tes",
    predictionCol="prediction"
)

rmse = evaluator_reg.evaluate(pred_reg, {evaluator_reg.metricName: "rmse"})
mae  = evaluator_reg.evaluate(pred_reg, {evaluator_reg.metricName: "mae"})
r2   = evaluator_reg.evaluate(pred_reg, {evaluator_reg.metricName: "r2"})

print(f"RMSE  (Root Mean Sq Error) : {rmse:.4f}")
print(f"MAE   (Mean Absolute Err)  : {mae:.4f}")
print(f"R²    (Koefisien Deter.)   : {r2:.4f}")
```

> 📊 **Interpretasi metrik regresi:**
>
> - **RMSE**: rata-rata jarak prediksi dari nilai aktual (semakin kecil = semakin baik)
> - **MAE**: rata-rata error absolut (lebih tahan terhadap outlier daripada RMSE)
> - **R²**: proporsi variansi yang dijelaskan oleh model (0–1, mendekati 1 = lebih baik)

### Task 3.4: Ringkasan Model Regresi

```python
lr_reg_model = model_reg.stages[-1]
print(f"Koefisien : {lr_reg_model.coefficients}")
print(f"Intercept : {lr_reg_model.intercept:.4f}")
print(f"\nRingkasan training:")
print(f"  Iterasi  : {lr_reg_model.summary.totalIterations}")
print(f"  R² train : {lr_reg_model.summary.r2:.4f}")
print(f"  RMSE train: {lr_reg_model.summary.rootMeanSquaredError:.4f}")
```

---

# 📝 BAGIAN 3: TUGAS KELOMPOK

---

> **Jenis:** Kelompok (4–5 orang, sesuai pembagian kelompok kelas)
> **Deadline:** Sebelum Pertemuan 7 — UTS (satu minggu via LMS)
> **Platform:** Google Colab (wajib) — submit link notebook yang bisa diakses publik + file PDF laporan singkat

---

## Deskripsi Tugas: "Membangun Sistem ML Terdistribusi untuk Keputusan Bisnis"

Bayangkan kelompok kalian adalah tim Data Science di sebuah perusahaan. Manajemen menghadapi **masalah bisnis nyata** dan menugaskan kalian untuk membangun sistem prediksi menggunakan **Spark MLlib** — bukan Scikit-learn, bukan Pandas, bukan model AI siap pakai.

### Syarat Dataset

Kelompok **wajib mencari dataset sendiri dari Kaggle** yang memenuhi syarat berikut:

| Syarat            | Keterangan                                                                         |
| ----------------- | ---------------------------------------------------------------------------------- |
| **Ukuran**  | Minimal **100.000 baris** (bukan dataset tutorial seperti Titanic/Iris) |
| **Format**  | CSV atau JSON                                                                      |
| **Fitur**   | Minimal**8 kolom**, campuran numerik dan kategorik                           |
| **Target**  | Ada kolom yang bisa dijadikan label klasifikasi ATAU nilai regresi                 |
| **Konteks** | Punya latar belakang bisnis yang jelas (bukan data sintetis random)                |

> 💡 **Ide topik:** penerbangan (delay/cancel), e-commerce (churn/return), kesehatan (penyakit), cuaca (prediksi suhu), HR analytics (attrition), kejahatan kota, kualitas udara, dll.

---

## Struktur Tugas & Deliverable

### 📌 Fase 1 — Pemahaman Data & Bisnis (Bobot: 20%)

**Kerjakan:**

1. Tuliskan **latar belakang bisnis**: masalah apa yang dipecahkan, siapa penggunanya, dan apa keputusan yang dibantu oleh model ini.
2. Lakukan **eksplorasi data komprehensif** menggunakan **PySpark** (bukan Pandas):
   - Distribusi setiap fitur (gunakan `describe()`, `groupBy`, `countDistinct`)
   - Deteksi dan tangani nilai null — **jelaskan strategi penanganannya** (drop, fill mean, fill mode, dll.)
   - Deteksi outlier menggunakan rentang IQR lewat Spark SQL
   - Visualisasi distribusi **minimal 4 fitur** (boleh konversi ke Pandas hanya untuk charting)
3. Rumuskan **dua pertanyaan prediksi** yang akan dijawab oleh model:
   - **Pertanyaan 1:** Klasifikasi (contoh: "Apakah penerbangan ini akan delay?")
   - **Pertanyaan 2:** Regresi (contoh: "Berapa menit keterlambatan yang diperkirakan?")

> ⚠️ **Penting:** Eksplorasi harus menggunakan Spark. Jika ditemukan Pandas di tahap ini, nilai Fase 1 dikurangi 50%.

---

### 📌 Fase 2 — Feature Engineering (Bobot: 20%)

**Kerjakan:**

1. Desain **skema feature engineering** yang sesuai dengan karakteristik data:

   - Tentukan kolom mana yang menjadi fitur, mana yang dibuang, dan **jelaskan alasannya**
   - Handle kolom kategorik dengan `StringIndexer` atau `OneHotEncoder` — **pilih salah satu dan jelaskan mengapa**
   - Handle skala fitur dengan `StandardScaler` atau `MinMaxScaler` — **pilih dan justifikasi**
   - Buat minimal **satu fitur turunan** (feature derived) dari kombinasi kolom yang ada

   Contoh fitur turunan: `rasio_delay = delay_departure / jarak_tempuh`, `age_group = binning usia`, dll.
2. Tampilkan **perbandingan sebelum dan sesudah** feature engineering untuk 5 baris data (gunakan `show(5, truncate=False)`).
3. Dokumentasikan keputusan engineering dalam tabel markdown di dalam notebook:

   | Kolom Asal | Transformasi | Kolom Baru | Alasan |
   | ---------- | ------------ | ---------- | ------ |
   | ...        | ...          | ...        | ...    |

---

### 📌 Fase 3 — Perbandingan Model (Bobot: 35%)

Ini adalah **inti tugas**. Kelompok harus membangun dan membandingkan **minimal 3 model** untuk masing-masing tugas prediksi.

#### 3A — Klasifikasi (pilih 3 dari daftar ini)

| Model                 | Import                                                           |
| --------------------- | ---------------------------------------------------------------- |
| Logistic Regression   | `from pyspark.ml.classification import LogisticRegression`     |
| Decision Tree         | `from pyspark.ml.classification import DecisionTreeClassifier` |
| Random Forest         | `from pyspark.ml.classification import RandomForestClassifier` |
| Gradient Boosted Tree | `from pyspark.ml.classification import GBTClassifier`          |
| Naive Bayes           | `from pyspark.ml.classification import NaiveBayes`             |

Untuk setiap model klasifikasi:

- Bangun **Pipeline lengkap** (feature eng → model)
- Split data **70/20/10** (train/val/test) — bukan 80/20 biasa
- Evaluasi dengan: **AUC, Accuracy, Precision, Recall, F1-Score**
- Tampilkan **confusion matrix** (hitung manual dengan Spark SQL: TP, FP, TN, FN)

#### 3B — Regresi (pilih 2 dari daftar ini)

| Model                   | Import                                                      |
| ----------------------- | ----------------------------------------------------------- |
| Linear Regression       | `from pyspark.ml.regression import LinearRegression`      |
| Decision Tree Regressor | `from pyspark.ml.regression import DecisionTreeRegressor` |
| Random Forest Regressor | `from pyspark.ml.regression import RandomForestRegressor` |
| GBT Regressor           | `from pyspark.ml.regression import GBTRegressor`          |

Untuk setiap model regresi:

- Evaluasi dengan: **RMSE, MAE, R²**
- Coba **minimal satu hyperparameter berbeda** (contoh: ubah `numTrees` di RF, atau `maxDepth` di DT) dan bandingkan hasilnya

#### 3C — Tabel Perbandingan Wajib

Di akhir Fase 3, buat **satu tabel ringkasan** (dalam notebook) yang membandingkan semua model:

| Model | Tipe | AUC / R² | RMSE (jika regresi) | Waktu Training | Rekomendasi? |
| ----- | ---- | --------- | ------------------- | -------------- | ------------ |
| ...   | ...  | ...       | ...                 | ...            | ✅ / ❌      |

---

### 📌 Fase 4 — Analisis & Rekomendasi Bisnis (Bobot: 15%)

**Kerjakan (dalam format narasi, bukan hanya output kode):**

1. **Pilih model terbaik** untuk masing-masing tugas dan **justifikasi secara bisnis** — bukan hanya berdasar metrik tertinggi. Pertimbangkan: interpretabilitas, kecepatan, biaya kesalahan (apakah FP atau FN lebih mahal untuk kasus ini?).
2. **Analisis Feature Importance**: Tampilkan 5 fitur paling berpengaruh pada model Random Forest atau GBT. Jelaskan mengapa fitur tersebut masuk akal dari perspektif bisnis.
3. **Simulasi keputusan**: Buat 3 baris data fiktif yang mencerminkan 3 profil pengguna berbeda, lakukan prediksi, dan interpretasikan hasilnya dalam kalimat bisnis (contoh: _"Penumpang rute A-B dengan maskapai C memiliki probabilitas 78% mengalami keterlambatan > 30 menit."_).
4. **Batasan model**: Sebutkan minimal 2 kelemahan atau kondisi di mana model ini **tidak boleh dipercaya** (contoh: dataset lama, distribusi data berubah, dll.).

---

### 📌 Fase 5 — Laporan & Presentasi (Bobot: 10%)

**Deliverable akhir:**

1. **Google Colab Notebook** — bersih, berurutan, dapat dijalankan ulang dari awal ke akhir tanpa error. Setiap sel harus memiliki judul markdown yang jelas.
2. **Laporan PDF (maks. 6 halaman)** — format bebas, berisi:

   - Latar belakang & pertanyaan bisnis
   - Ringkasan dataset
   - Keputusan feature engineering
   - Tabel perbandingan model
   - Rekomendasi bisnis & batasan
3. **Presentasi di Pertemuan 7 (5 menit + 3 menit tanya jawab)**:

   - Wajib demo notebook live
   - Setiap anggota kelompok harus siap menjawab pertanyaan tentang bagian yang mereka kerjakan

---

## Pembagian Kerja yang Disarankan

| Peran                        | Tanggung Jawab Utama                                                 |
| ---------------------------- | -------------------------------------------------------------------- |
| **Data Engineer**      | Cari dataset, setup Colab, load data, handle null & outlier          |
| **Feature Engineer**   | Desain transformasi, buat fitur turunan, dokumentasi skema           |
| **ML Engineer 1**      | Bangun & evaluasi model klasifikasi (3 model)                        |
| **ML Engineer 2**      | Bangun & evaluasi model regresi (2 model), hyperparameter tuning     |
| **Analyst / Reporter** | Analisis bisnis, feature importance, simulasi keputusan, laporan PDF |

> 💬 **Setiap anggota harus punya kontribusi kode yang bisa dibuktikan di notebook.** Gunakan komentar `# [Nama]: bagian ini dikerjakan oleh ...` di awal setiap sel besar.

---

## Rubrik Penilaian

### Dimensi 1: Pemahaman Data & Bisnis — 20 poin

| Skor   | Deskripsi                                                                                                                               |
| ------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| 17–20 | Konteks bisnis jelas dan spesifik; eksplorasi Spark lengkap (null, outlier, distribusi); dua pertanyaan prediksi dirumuskan dengan baik |
| 13–16 | Konteks bisnis ada; eksplorasi cukup baik namun ada celah (misal outlier tidak dianalisis)                                              |
| 9–12  | Eksplorasi minim; masalah bisnis terlalu umum atau tidak dihubungkan ke model                                                           |
| 5–8   | Hanya ada deskripsi dataset, tanpa analisis atau konteks bisnis                                                                         |
| 0–4   | Dataset tutorial (Titanic/Iris/Tips), atau tidak ada eksplorasi sama sekali                                                             |

### Dimensi 2: Feature Engineering — 20 poin

| Skor   | Deskripsi                                                                                                            |
| ------ | -------------------------------------------------------------------------------------------------------------------- |
| 17–20 | Setiap keputusan engineering dijustifikasi; ada fitur turunan bermakna; tabel dokumentasi lengkap                    |
| 13–16 | Feature engineering benar secara teknis; sebagian besar keputusan dijustifikasi                                      |
| 9–12  | Engineering dilakukan tapi tanpa justifikasi; tidak ada fitur turunan                                                |
| 5–8   | Pipeline tidak lengkap; ada fitur kategorik yang tidak di-encode atau fitur berskala besar yang tidak di-normalisasi |
| 0–4   | Feature engineering tidak dilakukan atau dilakukan di luar Spark                                                     |

### Dimensi 3: Perbandingan Model — 35 poin

| Skor   | Deskripsi                                                                                                                                    |
| ------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 30–35 | ≥3 model klasifikasi + ≥2 model regresi; semua metrik dihitung; confusion matrix ada; tabel perbandingan lengkap; hyperparameter di-tuning |
| 23–29 | ≥3 model dibandingkan; sebagian besar metrik ada; tabel perbandingan ada namun kurang lengkap                                               |
| 16–22 | Hanya 2 model; evaluasi tidak lengkap; tidak ada tabel perbandingan                                                                          |
| 9–15  | Hanya 1 model; pipeline tidak lengkap; evaluasi sangat minim                                                                                 |
| 0–8   | Tidak menggunakan Spark MLlib; atau model tidak bisa dijalankan                                                                              |

### Dimensi 4: Analisis & Rekomendasi Bisnis — 15 poin

| Skor   | Deskripsi                                                                                                                                                                    |
| ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 13–15 | Justifikasi pemilihan model berbasis bisnis (bukan hanya metrik tertinggi); feature importance dianalisis dengan narasi; simulasi 3 profil lengkap; batasan model disebutkan |
| 10–12 | Pemilihan model dijustifikasi; feature importance ada; simulasi ada tapi kurang interpretatif                                                                                |
| 7–9   | Hanya menyebut metrik terbaik tanpa justifikasi bisnis; simulasi tidak ada                                                                                                   |
| 4–6   | Rekomendasi sangat singkat; tidak ada analisis feature importance                                                                                                            |
| 0–3   | Tidak ada analisis bisnis sama sekali                                                                                                                                        |

### Dimensi 5: Laporan & Presentasi — 10 poin

| Skor  | Deskripsi                                                                                                                                   |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 9–10 | Notebook bersih & dapat dijalankan ulang; laporan PDF ≤6 hal dengan semua bagian; demo live sukses; semua anggota bisa menjawab pertanyaan |
| 7–8  | Notebook terstruktur; laporan ada tapi ada bagian yang kurang; presentasi lancar                                                            |
| 5–6  | Notebook bisa dijalankan tapi berantakan; laporan singkat; demo terbata-bata                                                                |
| 3–4  | Notebook tidak bisa dijalankan ulang; laporan tidak lengkap                                                                                 |
| 0–2  | Tidak submit notebook / tidak presentasi                                                                                                    |

### Rekap Penilaian

| Dimensi                    | Bobot          |
| -------------------------- | -------------- |
| 1. Pemahaman Data & Bisnis | 20%            |
| 2. Feature Engineering     | 20%            |
| 3. Perbandingan Model      | 35%            |
| 4. Analisis & Rekomendasi  | 15%            |
| 5. Laporan & Presentasi    | 10%            |
| **Total**            | **100%** |

> 🎁 **Bonus +5 poin**: Implementasikan **CrossValidator** dengan `ParamGridBuilder` untuk tuning otomatis minimal satu model, dan dokumentasikan hasilnya.

---

## Anti-Cheat: Yang Tidak Akan Mendapat Nilai

> ❌ Notebook yang bisa dihasilkan langsung oleh ChatGPT/Claude tanpa modifikasi
> ❌ Dataset yang tidak sesuai syarat (terlalu kecil, tutorial umum, sintetis tanpa konteks)
> ❌ Feature engineering tanpa justifikasi (hanya copy-paste template lab)
> ❌ Analisis bisnis yang generik dan tidak terikat pada dataset yang dipakai
> ❌ Anggota kelompok yang tidak bisa menjelaskan kode yang ada di notebook mereka saat tanya jawab

> ✅ **Yang membuat nilai tinggi:** Keputusan **mengapa** — mengapa memilih fitur ini, mengapa memilih model ini, mengapa threshold ini, dan apa akibat bisnisnya jika model salah.

---

# 📋 BAGIAN 4: PENUTUP

---

## Rangkuman Pertemuan 6

```
┌─────────────────────────────────────────────────────────────────┐
│                   RANGKUMAN HARI INI                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  📖 MATERI: Spark MLlib                                         │
│     • Spark MLlib = ML terdistribusi untuk data berskala besar  │
│     • Konsep: Transformer → Estimator → Pipeline → Evaluator   │
│     • Feature engineering wajib sebelum training                │
│                                                                 │
│  🔧 HANDS-ON LAB                                                │
│     • Lab 1: StringIndexer, VectorAssembler, StandardScaler     │
│     • Lab 2: Pipeline Klasifikasi — Logistic Regression         │
│     • Lab 3: Pipeline Regresi — Linear Regression               │
│                                                                 │
│  📝 TUGAS KELOMPOK (1 minggu, 4–5 orang):                        │
│     • Fase 1: Eksplorasi data & konteks bisnis                  │
│     • Fase 2: Feature engineering + fitur turunan               │
│     • Fase 3: Bandingkan ≥3 model klasifikasi + ≥2 regresi      │
│     • Fase 4: Analisis bisnis + simulasi keputusan              │
│     • Fase 5: Laporan PDF + presentasi live
│                                                                 │
│  🎯 KEY TAKEAWAY                                                 │
│     🤖 MLlib = Scikit-learn versi terdistribusi                 │
│     📦 Pipeline = rangkaian dari raw data hingga prediksi       │
│     📏 Selalu evaluasi model: AUC, Accuracy, RMSE, R²          │
│                                                                 │
│  ➡️  PERTEMUAN 7: Presentasi Tugas Kelompok                     │
└─────────────────────────────────────────────────────────────────┘
```


---

## Referensi

1. Meng, X., et al. (2016). "MLlib: Machine Learning in Apache Spark." *Journal of Machine Learning Research* 17, 1–7.
2. Damji, J., et al. (2020). *Learning Spark* (2nd ed.). O'Reilly Media. Bab 10–11.
3. Apache Spark MLlib Documentation. https://spark.apache.org/docs/latest/ml-guide.html
4. Apache Spark ML Pipelines. https://spark.apache.org/docs/latest/ml-pipeline.html
5. PySpark ML API Reference. https://spark.apache.org/docs/latest/api/python/reference/pyspark.ml.html
6. Géron, A. (2022). *Hands-On Machine Learning* (3rd ed.). O'Reilly Media.