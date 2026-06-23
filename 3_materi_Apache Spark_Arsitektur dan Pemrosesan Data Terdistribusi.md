# Pertemuan 5: Apache Spark — Arsitektur dan Pemrosesan Data Terdistribusi

|                           |                                                                              |
| ------------------------- | ---------------------------------------------------------------------------- |
| **Mata Kuliah**     | Big Data dan Data Lakehouse                                                  |
| **Pertemuan**       | 5 (Minggu 5)                                                                 |
| **Durasi**          | 120 menit                                                                    |
| **CPMK**            | CPMK-2                                                                       |
| **Kemampuan Akhir** | Mahasiswa mampu menggunakan Apache Spark untuk pemrosesan data terdistribusi |
| **Metode**          | 📖 Ceramah → 🔧 Praktikum → 🏆 Challenge → 📋 Penugasan Kelompok          |

---

## Agenda Di Kelas

| Waktu          | Durasi | Kegiatan                                  | Keterangan                                          |
| -------------- | ------ | ----------------------------------------- | --------------------------------------------------- |
| 00:00 – 00:25 | 25 min | 📖 Materi: Arsitektur & Konsep Spark      | Ceramah interaktif, tanya-jawab                     |
| 00:25 – 00:40 | 15 min | 🔧 Lab 1: SparkSession & Eksplorasi Dasar | Setup, membaca data, printSchema, describe          |
| 00:40 – 01:00 | 20 min | 🔧 Lab 2: DataFrame API & Transformasi    | filter, groupBy, agg, join, withColumn              |
| 01:00 – 01:20 | 20 min | 🔧 Lab 3: Spark SQL                       | createOrReplaceTempView, query SQL, window function |
| 01:20 – 01:50 | 30 min | 🏆 Challenge — 10 Soal                   | Kerjakan mandiri, dikumpulkan di akhir              |
| 01:50 – 02:00 | 10 min | 📋 Penjelasan Tugas Kelompok & Penutup    | Briefing Kaggle task + preview pertemuan 6          |

> 📊 **Komposisi waktu:** ~20% materi, ~45% hands-on lab, ~25% challenge, ~10% penutup

---

# 📖 BAGIAN 1: MATERI (Di Kelas — 25 menit)

---

## 1.1 Apa Itu Apache Spark?

**Apache Spark** adalah _unified analytics engine_ untuk pemrosesan data besar yang menyediakan API tingkat tinggi dalam Java, Scala, Python, R, dan SQL.

> 🏎️ **Analogi: Mobil F1 vs. Truk Kontainer**
>
> - **MapReduce** = Truk kontainer: bisa angkut banyak barang, tapi lambat karena harus berhenti di setiap rest area (disk I/O)
> - **Spark** = Mobil F1: sangat cepat karena semua data tetap di "kokpit" (memori/RAM)
>
> Spark bisa **100× lebih cepat** dari MapReduce untuk workload tertentu karena memproses data **in-memory**.

### MapReduce vs. Spark

```
┌─────────────────────────────────────────────────────────────────┐
│            MAPREDUCE vs SPARK — In-Memory Advantage              │
│                                                                 │
│  MAPREDUCE (Disk I/O setiap stage):                              │
│  ┌────┐     ┌────┐     ┌────┐     ┌────┐     ┌────┐            │
│  │Read│────▶│Map │────▶│Disk│────▶│Red.│────▶│Disk│            │
│  └────┘     └────┘     └────┘     └────┘     └────┘            │
│                     Lambat! ❌                                   │
│                                                                 │
│  SPARK (In-Memory):                                              │
│  ┌────┐     ┌────┐     ┌────┐     ┌────┐     ┌────┐            │
│  │Read│────▶│Map │────▶│RAM │────▶│Red.│────▶│RAM │            │
│  └────┘     └────┘     └────┘     └────┘     └────┘            │
│                     Cepat! ⚡                                    │
└─────────────────────────────────────────────────────────────────┘
```

| Aspek               | MapReduce                         | Apache Spark                      |
| ------------------- | --------------------------------- | --------------------------------- |
| **Kecepatan** | Lambat (disk I/O)                 | 10–100× lebih cepat (in-memory) |
| **API**       | Java, verbose                     | Python, Scala, Java, R, SQL       |
| **Iterasi**   | Buruk (ML training sangat lambat) | Sangat baik (data di RAM)         |
| **Streaming** | Tidak didukung                    | Structured Streaming              |
| **ML**        | Apache Mahout (terpisah)          | Spark MLlib (built-in)            |

---

## 1.2 Arsitektur Apache Spark

```
┌─────────────────────────────────────────────────────────────────┐
│                   ARSITEKTUR APACHE SPARK                         │
│                                                                 │
│  ┌────────────────────────────────────┐                          │
│  │          DRIVER PROGRAM            │                          │
│  │  ┌─────────────────────────────┐   │                          │
│  │  │       SparkSession          │   │  ← Entry point           │
│  │  └──────────┬──────────────────┘   │                          │
│  │  • Membuat execution plan (DAG)    │                          │
│  │  • Menjadwalkan tasks              │                          │
│  └─────────────┬──────────────────────┘                          │
│                │                                                 │
│       ┌────────┴────────┐                                        │
│       │ CLUSTER MANAGER │  ← Mengalokasikan resource             │
│       │ (YARN/K8s)      │                                        │
│       └────────┬────────┘                                        │
│          ┌─────┼─────────────┐                                   │
│          ▼     ▼             ▼                                   │
│  ┌────────┐ ┌────────┐ ┌────────┐                                │
│  │EXECUTOR│ │EXECUTOR│ │EXECUTOR│  ← Worker processes            │
│  │┌──┐┌──┐│ │┌──┐┌──┐│ │┌──┐┌──┐│                                │
│  ││T1││T2││ ││T3││T4││ ││T5││T6││  ← Tasks                      │
│  │└──┘└──┘│ │└──┘└──┘│ │└──┘└──┘│                                │
│  │ Cache  │ │ Cache  │ │ Cache  │  ← In-memory storage           │
│  └────────┘ └────────┘ └────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

| Komponen                  | Peran                                                                  |
| ------------------------- | ---------------------------------------------------------------------- |
| **Driver Program**  | Proses utama — membuat SparkSession, membangun DAG, menjadwalkan task |
| **SparkSession**    | Entry point untuk semua fungsionalitas Spark                           |
| **Cluster Manager** | Mengalokasikan resource (CPU, RAM) ke aplikasi Spark                   |
| **Executor**        | Proses JVM di worker node — menjalankan task dan menyimpan cache      |
| **Task**            | Unit kerja terkecil — satu task = satu partisi data                   |

---

## 1.3 Lazy Evaluation & DAG

```
┌─────────────────────────────────────────────────────────────────┐
│                  LAZY EVALUATION & DAG                            │
│                                                                 │
│  1. df = spark.read.csv("data.csv")     ← Transformasi (lazy)   │
│  2. df2 = df.filter(col("age") > 25)    ← Transformasi (lazy)   │
│  3. df3 = df2.groupBy("city").count()   ← Transformasi (lazy)   │
│  4. df3.show()                          ← ACTION! → eksekusi!  │
│                                                                 │
│  Spark TIDAK eksekusi baris 1–3 satu per satu.                   │
│  Baris 1–3 hanya membangun RENCANA (DAG).                        │
│  Baris 4 memicu eksekusi seluruh rencana sekaligus!             │
│                                                                 │
│  ✅ Keuntungan: Spark bisa MENGOPTIMASI seluruh pipeline         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1.4 RDD, DataFrame, dan Spark SQL

| Fitur                 | RDD                              | DataFrame                              |
| --------------------- | -------------------------------- | -------------------------------------- |
| **Schema**      | Tidak ada                        | Ada (kolom + tipe data)                |
| **Optimasi**    | Tidak ada                        | Catalyst Optimizer                     |
| **API**         | Functional (map, filter, reduce) | Declarative (select, where, groupBy)   |
| **SQL**         | Tidak bisa                       | Bisa di-query dengan SQL               |
| **Rekomendasi** | Low-level control saja           | ✅**Default untuk 95% use case** |

> 💡 **Key point:** SQL dan DataFrame API menghasilkan rencana eksekusi yang **identik** karena sama-sama melewati Catalyst Optimizer — pilih mana yang lebih nyaman dibaca.

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

## 🔧 Lab 1: SparkSession & Eksplorasi Dasar (15 menit)

### Tujuan

Membuat SparkSession, memuatkan dataset, dan melakukan eksplorasi awal.

### Task 1.1: Setup SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lab Pertemuan 5") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")
```

### Task 1.2: Buat Dataset & Muat ke DataFrame

```python
data = [
    ("Budi",    "Informatika",      3.80, 2022, "Surabaya"),
    ("Ani",     "Sistem Informasi", 3.50, 2022, "Jakarta"),
    ("Citra",   "Informatika",      3.90, 2021, "Bandung"),
    ("Dedi",    "Sistem Informasi", 3.20, 2023, "Surabaya"),
    ("Eka",     "Informatika",      3.60, 2021, "Jakarta"),
    ("Fajar",   "Data Science",     3.70, 2022, "Bandung"),
    ("Gita",    "Data Science",     3.40, 2023, "Surabaya"),
    ("Hadi",    "Informatika",      2.90, 2021, "Jakarta"),
    ("Indah",   "Sistem Informasi", 3.80, 2022, "Bandung"),
    ("Joko",    "Data Science",     3.10, 2023, "Surabaya"),
    ("Kartika", "Informatika",      3.95, 2021, "Jakarta"),
    ("Lukman",  "Data Science",     3.60, 2022, "Bandung"),
    ("Maya",    "Sistem Informasi", 3.30, 2023, "Surabaya"),
    ("Nadia",   "Informatika",      3.70, 2022, "Jakarta"),
    ("Omar",    "Data Science",     3.85, 2021, "Bandung"),
]

columns = ["nama", "jurusan", "ipk", "angkatan", "kota"]
df = spark.createDataFrame(data, columns)
```

### Task 1.3: Eksplorasi Awal

```python
df.show()           # tampilkan data
df.printSchema()    # lihat tipe data tiap kolom
df.describe().show()  # statistik deskriptif

print(f"Jumlah baris: {df.count()}")
print(f"Jumlah kolom: {len(df.columns)}")
```

**📝 Catat hasilnya — akan digunakan di soal challenge!**

---

## 🔧 Lab 2: DataFrame API & Transformasi (20 menit)

### Tujuan

Menguasai operasi DataFrame: filter, groupBy, agg, withColumn, join.

### Task 2.1: Filter & Select

```python
from pyspark.sql.functions import col

# Filter IPK > 3.5
df.filter(col("ipk") > 3.5).show()

# Filter gabungan: Informatika AND angkatan 2022
df.filter((col("jurusan") == "Informatika") & (col("angkatan") == 2022)).show()

# Select kolom tertentu untuk mahasiswa dari Surabaya
df.select("nama", "jurusan", "ipk") \
  .filter(col("kota") == "Surabaya") \
  .show()
```

### Task 2.2: GroupBy & Agregasi

```python
from pyspark.sql.functions import avg, count, max, min, round

df.groupBy("jurusan") \
  .agg(
      round(avg("ipk"), 2).alias("rata_ipk"),
      count("*").alias("jumlah"),
      round(max("ipk"), 2).alias("ipk_max"),
      round(min("ipk"), 2).alias("ipk_min")
  ) \
  .orderBy(col("rata_ipk").desc()) \
  .show()
```

### Task 2.3: Kolom Baru dengan withColumn

```python
from pyspark.sql.functions import when

df_kategori = df.withColumn(
    "kategori",
    when(col("ipk") >= 3.75, "Cum Laude")
    .when(col("ipk") >= 3.50, "Sangat Memuaskan")
    .when(col("ipk") >= 3.00, "Memuaskan")
    .otherwise("Perlu Perbaikan")
)
df_kategori.select("nama", "ipk", "kategori").show()
```

### Task 2.4: Join DataFrame

```python
data_ukm = [
    ("Budi",    "Robotik"),
    ("Ani",     "Debat"),
    ("Citra",   "Robotik"),
    ("Fajar",   "Data Club"),
    ("Gita",    "Debat"),
    ("Kartika", "Data Club"),
    ("Omar",    "Robotik"),
]
df_ukm = spark.createDataFrame(data_ukm, ["nama", "ukm"])

# Inner join
df.join(df_ukm, on="nama", how="inner").show()

# Left join → cari yang tidak ikut UKM
df.join(df_ukm, on="nama", how="left") \
  .filter(col("ukm").isNull()) \
  .select("nama", "jurusan") \
  .show()
```

---

## 🔧 Lab 3: Spark SQL (20 menit)

### Tujuan

Menggunakan Spark SQL untuk menganalisis data dengan sintaks SQL standar.

### Task 3.1: Register Temporary View

```python
# Daftarkan DataFrame sebagai tabel SQL sementara
df_kategori.createOrReplaceTempView("mahasiswa")
df_ukm.createOrReplaceTempView("ukm")

# Cek tabel yang terdaftar
spark.sql("SHOW TABLES").show()
```

### Task 3.2: Query SQL Dasar

```python
# Semua mahasiswa diurutkan IPK tertinggi
spark.sql("""
    SELECT nama, jurusan, ipk, kategori
    FROM mahasiswa
    ORDER BY ipk DESC
""").show()

# Agregasi per jurusan
spark.sql("""
    SELECT jurusan,
           COUNT(*) AS jumlah,
           ROUND(AVG(ipk), 2) AS rata_ipk
    FROM mahasiswa
    GROUP BY jurusan
    ORDER BY rata_ipk DESC
""").show()
```

### Task 3.3: JOIN & CASE WHEN

```python
# Join mahasiswa dan UKM, tampilkan status UKM
spark.sql("""
    SELECT m.nama, m.jurusan, m.ipk,
           CASE WHEN u.ukm IS NOT NULL THEN u.ukm ELSE 'Tidak Ikut' END AS ukm
    FROM mahasiswa m
    LEFT JOIN ukm u ON m.nama = u.nama
    ORDER BY m.ipk DESC
""").show()
```

### Task 3.4: Window Function

```python
# Ranking IPK dalam setiap jurusan
spark.sql("""
    SELECT nama, jurusan, ipk,
           ROW_NUMBER() OVER (PARTITION BY jurusan ORDER BY ipk DESC) AS ranking
    FROM mahasiswa
""").show()
```

---

# 🏆 BAGIAN 3: CHALLENGE (30 menit)

> **Kerjakan secara mandiri menggunakan dataset challenge di bawah ini.**
> Boleh memakai DataFrame API **atau** Spark SQL — sesuai kenyamanan kamu.

## Setup Dataset Challenge

Jalankan kode berikut sebagai sel pertama di bagian challenge:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, max, min, round

spark = SparkSession.builder.appName("Challenge P5").master("local[*]").getOrCreate()

# Upload file ke Colab terlebih dahulu (Files → Upload), atau mount Google Drive
# kemudian sesuaikan path-nya

df_mhs = spark.read.csv("mahasiswa.csv", header=True, inferSchema=True)

df_ukm = spark.read.csv("ukm.csv", header=True, inferSchema=True)

# Tambah kolom kategori IPK
df_mhs = df_mhs.withColumn(
    "kategori",
    when(col("ipk") >= 3.75, "Cum Laude")
    .when(col("ipk") >= 3.50, "Sangat Memuaskan")
    .when(col("ipk") >= 3.00, "Memuaskan")
    .otherwise("Perlu Perbaikan")
)

# Daftarkan sebagai SQL view
df_mhs.createOrReplaceTempView("mahasiswa")
df_ukm.createOrReplaceTempView("ukm")

# Cek data
df_mhs.show(5)
print(f"Total mahasiswa: {df_mhs.count()}")
```

> 📁 **File dataset:**
>
> - `mahasiswa.csv` — 60 baris, kolom: `nama`, `jurusan`, `ipk`, `angkatan`, `kota`
> - `ukm.csv` — 32 baris, kolom: `nama`, `ukm`

---

---

**Soal 1.** Tampilkan semua mahasiswa dari kota **Jakarta** yang memiliki IPK **≥ 3.7**, diurutkan dari IPK tertinggi ke terendah. Kolom yang ditampilkan: `nama`, `jurusan`, `ipk`.

---

**Soal 2.** Hitung jumlah mahasiswa per **angkatan** dan tampilkan rata-rata IPK per angkatan (dibulatkan 2 desimal). Urutkan dari angkatan paling lama.

---

**Soal 3.** Tambahkan kolom baru bernama `status_lulus` dengan aturan:

- IPK ≥ 3.5 → `"Lulus Dengan Pujian"`
- IPK ≥ 2.75 → `"Lulus"`
- IPK < 2.75 → `"Perlu Remediasi"`

Tampilkan kolom `nama`, `ipk`, dan `status_lulus`.

---

**Soal 4.** Dari seluruh mahasiswa, tampilkan **5 mahasiswa dengan IPK terendah** beserta jurusan dan kotanya.

---

**Soal 5.** Hitung jumlah mahasiswa per **kombinasi jurusan dan kota**. Tampilkan hanya kombinasi yang memiliki **lebih dari 1 mahasiswa**, diurutkan dari jumlah terbanyak.

---

**Soal 6.** Gunakan **Spark SQL**: Tampilkan nama, jurusan, IPK, dan kategori untuk semua mahasiswa yang masuk kategori **"Cum Laude"** atau **"Sangat Memuaskan"**, diurutkan berdasarkan IPK tertinggi.

---

**Soal 7.** Gunakan **Spark SQL**: Buat laporan yang menampilkan untuk setiap jurusan — jumlah mahasiswa, rata-rata IPK, IPK tertinggi, IPK terendah, dan jumlah mahasiswa yang masuk kategori **"Cum Laude"**.

---

**Soal 8.** Gunakan **Spark SQL** dengan **window function**: Ranking (`RANK`) mahasiswa berdasarkan IPK **dalam setiap jurusan** secara menurun. Tampilkan hanya mahasiswa yang berada di **ranking 1 dan 2** per jurusan.

---

**Soal 9.** Join tabel `mahasiswa` dan `ukm`, lalu hitung rata-rata IPK untuk dua kelompok: mahasiswa yang **ikut UKM** dan yang **tidak ikut UKM**. Tampilkan hasilnya dalam satu tabel dengan kolom `status_ukm`, `jumlah`, `rata_ipk`.

---

**Soal 10.** Gunakan **Spark SQL**: Buat satu query yang menghasilkan nama dan jurusan mahasiswa yang **IPK-nya di atas rata-rata IPK jurusannya sendiri**. Urutkan berdasarkan jurusan, lalu IPK tertinggi.

_(Petunjuk: gunakan subquery atau window function seperti `AVG() OVER (PARTITION BY jurusan)`.)_

---

# 📋 BAGIAN 4: TUGAS KELOMPOK

---

## Tugas Kelompok: "Eksplorasi Data Kaggle dengan Apache Spark"

> **Jenis:** Kelompok (sesuai pembagian kelompok kelas)
> **Deadline:** Sebelum Pertemuan 7 (dua minggu — via LMS)
> **Platform:** Google Colab (wajib) — submit link notebook yang bisa diakses publik

---

### Deskripsi Tugas

Setiap kelompok **mencari dataset dari Kaggle** yang menarik dan relevan, lalu menganalisisnya menggunakan **Apache Spark (PySpark)**. Syarat dataset:

- Ukuran file minimal 100 **MB**
- Format: CSV, JSON, atau Parquet
- **Bukan dataset tutorial umum** (Titanic, Iris, Tips, dll.)

> 💡 **Ide topik:** e-commerce, cuaca/iklim, kecelakaan lalu lintas, statistik game, penerbangan, rating film, harga kripto, rekam medis, media sosial, dll.

---

### Struktur Notebook

| Bagian                                | Isi                                                                 | Bobot |
| ------------------------------------- | ------------------------------------------------------------------- | ----- |
| **1. Pengenalan Dataset**       | Link Kaggle, deskripsi, alasan memilih, konteks bisnis              | 15%   |
| **2. Setup & Muat Data**        | SparkSession, mount Drive, baca file, schema, show()                | 10%   |
| **3. Eksplorasi & Pembersihan** | Null, duplikat, casting, statistik deskriptif                       | 20%   |
| **4. Analisis & Insight**       | Min. 5 analisis (≥2 Spark SQL, ≥1 window function) + interpretasi | 40%   |
| **5. Ringkasan & Rekomendasi**  | Tabel ≥5 insight, 2–3 rekomendasi bisnis, 1 pertanyaan lanjutan   | 10%   |
| **6. Notebook & Presentasi**    | Bersih, berurutan, markdown rapi; slide 5–8 hal                    | 5%    |

**Presentasi** dilaksanakan di Pertemuan 7 (7 menit + 3 menit tanya jawab per kelompok).

---

## 📊 Rubrik Penilaian Tugas Kelompok

> **Total: 100 poin + bonus 5 poin**

### Dimensi 1: Pemilihan & Pemahaman Dataset — 15 poin

| Skor   | Deskripsi                                                                                                     |
| ------ | ------------------------------------------------------------------------------------------------------------- |
| 13–15 | Dataset relevan, ukuran memadai, latar belakang & alasan pemilihan dijelaskan dengan konteks bisnis yang kuat |
| 10–12 | Dataset memenuhi syarat, latar belakang ada namun kurang mendalam                                             |
| 7–9   | Dataset memenuhi syarat teknis tetapi penjelasannya minim                                                     |
| 4–6   | Dataset tidak memenuhi salah satu syarat (terlalu kecil / terlalu sedikit kolom)                              |
| 0–3   | Dataset adalah dataset tutorial umum atau tidak ada penjelasan sama sekali                                    |

### Dimensi 2: Setup & Pemuatan Data — 10 poin

| Skor  | Deskripsi                                                                           |
| ----- | ----------------------------------------------------------------------------------- |
| 9–10 | SparkSession dibuat dengan benar, data dimuat tanpa error, schema ditampilkan tepat |
| 7–8  | Setup berjalan dengan minor issue yang tidak mempengaruhi analisis                  |
| 5–6  | Setup berhasil namun ada masalah tipe data yang tidak ditangani                     |
| 3–4  | Setup berhasil sebagian, ada error yang dibypass                                    |
| 0–2  | Setup gagal atau tidak menggunakan PySpark                                          |

### Dimensi 3: Eksplorasi & Pembersihan Data — 20 poin

| Skor   | Deskripsi                                                                                            |
| ------ | ---------------------------------------------------------------------------------------------------- |
| 17–20 | Eksplorasi lengkap (null, duplikat, tipe, statistik); pembersihan dilakukan dengan justifikasi jelas |
| 13–16 | Eksplorasi baik, pembersihan dilakukan namun tanpa penjelasan mengapa                                |
| 9–12  | Eksplorasi sebagian; ada null/duplikat yang tidak ditangani padahal terdeteksi                       |
| 5–8   | Eksplorasi sangat minim, tidak ada pembersihan data                                                  |
| 0–4   | Tidak ada eksplorasi atau seluruhnya menggunakan Pandas                                              |

### Dimensi 4: Kualitas Analisis & Insight — 40 poin

| Skor   | Deskripsi                                                                                                                |
| ------ | ------------------------------------------------------------------------------------------------------------------------ |
| 34–40 | ≥5 analisis dengan pertanyaan bisnis jelas; ≥2 Spark SQL; ≥1 window function; setiap hasil diinterpretasi dengan baik |
| 26–33 | 4–5 analisis yang baik; Spark SQL digunakan setidaknya sekali; sebagian besar hasil diinterpretasi                      |
| 18–25 | 3–4 analisis; kurang memanfaatkan Spark SQL atau window function; interpretasi terbatas                                 |
| 10–17 | 1–2 analisis sangat sederhana; tidak ada Spark SQL atau window function                                                 |
| 0–9   | Analisis menggunakan Pandas, atau tidak menghasilkan insight apapun                                                      |

### Dimensi 5: Ringkasan & Rekomendasi — 10 poin

| Skor  | Deskripsi                                                                                      |
| ----- | ---------------------------------------------------------------------------------------------- |
| 9–10 | Tabel insight lengkap (≥5), rekomendasi bisnis spesifik, ada pertanyaan lanjutan yang menarik |
| 7–8  | Insight dan rekomendasi ada namun kurang spesifik                                              |
| 5–6  | Hanya insight tanpa rekomendasi atau sebaliknya                                                |
| 3–4  | Ringkasan sangat singkat, tidak mencerminkan analisis                                          |
| 0–2  | Tidak ada ringkasan atau rekomendasi                                                           |

### Dimensi 6: Kualitas Notebook & Presentasi — 5 poin

| Skor | Deskripsi                                                                    |
| ---- | ---------------------------------------------------------------------------- |
| 5    | Notebook bersih, berurutan, markdown rapi di setiap bagian; slide informatif |
| 4    | Notebook terstruktur baik; slide ada namun kurang informatif                 |
| 3    | Notebook cukup terbaca; presentasi minim                                     |
| 1–2 | Notebook berantakan atau tidak bisa dijalankan ulang                         |
| 0    | Tidak submit atau tidak presentasi                                           |

### Rekap Penilaian

| Dimensi                           | Bobot          |
| --------------------------------- | -------------- |
| 1. Pemilihan & Pemahaman Dataset  | 15%            |
| 2. Setup & Pemuatan Data          | 10%            |
| 3. Eksplorasi & Pembersihan Data  | 20%            |
| 4. Kualitas Analisis & Insight    | 40%            |
| 5. Ringkasan & Rekomendasi        | 10%            |
| 6. Kualitas Notebook & Presentasi | 5%             |
| **Total**                   | **100%** |

> 🎁 **Bonus +5 poin**: Visualisasi hasil analisis menggunakan Matplotlib/Seaborn (konversi ke Pandas hanya untuk charting diperbolehkan)

---

## Rangkuman Pertemuan 5

```
┌─────────────────────────────────────────────────────────────────┐
│                   RANGKUMAN HARI INI                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  📖 MATERI: Teori Spark                                          │
│     • Arsitektur: Driver → Cluster Manager → Executors → Tasks   │
│     • Lazy Evaluation → DAG → Optimasi otomatis                 │
│     • RDD: immutable, lazy, fault-tolerant via lineage          │
│     • DataFrame + Spark SQL > RDD untuk 95% use case            │
│                                                                 │
│  🔧 HANDS-ON LAB                                                 │
│     • Lab 1: SparkSession + eksplorasi dasar                     │
│     • Lab 2: DataFrame API (filter, groupBy, join, withColumn)   │
│     • Lab 3: Spark SQL (SELECT, GROUP BY, JOIN, Window Function) │
│                                                                 │
│  🏆 CHALLENGE: 10 soal mandiri (100 poin)                        │
│                                                                 │
│  🎯 KEY TAKEAWAY                                                  │
│     ⚡ Spark = in-memory, unified, cepat                          │
│     💻 DataFrame API + SQL = combo utama untuk data engineering  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Referensi

1. Zaharia, M., et al. (2016). "Apache Spark: A Unified Engine for Big Data Processing." *Communications of the ACM*.
2. Damji, J., et al. (2020). *Learning Spark* (2nd ed.). O'Reilly Media.
3. Apache Spark Documentation. https://spark.apache.org/docs/latest/
4. PySpark API Reference. https://spark.apache.org/docs/latest/api/python/
5. Google Colab + PySpark Guide. https://colab.research.google.com/
6. Kaggle Datasets. https://www.kaggle.com/datasets