# Pertemuan 12: Data Lakehouse — Arsitektur, Medallion, dan Delta Lake

|                           |                                                                                    |
| ------------------------- | ---------------------------------------------------------------------------------- |
| **Mata Kuliah**     | Big Data dan Data Lakehouse                                                        |
| **Pertemuan**       | 12 (Minggu 12)                                                                     |
| **Durasi**          | ± 210 menit (termasuk teori + hands-on)                                           |
| **CPMK**            | CPMK-3                                                                             |
| **Kemampuan Akhir** | Mahasiswa mampu merancang dan mengimplementasikan Data Lakehouse dengan Delta Lake |
| **Metode**          | 🖥️ Ceramah, Demonstrasi, Praktikum (Hands-on Lab)                                |

---

## 📋 Panduan Belajar

Ikuti langkah-langkah berikut secara **berurutan**:

| No | Langkah                                                                 | Estimasi | Keterangan                                            |
| -- | ----------------------------------------------------------------------- | -------- | ----------------------------------------------------- |
| 1  | 📖 Baca**Bagian 1**: Evolusi Arsitektur Data                      | 20 menit | Dari Data Warehouse ke Data Lakehouse                 |
| 2  | 📖 Baca**Bagian 2**: Arsitektur Data Lakehouse                    | 20 menit | Komponen utama, open table format                     |
| 3  | 📖 Baca**Bagian 3**: Medallion Architecture                       | 20 menit | Bronze, Silver, Gold — konsep dan penerapan          |
| 4  | 📖 Baca**Bagian 4**: Delta Lake & Table Formats Modern            | 25 menit | ACID, Time Travel, Schema Evolution, perbandingan     |
| 5  | 📖 Baca**Bagian 5**: Ekosistem & Use Case Industri                | 15 menit | Query engine, data catalog, studi kasus Indonesia     |
| 6  | 🛠️ Setup Docker Spark + Delta Lake                                    | 15 menit | Ikuti panduan `hands-on/delta-lake/00_setup.md`     |
| 7  | 🔧 Kerjakan**Lab 1**: Eksplorasi Delta Lake & Transaction Log     | 25 menit | ACID write, read, snapshot                            |
| 8  | 🔧 Kerjakan**Lab 2**: Pipeline Medallion Bronze → Silver → Gold | 30 menit | End-to-end pipeline dari raw data ke agregasi         |
| 9  | 🔧 Kerjakan**Lab 3**: Time Travel & Schema Evolution              | 20 menit | Versioning data, rollback, tambah kolom               |
| 10 | 📝 Catat topik & dataset untuk**Tugas Week 13**                   | —       | Detail tugas ada di `tugas/tugas-data-lakehouse.md` |

> ⏱️ **Total estimasi waktu:** ± 190–210 menit

---

# 📖 MATERI TEORI

> ⏱️ **Estimasi waktu baca:** 100 menit
> 📌 **Pelajari semua bagian sebelum mengerjakan hands-on lab.**

---

## Bagian 1: Evolusi Arsitektur Data (20 menit)

### 1.1 Mengapa Kita Perlu Memahami Evolusinya?

Sebelum kita bisa memahami apa itu Data Lakehouse, kita perlu melihat masalah yang mendorongnya lahir. Arsitektur pengelolaan data telah berevolusi melalui tiga generasi besar:

```
┌────────────────────────────────────────────────────────────────────────┐
│                  EVOLUSI ARSITEKTUR DATA (1980–2020+)                   │
│                                                                        │
│  1980–2000           2010–2015              2020+                      │
│                                                                        │
│  ┌────────────┐      ┌────────────┐      ┌────────────────────────┐   │
│  │    DATA    │      │    DATA    │      │      DATA LAKEHOUSE     │   │
│  │  WAREHOUSE │ ───▶ │    LAKE    │ ───▶ │                        │   │
│  │            │      │            │      │  BI & ML di atas       │   │
│  │ SQL, OLAP  │      │ Hadoop/S3  │      │  open storage murah    │   │
│  └────────────┘      └────────────┘      └────────────────────────┘   │
│                                                                        │
│  Structured only     All data types        All data types +           │
│  Expensive           Cheap storage         DW-quality governance       │
│  Fast queries        Slow queries          Fast queries                │
│  ACID ✅             ACID ❌               ACID ✅                    │
│  ML ❌               ML ✅                 ML ✅                      │
└────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Era Data Warehouse

**Data Warehouse** adalah sistem penyimpanan data analitik yang muncul sekitar tahun 1980–1990-an, dipopulerkan oleh Bill Inmon dan Ralph Kimball.

**Cara Kerja:**

- Data dari berbagai sumber operasional (ERP, CRM, POS) dikumpulkan melalui proses **ETL** (*Extract, Transform, Load*).
- Data harus diubah terlebih dahulu sesuai skema yang ketat (*Schema-on-Write*) sebelum bisa disimpan.
- Data disimpan dalam format kolom yang dioptimalkan untuk kueri analitik (OLAP).

| Kelebihan ✅                                  | Kekurangan ❌                                                          |
| --------------------------------------------- | ---------------------------------------------------------------------- |
| Kueri SQL sangat cepat (sudah dioptimalkan)   | **Sangat mahal** (lisensi Oracle, Teradata bisa miliaran Rupiah) |
| Mendukung transaksi ACID secara native        | Kaku — skema harus didefinisikan di awal (*schema-on-write*)        |
| Kualitas data sangat terjamin                 | Tidak mendukung data tidak terstruktur (video, teks bebas)             |
| Cocok untuk laporan bisnis (BI) dan dashboard | Tidak efisien untuk Machine Learning modern                            |
| Vendor menyediakan dukungan dan SLA           | *Data Silos* — satu gudang tidak bisa layani semua kebutuhan        |

> 🧠 **Contoh Data Warehouse populer:**
> Oracle Exadata, Teradata, IBM Db2 Warehouse, Microsoft SQL Server Analysis Services, dan versi cloud-nya: Amazon Redshift, Google BigQuery, Snowflake.

### 1.3 Era Data Lake

Sekitar tahun 2010, meledaknya volume data (media sosial, IoT, log aplikasi) membuat Data Warehouse tidak lagi memadai — terlalu mahal dan terlalu kaku. Solusinya adalah **Data Lake**.

**Konsep Data Lake** diperkenalkan oleh James Dixon (CTO Pentaho, 2010):

> *"A data lake is a large body of water in a natural state. The contents of the data lake stream in from a source to fill the lake, and various users of the lake can come to examine, dive in, or take samples."*

**Cara Kerja:**

- Simpan **semua data dalam format aslinya** (CSV, JSON, Parquet, Avro, image, video) di *object storage* murah (HDFS, Amazon S3, GCS).
- Skema tidak harus ditentukan saat menyimpan — cukup saat membaca (*Schema-on-Read*).
- Fleksibel untuk berbagai jenis workload: BI, Data Science, ML.

| Kelebihan ✅                                      | Kekurangan ❌                                                               |
| ------------------------------------------------- | --------------------------------------------------------------------------- |
| **Sangat murah** (S3 ~$23/TB/bulan)         | Tidak ada jaminan kualitas data (*garbage in, garbage out*)               |
| Menerima semua jenis data tanpa transformasi awal | **Tidak ada transaksi ACID** — update/delete sulit dan berisiko      |
| Sangat cocok untuk Machine Learning               | Kueri analitik lambat (tidak ada indeks seperti di DW)                      |
| Skalabel secara horizontal                        | Sering berubah menjadi**"Data Swamp"** (rawa data) — tidak terkelola |
| Mendukung beragam format file                     | *Metadata* tidak terstruktur, data sulit ditemukan                        |

### 1.4 Masalah "Data Swamp"

Dalam praktiknya, banyak organisasi yang membangun Data Lake tetapi kemudian gagal mengelolanya dengan baik. Data Lake yang tidak terkelola berubah menjadi **Data Swamp**:

```
┌──────────────────────────────────────────────────────────────────────┐
│                    GEJALA DATA SWAMP                                  │
│                                                                      │
│  ❌ "Data mana yang paling baru? Yang di folder ini atau itu?"        │
│  ❌ "Dataset ini sudah di-update tapi pipeline lain masih baca       │
│     versi lama — hasilnya tidak konsisten"                           │
│  ❌ "Ada yang hapus folder secara tidak sengaja, data hilang"         │
│  ❌ "Tidak tahu kolom ini artinya apa dan siapa yang mengisinya"      │
│  ❌ Data Science team butuh 3 minggu hanya untuk mencari dataset      │
│     yang mereka butuhkan                                             │
│                                                                      │
│  ROOT CAUSE:                                                         │
│  • Tidak ada ACID transactions                                       │
│  • Tidak ada schema enforcement                                      │
│  • Tidak ada data catalog / metadata management                      │
│  • Tidak ada data versioning / audit trail                           │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.5 Lahirnya Data Lakehouse

**Data Lakehouse** diperkenalkan secara formal oleh tim Databricks melalui paper ilmiah pada konferensi CIDR 2021:

> *"Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics"*
> — Armbrust, M., et al. (2021)

Ide dasarnya sederhana namun powerful: **implementasikan fitur-fitur manajemen data ala Data Warehouse (ACID, schema enforcement, governance) di atas infrastruktur Data Lake yang murah dan terbuka**.

> 🇮🇩 **Konteks Indonesia:**
>
> - Bank-bank besar (BCA, Mandiri, BRI) telah memiliki Data Warehouse mahal, tetapi volume data fintech (OVO, GoPay) tumbuh begitu pesat hingga perlu pendekatan Data Lake.
> - Startup seperti Tokopedia, Shopee Indonesia, dan Traveloka butuh satu platform yang bisa melayani keduanya: laporan keuangan akurat (BI) sekaligus model rekomendasi ML — Data Lakehouse adalah jawabannya.

---

## Bagian 2: Arsitektur Data Lakehouse (20 menit)

### 2.1 Komponen Utama

Data Lakehouse bukanlah satu produk, melainkan sekumpulan lapisan (*layers*) teknologi yang bekerja bersama:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    ARSITEKTUR DATA LAKEHOUSE                              │
│                                                                          │
│  LAPISAN 5: BI & ML                                                      │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌────────────────────────┐  │
│  │ Superset │  │  Grafana │  │  Jupyter  │  │  Spark MLlib / TF      │  │
│  └──────────┘  └──────────┘  └───────────┘  └────────────────────────┘  │
│  ─────────────────────────────────────────────────────────────────────── │
│  LAPISAN 4: QUERY ENGINE                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────────┐  │
│  │  Spark SQL   │  │ Trino/Presto │  │      Databricks SQL            │  │
│  └──────────────┘  └──────────────┘  └────────────────────────────────┘  │
│  ─────────────────────────────────────────────────────────────────────── │
│  LAPISAN 3: CATALOG & GOVERNANCE                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────────┐  │
│  │Hive Metastore│  │  Unity Catalog│  │  AWS Glue / Apache Atlas       │  │
│  └──────────────┘  └──────────────┘  └────────────────────────────────┘  │
│  ─────────────────────────────────────────────────────────────────────── │
│  LAPISAN 2: TABLE FORMAT (KUNCI LAKEHOUSE)                                │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────────────┐  │
│  │  Delta Lake  │  │Apache Iceberg│  │       Apache Hudi               │  │
│  │ (Databricks) │  │  (Netflix)   │  │       (Uber)                    │  │
│  └──────────────┘  └──────────────┘  └────────────────────────────────┘  │
│  ─────────────────────────────────────────────────────────────────────── │
│  LAPISAN 1: FILE FORMAT                                                   │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │           Apache Parquet (columnar, compressed)                     │  │
│  │           Apache ORC, Apache Avro (alternatif)                      │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│  ─────────────────────────────────────────────────────────────────────── │
│  LAPISAN 0: STORAGE (MURAH & SCALABLE)                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────────┐   │
│  │ Amazon S3│  │  GCS     │  │   ADLS   │  │   HDFS (On-Premise)     │   │
│  └──────────┘  └──────────┘  └──────────┘  └────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Mengapa Apache Parquet Penting?

**Apache Parquet** adalah format file *columnar* (berbasis kolom) yang menjadi fondasi dari semua sistem Data Lakehouse modern.

```
┌──────────────────────────────────────────────────────────────────────┐
│          ROW-BASED vs COLUMNAR STORAGE                                │
│                                                                      │
│  Data: Tabel Transaksi                                               │
│  ┌──────┬────────┬──────────┬──────────┐                            │
│  │  ID  │ Amount │ Category │   Date   │                            │
│  ├──────┼────────┼──────────┼──────────┤                            │
│  │  1   │ 50.000 │ Makanan  │ 2024-01  │                            │
│  │  2   │ 75.000 │ Transport│ 2024-01  │                            │
│  │  3   │ 30.000 │ Makanan  │ 2024-02  │                            │
│  └──────┴────────┴──────────┴──────────┘                            │
│                                                                      │
│  ROW-BASED (CSV, RDBMS):             COLUMNAR (Parquet):             │
│  [1,50000,Makanan,2024-01]           ID:     [1,2,3]                 │
│  [2,75000,Transport,2024-01]         Amount: [50000,75000,30000]     │
│  [3,30000,Makanan,2024-02]           Cat:    [Makanan,Transport,...] │
│                                      Date:   [2024-01,2024-01,...]   │
│                                                                      │
│  Query: "Hitung total Amount"                                        │
│  Row-based: baca SEMUA kolom    Columnar: baca kolom Amount SAJA     │
│  → I/O tinggi                   → I/O sangat rendah ✅               │
│                                                                      │
│  + Kompresi kolom JAUH lebih efisien (data sejenis)                  │
│  + Pushdown filter (baca hanya data yang dibutuhkan)                 │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.3 Perbandingan: Data Warehouse vs Data Lake vs Data Lakehouse

| Aspek                       | Data Warehouse          | Data Lake              | Data Lakehouse            |
| --------------------------- | ----------------------- | ---------------------- | ------------------------- |
| **Jenis Data**        | Terstruktur saja        | Semua jenis            | Semua jenis               |
| **Skema**             | Schema-on-Write (ketat) | Schema-on-Read (bebas) | Schema-on-Write + Evolusi |
| **ACID Transactions** | ✅ Ya                   | ❌ Tidak               | ✅ Ya (via table format)  |
| **Biaya Storage**     | Sangat mahal            | Sangat murah           | Murah                     |
| **Kinerja Kueri BI**  | Sangat cepat            | Lambat                 | Cepat                     |
| **Dukungan ML**       | Terbatas                | Sangat baik            | Sangat baik               |
| **Data Versioning**   | Terbatas                | ❌ Tidak               | ✅ Time Travel            |
| **Open Format**       | Proprietary             | Terbuka                | Terbuka                   |
| **Contoh Teknologi**  | Snowflake, Redshift     | Hadoop HDFS, S3        | Delta Lake, Iceberg, Hudi |

---

## Bagian 3: Medallion Architecture (20 menit)

### 3.1 Konsep Medallion Architecture

**Medallion Architecture** adalah pola desain (*design pattern*) yang dikembangkan oleh Databricks untuk menyusun data di dalam Data Lakehouse secara logis dalam lapisan yang mencerminkan **kualitas data yang semakin meningkat** secara progresif.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                                  │
│                                                                            │
│  SUMBER DATA                                                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐          │
│  │   REST API  │  │   Kafka    │  │  Database  │  │  File CSV  │          │
│  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘          │
│         │               │               │               │                  │
│         └───────────────┴───────────────┴───────────────┘                  │
│                                 │                                          │
│                                 ▼                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  🥉 BRONZE LAYER — "Raw Vault" / "Landing Zone"                      │  │
│  │  • Data mentah, format asli, tidak dimodifikasi                      │  │
│  │  • Hanya ditambah metadata: waktu ingest, sumber data               │  │
│  │  • Mode: Append-only (tidak pernah dihapus/diubah)                  │  │
│  │  • Contoh: /data/bronze/orders/ (parquet, raw JSON)                  │  │
│  └──────────────────────────────────┬───────────────────────────────────┘  │
│                                     │ Cleaning & Validation                │
│                                     ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  🥈 SILVER LAYER — "Cleaned & Conformed"                             │  │
│  │  • Data sudah dibersihkan: null ditangani, duplikat dihapus         │  │
│  │  • Tipe data sudah di-cast dengan benar (string → date, int)        │  │
│  │  • Data di-join / diperkaya dari tabel referensi                    │  │
│  │  • Contoh: /data/silver/orders/ (clean parquet, typed schema)       │  │
│  └──────────────────────────────────┬───────────────────────────────────┘  │
│                                     │ Aggregation & Business Logic         │
│                                     ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  🥇 GOLD LAYER — "Curated Business Data"                             │  │
│  │  • Data diagregasi sesuai kebutuhan bisnis spesifik                 │  │
│  │  • Biasanya berbentuk Star Schema / Fact-Dimension tables            │  │
│  │  • Langsung bisa di-query oleh BI tools (Superset, Grafana)         │  │
│  │  • Contoh: /data/gold/daily_sales/ (aggregated metrics)             │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                         │                     │                            │
│                         ▼                     ▼                            │
│               BI Dashboard             ML Training                         │
│              (Superset, Grafana)       (Spark MLlib)                       │
└────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Deep Dive: Bronze Layer

Bronze adalah lapisan pertama dan paling penting karena menjadi **sumber kebenaran tunggal** (*single source of truth*) untuk data mentah.

**Prinsip Utama Bronze:**

1. **Immutability**: Data yang sudah masuk tidak boleh dimodifikasi. Jika ada kesalahan, pipeline di layer atasnya yang diperbaiki, bukan data Bronze.
2. **Full History**: Bronze menyimpan semua versi data, termasuk data yang mungkin mengandung error. Ini memungkinkan *reprocessing* kapan saja.
3. **Metadata Tambahan**: Biasanya ditambahkan kolom seperti `_ingested_at` (waktu ingest), `_source` (nama sistem sumber).

> 💡 **Analogi:** Bronze adalah mesin perekam CCTV. Ia merekam semua yang terjadi apa adanya tanpa menghapus rekaman lama. Jika nanti butuh investigasi, rekaman lama tetap ada.

**Contoh Skema Bronze (Transaksi E-Commerce):**

| Kolom              | Tipe      | Keterangan                               |
| ------------------ | --------- | ---------------------------------------- |
| `raw_payload`    | STRING    | Data mentah (kadang disimpan as-is JSON) |
| `transaction_id` | STRING    | ID dari sumber, belum di-validasi        |
| `amount`         | STRING    | Masih string, belum di-cast ke number    |
| `_ingested_at`   | TIMESTAMP | Kapan data diingest (metadata)           |
| `_source`        | STRING    | Dari sistem mana datanya                 |

### 3.3 Deep Dive: Silver Layer

Silver adalah lapisan transformasi utama, di mana data mulai **bisa dipercaya** untuk analisis.

**Transformasi Umum di Silver:**

- **Deduplication**: Hapus baris duplikat berdasarkan `transaction_id`.
- **Type Casting**: `"50000"` (string) → `50000.0` (double).
- **Null Handling**: Isi null dengan nilai default, atau filter baris yang null di kolom kritis.
- **Standardization**: Format tanggal disamakan (`"01/05/24"` → `2024-05-01`).
- **Enrichment / Join**: Join dengan tabel referensi (misal: `product_id` → nama produk, harga).

> 💡 **Analogi:** Silver adalah departemen QC (Quality Control) di pabrik. Data dari Bronze "dicuci" dan diperiksa sebelum diteruskan.

### 3.4 Deep Dive: Gold Layer

Gold adalah lapisan yang paling "dekat" dengan pengguna bisnis. Data di sini bersifat **tujuan-spesifik** (*purpose-built*).

**Karakteristik Gold:**

- Setiap tabel Gold biasanya dibuat untuk satu pertanyaan bisnis spesifik.
- Tidak ada transformasi berat di sini, hanya agregasi dan join final.
- Biasanya menggunakan pemodelan dimensi: **Fact Table** (transaksi) + **Dimension Table** (produk, pelanggan, waktu).

**Contoh Tabel Gold untuk E-Commerce:**

| Nama Tabel Gold                | Tujuan                                           |
| ------------------------------ | ------------------------------------------------ |
| `gold.daily_revenue`         | Total pendapatan per hari untuk dashboard CFO    |
| `gold.product_performance`   | Produk terlaris per kategori per bulan           |
| `gold.customer_segmentation` | Segmen pelanggan untuk tim Marketing             |
| `gold.fraud_signals`         | Transaksi berisiko tinggi untuk tim Risk & Fraud |

### 3.5 Studi Kasus: Medallion untuk Data Kualitas Udara Kota

> 🇮🇩 **Konteks:** Dinas Lingkungan Hidup DKI Jakarta ingin memantau dan menganalisis kualitas udara real-time dari ratusan sensor IoT yang tersebar di seluruh kota.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  STUDI KASUS: SISTEM PEMANTAUAN KUALITAS UDARA JAKARTA                   │
│                                                                         │
│  SUMBER DATA                                                            │
│  IoT Sensor (MQTT/Kafka) → PM2.5, PM10, CO2, NO2, O3, Suhu, Kelembaban │
│                                                                         │
│  🥉 BRONZE                                                               │
│  /bronze/air_quality_raw/                                               │
│  • sensor_id, district, raw_readings (JSON string), _ingested_at       │
│  • Semua pembacaan sensor masuk, termasuk yang error (nilai -999)       │
│  • 500+ sensor × 1 reading/menit = ~720.000 records/hari               │
│                                                                         │
│  🥈 SILVER                                                               │
│  /silver/air_quality_clean/                                             │
│  • Filter error readings (PM25 < 0 atau > 500 dianggap error sensor)   │
│  • Tambahkan kolom: kecamatan, koordinat GPS dari tabel referensi       │
│  • Hitung AQI (Air Quality Index) dari raw readings                    │
│  • Cast timestamp yang benar                                            │
│                                                                         │
│  🥇 GOLD                                                                 │
│  /gold/aqi_hourly_by_district/                                          │
│  • Rata-rata AQI per kecamatan per jam                                  │
│  → Dashboard publik: "Peta Kualitas Udara Jakarta Hari Ini"             │
│  /gold/pollution_alert_signals/                                         │
│  • Kecamatan dengan AQI > 150 (Unhealthy) selama > 2 jam berturut     │
│  → Sistem peringatan dini: notifikasi ke Dinas & warga                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Bagian 4: Delta Lake & Table Formats Modern (25 menit)

### 4.1 Apa Itu Table Format?

**Table Format** adalah lapisan metadata yang memberikan "kecerdasan" pada sekumpulan file Parquet di storage. Tanpa table format, sekumpulan file Parquet hanyalah file biasa — tidak ada konsep "tabel", tidak ada transaksi, tidak ada versioning.

Table format menambahkan:

- **Transaction Log**: Rekam setiap perubahan (write, update, delete) secara atomik.
- **Schema Registry**: Simpan dan validasi skema tabel.
- **File Manifest**: Daftar file Parquet mana yang menjadi bagian dari versi tabel saat ini.
- **Statistics**: Statistik per kolom untuk *query pruning*.

### 4.2 Delta Lake — Arsitektur Internal

Delta Lake dikembangkan oleh Databricks (2019) dan sekarang merupakan proyek open source di bawah Linux Foundation.

**Komponen Utama Delta Lake:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  STRUKTUR DIREKTORI DELTA TABLE                          │
│                                                                         │
│  /data/silver/orders/                                                   │
│  ├── _delta_log/                   ← TRANSACTION LOG (kunci Delta Lake) │
│  │   ├── 00000000000000000000.json ← Versi 0: CREATE TABLE              │
│  │   ├── 00000000000000000001.json ← Versi 1: INSERT 1000 rows          │
│  │   ├── 00000000000000000002.json ← Versi 2: UPDATE status='DONE'      │
│  │   ├── 00000000000000000003.json ← Versi 3: DELETE cancelled orders   │
│  │   └── 00000000000000000004.checkpoint.parquet ← Checkpoint (summary) │
│  ├── part-00000-abc123.snappy.parquet  ← File data aktif                │
│  ├── part-00001-def456.snappy.parquet  ← File data aktif                │
│  └── part-00002-ghi789.snappy.parquet  ← File data aktif                │
│                                                                         │
│  Setiap file JSON di _delta_log berisi:                                  │
│  • "add" actions: file baru yang ditambahkan                            │
│  • "remove" actions: file yang "dihapus" (soft delete, file asli tetap) │
│  • "metaData" actions: perubahan schema                                 │
│  • "commitInfo": siapa yang commit, kapan, operasi apa                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 ACID Transactions di Delta Lake

Delta Lake memberikan jaminan **ACID penuh** di atas *object storage*:

| Properti ACID         | Artinya                                             | Cara Delta Lake Mengimplementasikannya            |
| --------------------- | --------------------------------------------------- | ------------------------------------------------- |
| **Atomicity**   | Semua perubahan berhasil, atau tidak sama sekali    | Setiap commit ditulis atomik ke `_delta_log`    |
| **Consistency** | Data selalu valid (schema terjaga)                  | Schema Enforcement menolak data yang tidak sesuai |
| **Isolation**   | Transaksi bersamaan tidak mengganggu satu sama lain | Optimistic Concurrency Control (OCC)              |
| **Durability**  | Data yang sudah di-commit tidak akan hilang         | File disimpan di object storage yang durable      |

> 🔍 **Kenapa ini penting?**
> Di Data Lake biasa (HDFS/S3 tanpa Delta), jika ada dua proses yang menulis ke folder yang sama secara bersamaan, hasilnya bisa *corrupt* atau *inconsistent*. Delta Lake mencegah hal ini dengan protokol transaksi berbasis log.

### 4.4 Time Travel — Mengkueri Data di Masa Lampau

Salah satu fitur paling populer Delta Lake adalah **Time Travel**: kemampuan untuk membaca data pada versi tertentu atau pada waktu tertentu di masa lampau.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      TIME TRAVEL DI DELTA LAKE                          │
│                                                                         │
│  TIMELINE PERUBAHAN:                                                    │
│                                                                         │
│  Versi 0    Versi 1      Versi 2          Versi 3                       │
│  (Create)   (Insert)     (Update)         (Delete)                      │
│  ────────────────────────────────────────────────▶ Waktu                │
│                                                                         │
│  QUERY TIME TRAVEL:                                                     │
│                                                                         │
│  # Menggunakan versi number                                             │
│  spark.read.format("delta") \                                           │
│      .option("versionAsOf", 1) \    ← Baca data di Versi 1             │
│      .load("/data/silver/orders")                                       │
│                                                                         │
│  # Menggunakan timestamp                                                │
│  spark.read.format("delta") \                                           │
│      .option("timestampAsOf", "2024-05-01") \  ← Baca data 1 Mei 2024  │
│      .load("/data/silver/orders")                                       │
│                                                                         │
│  USE CASES:                                                             │
│  ✅ Audit: "Berapa revenue kita sebelum ada koreksi data kemarin?"      │
│  ✅ Debugging: "Data ML di-train pakai versi data yang mana?"           │
│  ✅ Rollback: Kembalikan tabel ke versi sebelumnya jika ada kesalahan   │
│  ✅ Reprodusibilitas: Jalankan laporan dengan kondisi data historis     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.5 Schema Evolution & Schema Enforcement

Delta Lake menyediakan dua mekanisme yang saling melengkapi untuk manajemen skema:

**Schema Enforcement (default: ON)**
Menolak data baru yang tidak sesuai skema tabel yang sudah ada:

```python
# Ini akan GAGAL jika tabel sudah ada dan tidak punya kolom 'new_col'
df_with_extra_col.write.format("delta").mode("append").save("/data/orders")
# Error: AnalysisException: A schema mismatch detected when writing to Delta table
```

**Schema Evolution (opsional: harus di-enable)**
Memperbolehkan penambahan kolom baru secara otomatis:

```python
# Aktifkan schema evolution → kolom baru ditambahkan ke tabel secara otomatis
df_with_new_col.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/data/orders")
```

### 4.6 Perbandingan Table Formats: Delta Lake vs Iceberg vs Hudi

| Fitur                       | Delta Lake                            | Apache Iceberg                                          | Apache Hudi                          |
| --------------------------- | ------------------------------------- | ------------------------------------------------------- | ------------------------------------ |
| **Dibuat oleh**       | Databricks (2019)                     | Netflix (2018)                                          | Uber (2017)                          |
| **ACID Transactions** | ✅                                    | ✅                                                      | ✅                                   |
| **Time Travel**       | ✅                                    | ✅                                                      | ✅ (terbatas)                        |
| **Schema Evolution**  | ✅                                    | ✅ (lebih canggih)                                      | ✅                                   |
| **Upsert/Merge**      | ✅ (MERGE INTO)                       | ✅                                                      | ✅ (optimized/COW-MOR)               |
| **Kekuatan Utama**    | Integrasi Spark, ekosistem Databricks | Multi-engine (Spark, Trino, Flink), hidden partitioning | Streaming upserts, incremental pulls |
| **Engine Terbaik**    | Apache Spark                          | Spark, Trino, Flink                                     | Spark, Flink                         |
| **License**           | Apache 2.0 (OSS)                      | Apache 2.0 (OSS)                                        | Apache 2.0 (OSS)                     |

> 📌 **Rekomendasi Praktis:**
>
> - Pilih **Delta Lake** jika ekosistem utama Anda adalah Spark/Databricks.
> - Pilih **Apache Iceberg** jika butuh dukungan multi-engine (Spark + Trino untuk BI + Flink untuk streaming).
> - Pilih **Apache Hudi** jika use case utama adalah streaming ingest dengan banyak operasi upsert (misal: CDC dari database).

---

## Bagian 5: Ekosistem & Use Case Industri (15 menit)

### 5.1 Query Engine di Data Lakehouse

Data Lakehouse menggunakan berbagai *query engine* yang bisa membaca langsung dari *object storage* tanpa perlu memindahkan data:

| Query Engine                | Kelebihan                                                        | Ideal Untuk           |
| --------------------------- | ---------------------------------------------------------------- | --------------------- |
| **Apache Spark SQL**  | Sudah terintegrasi dengan Delta/Iceberg/Hudi, powerful untuk ETL | Pipeline ETL, ML prep |
| **Trino (ex-Presto)** | Kueri ad-hoc yang sangat cepat, mendukung banyak sumber data     | BI / analyst queries  |
| **Apache Flink SQL**  | Streaming SQL — kueri real-time atas data streaming             | Real-time analytics   |
| **DuckDB**            | OLAP in-process yang sangat cepat, cocok di laptop               | Eksplorasi data lokal |

### 5.2 Data Catalog — Menemukan Data yang Tepat

**Data Catalog** adalah "direktori" yang menyimpan metadata semua aset data di organisasi — siapa yang punya data apa, artinya apa, kualitasnya seperti apa, dan siapa yang boleh mengaksesnya.

Tanpa Data Catalog, tim Data Science bisa menghabiskan **60-70% waktunya hanya untuk mencari dan memahami data** yang ada.

| Komponen Catalog               | Deskripsi                                                           |
| ------------------------------ | ------------------------------------------------------------------- |
| **Data Discovery**       | Cari dataset berdasarkan nama kolom, deskripsi, tag, pemilik        |
| **Schema Registry**      | Simpan dan versi skema setiap tabel                                 |
| **Data Lineage**         | Pelacakan asal-usul data: dari mana data ini berasal? Pipeline apa? |
| **Data Quality Metrics** | Berapa % null? Apakah ada anomali nilai?                            |
| **Access Control**       | Siapa yang boleh membaca/menulis tabel tertentu?                    |

### 5.3 Data Lakehouse di Industri Indonesia

> 🇮🇩 **Studi Kasus Nyata:**

**Tokopedia / GoTo:**

- Menggunakan platform berbasis Spark + Iceberg untuk memproses ratusan terabyte data transaksi, event log pengguna, dan data seller.
- Gold layer digunakan untuk dashboard seller analytics (GoTo Merchant), fraud detection, dan model rekomendasi.

**Bank Digital (Jenius, BYOND, etc.):**

- Data Lakehouse digunakan untuk *regulatory reporting* (laporan OJK) sekaligus model *credit scoring* berbasis ML.
- Delta Lake dipilih karena kemampuan ACID yang dibutuhkan untuk keakuratan laporan keuangan.

**Startup Logistik (SiCepat, J&T):**

- Sensor GPS armada kendaraan menghasilkan streaming data yang masuk ke Bronze layer via Kafka.
- Silver layer melakukan cleaning dan deduplikasi, Gold layer menghasilkan estimasi ETA akurat untuk dashboard pelanggan.

---

# 🛠️ HANDS-ON LAB

> ⏱️ **Estimasi waktu:** 75 menit
> 🐋 **Prasyarat:** Docker Desktop berjalan, container sudah di-`up` sesuai panduan `00_setup.md`.
> 📁 **Semua script ada di:** `materi/hands-on/delta-lake/`

---

## Lab 1: Eksplorasi Delta Lake & Transaction Log (25 menit)

**Tujuan:** Memahami bagaimana Delta Lake menyimpan data dan mengelola transaction log.

### Langkah 1: Generate Data Dummy

Buka Terminal di JupyterLab, jalankan:

```bash
cd /home/jovyan/work
python generate_data.py
```

Ini akan membuat file `raw_transactions.csv` berisi ~1000 baris data transaksi e-commerce simulasi yang sengaja mengandung: baris duplikat, nilai null, dan format tidak konsisten.

### Langkah 2: Ingest ke Bronze Layer

Jalankan script atau copy isi `01_bronze_ingestion.py` ke Jupyter Notebook, lalu eksekusi:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("Lab1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Baca CSV mentah
raw_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("raw_transactions.csv")
raw_df.show(5)

# Simpan ke Bronze (format Delta)
raw_df.write.format("delta").mode("append").save("./data/bronze/transactions")
print("Berhasil disimpan ke Bronze!")
```

### Langkah 3: Eksplorasi Transaction Log

Setelah menulis data, lihat isi `_delta_log` yang dibuat secara otomatis:

```bash
# Di terminal JupyterLab:
ls -la ./data/bronze/transactions/_delta_log/
cat ./data/bronze/transactions/_delta_log/00000000000000000000.json
```

> 🔍 **Yang perlu diperhatikan:**
>
> - File JSON berisi `commitInfo`, `metaData`, dan `add` actions.
> - Field `add` mencatat file Parquet mana yang merupakan bagian dari tabel.
> - Field `stats` berisi statistik per kolom (min, max, null_count) untuk query optimization.

**✏️ Pertanyaan Refleksi Lab 1:**

1. Berapa jumlah file Parquet yang dibuat di folder bronze?
2. Apa informasi yang ada di `commitInfo` dalam transaction log?
3. Apa yang dimaksud dengan `add` dan `remove` action di transaction log?

---

## Lab 2: Pipeline Medallion Bronze → Silver → Gold (30 menit)

**Tujuan:** Membangun pipeline ETL end-to-end menggunakan Medallion Architecture.

### Langkah 1: Transformasi Silver (Cleaning)

Copy isi `02_silver_transform.py` ke notebook baru, eksekusi, dan perhatikan hasilnya:

```python
# Baca Bronze
bronze_df = spark.read.format("delta").load("./data/bronze/transactions")
print(f"Bronze row count: {bronze_df.count()}")

# Cleaning ke Silver
from pyspark.sql.functions import col, to_timestamp

silver_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("amount").isNotNull() & (col("amount") != "")) \
    .fillna({"status": "UNKNOWN"}) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("transaction_date", to_timestamp(col("transaction_date")))

print(f"Silver row count (setelah cleaning): {silver_df.count()}")
silver_df.show(5)

# Simpan ke Silver
silver_df.write.format("delta").mode("overwrite").save("./data/silver/transactions")
```

> 📊 **Bandingkan jumlah baris Bronze vs Silver:**
> Berapa baris yang hilang setelah proses cleaning? Baris yang hilang berasal dari: duplikat dan baris dengan amount kosong.

### Langkah 2: Agregasi Gold

Copy isi `03_gold_aggregate.py` ke notebook, eksekusi:

```python
from pyspark.sql.functions import sum, count, to_date

silver_df = spark.read.format("delta").load("./data/silver/transactions")

gold_df = silver_df \
    .filter(col("status") == "COMPLETED") \
    .withColumn("date", to_date(col("transaction_date"))) \
    .groupBy("date") \
    .agg(
        sum("amount").alias("total_revenue"),
        count("transaction_id").alias("total_transactions")
    ).orderBy("date")

gold_df.show(15)
gold_df.write.format("delta").mode("overwrite").save("./data/gold/daily_revenue")
print("Gold layer siap!")
```

> 💡 **Insight:** Data di Gold layer sudah siap langsung disambungkan ke dashboard BI seperti Apache Superset atau Grafana. Seorang analis bisa langsung menulis SQL: `SELECT * FROM daily_revenue WHERE date = '2023-10-15'` tanpa perlu memahami proses cleaning di baliknya.

**✏️ Pertanyaan Refleksi Lab 2:**

1. Mengapa kita menggunakan `.mode("append")` di Bronze tapi `.mode("overwrite")` di Silver dan Gold?
2. Apa yang terjadi jika kita menjalankan script Bronze dua kali? Apakah data jadi duplikat di Bronze? Bagaimana cara mencegahnya?
3. Bagaimana cara menambahkan metrik lain ke Gold layer, misalnya rata-rata nilai transaksi per hari?

---

## Lab 3: Time Travel & Schema Evolution (20 menit)

**Tujuan:** Memahami kemampuan unik Delta Lake: melihat data di versi lampau dan mengubah skema.

### Langkah 1: Lakukan Update & Lihat History

Copy isi `04_time_travel.py` ke notebook:

```python
from delta.tables import DeltaTable

silver_path = "./data/silver/transactions"
deltaTable = DeltaTable.forPath(spark, silver_path)

# Update: ubah semua transaksi PENDING menjadi COMPLETED
print("Melakukan UPDATE pada Silver table...")
deltaTable.update(
    condition = "status = 'PENDING'",
    set = { "status": "'COMPLETED'" }
)

# Lihat history tabel
print("\nHistory tabel Silver:")
deltaTable.history().select("version", "timestamp", "operation").show()
```

### Langkah 2: Query Versi Lama

```python
# Berapa total PENDING sebelum di-update? (Time Travel ke versi 0)
old_df = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
print("Jumlah PENDING sebelum update:")
old_df.filter(col("status") == "PENDING").count()

# Bandingkan dengan versi sekarang
current_df = spark.read.format("delta").load(silver_path)
print("Jumlah PENDING sekarang (setelah update):")
current_df.filter(col("status") == "PENDING").count()
```

### Langkah 3: Schema Evolution — Tambah Kolom Baru

```python
from pyspark.sql.functions import lit

# Simulasi: ada kolom baru 'region' yang ingin ditambahkan ke Silver
silver_df_new = spark.read.format("delta").load(silver_path) \
    .withColumn("region", lit("Jawa Barat"))

# Tanpa mergeSchema, ini akan ERROR
# silver_df_new.write.format("delta").mode("append").save(silver_path)

# Dengan mergeSchema = True, kolom baru 'region' ditambahkan otomatis
silver_df_new.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(silver_path)

print("Skema setelah schema evolution:")
spark.read.format("delta").load(silver_path).printSchema()
```

**✏️ Pertanyaan Refleksi Lab 3:**

1. Apa perbedaan antara `versionAsOf` dan `timestampAsOf` untuk Time Travel?
2. Kapan Anda sebaiknya menggunakan Schema Evolution (`mergeSchema`) dan kapan tidak?
3. Jika tim data science sudah melatih model ML menggunakan data Silver versi 1, bagaimana cara memastikan mereka bisa mereproduksi hasil yang sama 6 bulan kemudian?

---

# 📝 Tugas Week 12 → Dikumpulkan & Dipresentasikan Week 13

Berdasarkan pengalaman hands-on hari ini, kamu dan kelompokmu akan membangun pipeline Data Lakehouse dari tugas ETS sebelumnya

**Detail tugas ada di:** [`materi/tugas/tugas-data-lakehouse.md`](./tugas/tugas-data-lakehouse.md)

---

# 📚 Referensi

1. Armbrust, M., et al. (2021). *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR 2021. [PDF](http://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf)
2. Delta Lake Documentation: [https://docs.delta.io/latest/index.html](https://docs.delta.io/latest/index.html)
3. Databricks. (2023). *Medallion Architecture*. [https://www.databricks.com/glossary/medallion-architecture](https://www.databricks.com/glossary/medallion-architecture)
4. Zaharia, M., et al. (2022). *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores*. VLDB Endowment.
5. Apache Iceberg Documentation: [https://iceberg.apache.org/docs/latest/](https://iceberg.apache.org/docs/latest/)
6. Apache Hudi Documentation: [https://hudi.apache.org/docs/overview/](https://hudi.apache.org/docs/overview/)
7. Dixon, J. (2010). *Pentaho, Hadoop, and Data Lakes*. [Blog Post]