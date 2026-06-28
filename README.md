<p align="center">
  <img src="Assets/Lambang ITS PNG v1.png" alt="Logo Institut Teknologi Sepuluh Nopember" width="150">
</p>

<h1 align="center">LAPORAN FINAL PROJECT</h1>
<h2 align="center">Big Data System — 2026</h2>
<h3 align="center"><em>KECAMATRAS — Kecamatan Metrics & Anomaly Tracking of Surabaya</em></h3>
<h4 align="center">Departemen Teknologi Informasi — Institut Teknologi Sepuluh Nopember</h4>

<p align="center">
  <img src="https://img.shields.io/badge/Big_Data-Apache_Hadoop_HDFS-66CCFF?style=flat-square&logo=apachehadoop&logoColor=black" alt="Hadoop">
  <img src="https://img.shields.io/badge/Engine-Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Streaming-Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/Storage-Delta_Lake-00ADD8?style=flat-square" alt="Delta Lake">
  <img src="https://img.shields.io/badge/Orchestration-Docker_Compose-2496ED?style=flat-square&logo=docker" alt="Docker">
  <img src="https://img.shields.io/badge/Language-Python_3.12-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python">
</p>

---

### Anggota Kelompok

| No | Nama | NRP |
|:--:|------|:---:|
| 1 | Arya Bisma Putra Refman | `5027241036` |
| 2 | Kharisma Fahrun Nisa | `5027231086` |
| 3 | M. Hikari Reiziq Rakhmadinta | `5027241079` |
| 4 | Aras Rizky Ananta | `5027221053` |
| 5 | Ica Zika Hamizah | `5027241058` |

---

## Daftar Isi

- [Tentang Proyek](#tentang-proyek)
- [Arsitektur Sistem](#arsitektur-sistem)
- [Tech Stack](#tech-stack)
- [Sumber Data](#sumber-data)
- [Prasyarat](#prasyarat)
- [Panduan Setup & Instalasi](#panduan-setup--instalasi)
- [Menjalankan Pipeline End-to-End](#menjalankan-pipeline-end-to-end)
- [Dokumentasi Screenshot](#dokumentasi-screenshot)
- [Hasil Akhir (Gold Layer Output)](#hasil-akhir-gold-layer-output)
- [Struktur Direktori](#struktur-direktori)
- [Formula & Metrik](#formula--metrik)
- [Troubleshooting](#troubleshooting)
- [Web UI Monitoring](#web-ui-monitoring)
- [Pilar Big Data yang Diimplementasikan](#pilar-big-data-yang-diimplementasikan)
- [Tim Pengembang](#tim-pengembang)

---

## Tentang Proyek

**KECAMATRAS** adalah singkatan dari **Kecamatan Metrics & Anomaly Tracking of Surabaya**. Proyek ini dibangun untuk menjawab tiga masalah utama:

1. **Human Blindspot** — Masyarakat awam tidak bisa melihat rekam jejak ancaman tersembunyi (kawasan rawan begal, sarang wabah penyakit) hanya dari survei fisik singkat.
2. **Data Fragmentation** — Data kejahatan dan anomali lingkungan berserakan di portal berita dalam bentuk teks mentah (*unstructured data*) yang mustahil dievaluasi secara kuantitatif oleh warga biasa.
3. **Macro-Pattern Blindness** — Tanpa indikator prediktif, masyarakat dan pemerintah lokal terlambat bertindak dan terjebak siklus kerugian berulang.

### Solusi yang Dibangun

Sistem ini menghasilkan **2 indeks bahaya berskala 0-100** untuk setiap kecamatan:

| Indeks | Deskripsi | Skala |
|--------|-----------|-------|
| Indeks Kriminalitas | Mengukur tingkat kerawanan kejahatan fisik (begal, curanmor, narkoba) | 0 (Aman) → 100 (Bahaya) |
| Indeks Kesehatan | Mengukur risiko kesehatan lingkungan berdasarkan wabah penyakit dan ketersediaan faskes | 0 (Aman) → 100 (Berisiko) |

---

## Arsitektur Sistem

Proyek ini menggunakan arsitektur **Medallion (Data Lakehouse)** dengan kombinasi *Event Streaming* dan *Batch Processing*:

```text
                    ┌─────────────────────┐
                    │  Google News RSS    │
                    │  (Berita Surabaya)  │
                    └─────────┬───────────┘
                              │ feedparser + kafka-python
                              ▼
                    ┌─────────────────────┐
                    │   Apache Kafka      │
                    │   (Event Streaming) │
                    └─────────┬───────────┘
                              │ Spark Structured Streaming
                              ▼
┌─────────────┐    ┌──────────────────────┐
│  CSV Files  │───▶│     BRONZE LAYER     │  ← Data mentah, tanpa modifikasi
│  (Statis)   │    │   (HDFS Delta Lake)  │
└─────────────┘    └─────────┬────────────┘
                              │ PySpark (Text Cleaning, Geo-Parsing)
                              ▼
                    ┌──────────────────────┐
                    │     SILVER LAYER     │  ← Data bersih, terstandardisasi
                    │   (HDFS Delta Lake)  │
                    └─────────┬────────────┘
                              │ Spark MLlib (LDA) + Analytical Joins
                              ▼
                    ┌──────────────────────┐
                    │      GOLD LAYER      │  ← Indeks final (0-100)
                    │   (HDFS Delta Lake)  │
                    └──────────────────────┘
```

**Diagram Arsitektur Enterprise-Grade (Infrastruktur Aktual):**

![Arsitektur Enterprise-Grade — Diagram lengkap seluruh komponen KECAMATRAS](Assets/Arsitektur%20Enterprise-Grade%20(HDFS).png)
> *Diagram arsitektur enterprise-grade menunjukkan alur data lengkap: dari **Host/WSL Ubuntu** (Google News RSS → `00_ingestion_api.py` sebagai Kafka Producer, beserta 3 sumber CSV/Hardcode) masuk ke **Docker Compose** yang berisi Pilar 5 (Zookeeper:2181 + Kafka Broker:9092 topic `kecamatras-stream`), diproses oleh Pilar 3 (Spark Master:8080/:7077 + Spark Worker:8081), lalu ditulis ke Pilar 2 (HDFS NameNode:9870/:8020 + DataNode) sebagai Delta Lake tables di **Pilar 6** (4 tabel Bronze: `tbl_raw_news`, `tbl_raw_faskes_baseline`, `tbl_raw_disease_baseline`, `tbl_static_crime_baseline`). YARN (ResourceManager:8088 + NodeManager:8042) mengelola sumber daya klaster.*

Seluruh data disimpan di **HDFS (Hadoop Distributed File System)** dalam format **Delta Lake** — bukan di *local file system* — sesuai *best practice* Big Data.

---

## Tech Stack

| Komponen | Teknologi | Versi | Fungsi |
|----------|-----------|-------|--------|
| <img src="https://img.shields.io/badge/Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black" alt="Hadoop"> | Apache Hadoop HDFS + YARN | 3.3.6 | Distributed Storage & Resource Management |
| <img src="https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka"> | Apache Kafka + Zookeeper | 7.5.0 | Event Streaming Platform |
| <img src="https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark"> | Apache Spark (PySpark) | 3.5.1 | Distributed Processing Engine |
| <img src="https://img.shields.io/badge/Spark_MLlib-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark MLlib"> | Spark MLlib (LDA) | 3.5.1 | Topic Modeling / NLP |
| <img src="https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge" alt="Delta Lake"> | Delta Lake | 3.0.0 | ACID-compliant Data Format |
| <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"> | Docker Compose | — | Containerization (8 services) |
| <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"> | Python | 3.12 | Scripting & Ingestion |

---

## Sumber Data

| No | Dataset | Tipe | Sumber | Jumlah Record |
|----|---------|------|--------|---------------|
| 1 | **Fasilitas Kesehatan** | Statis (CSV) | Portal Satu Data Indonesia | ~30.891 baris |
| 2 | **Data Penyakit Puskesmas** | Statis (CSV) | Portal Satu Data Indonesia | ~67.853 baris |
| 3 | **Kasus Kriminal Baseline** | Statis (Hardcode) | Paper Jurnal Akademis* | 31 kecamatan |
| 4 | **Berita Real-time** | Dinamis (Streaming) | Google News RSS API | ~152+ berita |

> \*Paper: *"Penerapan Statistika Deskriptif dalam Memetakan Titik Rawan Kriminal Berdasarkan Kecamatan pada Kota Surabaya"* — Data kasus kriminal ditranskripsi dari tabel paper ke dalam Python Dictionary di `01_bronze.py`.

---

## Prasyarat

Pastikan sistem Anda sudah memiliki:

- [x] **Docker Desktop** (dengan WSL2 backend jika di Windows)
- [x] **Python 3.10+** (disarankan 3.12)
- [x] **pip** (Python package manager)
- [x] **Git** (untuk clone repository)
- [x] **~4 GB RAM bebas** (untuk menjalankan 8 container Docker)

---

## Panduan Setup & Instalasi

### Langkah 1: Clone Repository

```bash
git clone https://github.com/airenmeyy/kelompok-1-eas-bigdata.git
cd kelompok-1-eas-bigdata
```

### Langkah 2: Install Dependensi Python

```bash
# (Opsional) Buat virtual environment
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
# .venv\Scripts\activate     # Windows

# Install semua library yang dibutuhkan
pip install -r requirements.txt
```

Library yang akan terinstal:

| Library | Fungsi |
|---------|--------|
| `pyspark>=3.3.0` | Engine pemrosesan data terdistribusi |
| `delta-spark>=2.3.0` | Format tabel ACID di atas HDFS |
| `kafka-python-ng>=2.2.2` | Producer Python untuk Apache Kafka |
| `feedparser>=6.0.0` | Parser untuk RSS Feed (Google News) |
| `beautifulsoup4>=4.12.0` | Ekstraksi teks dari HTML |
| `requests>=2.28.0` | HTTP client untuk mengambil data web |

### Langkah 3: Nyalakan Infrastruktur Docker

```bash
# Nyalakan semua 8 container
docker compose up -d

# Tunggu ~60 detik agar HDFS selesai inisialisasi
sleep 60

# Verifikasi: Pastikan 8 container berstatus "running"
docker compose ps
```

Anda seharusnya melihat **8 service** berjalan:

| Service | Container Name | Fungsi |
|---------|---------------|--------|
| `namenode` | kecamatras-namenode | HDFS NameNode (otak penyimpanan) |
| `datanode` | kecamatras-datanode | HDFS DataNode (penyimpan data) |
| `resourcemanager` | kecamatras-resourcemanager | YARN Resource Manager |
| `nodemanager` | kecamatras-nodemanager | YARN Node Manager |
| `zookeeper` | kecamatras-zookeeper | Koordinator Kafka |
| `kafka` | kecamatras-kafka | Event Broker (antrian pesan) |
| `spark-master` | kecamatras-spark-master | Spark Master Node |
| `spark-worker` | kecamatras-spark-worker | Spark Worker Node |

**Screenshot: 8 Docker Container Berjalan**

![Docker Container — 8 service KECAMATRAS berjalan dengan status healthy di Docker Desktop](Assets/docker_container.png)
> *Tampilan Docker Desktop menunjukkan 8 container KECAMATRAS aktif: zookeeper, spark-master, kafka, datanode, spark-worker, resourcemanager, nodemanager, dan namenode.*

### Langkah 4: Verifikasi HDFS via Web UI

Buka browser dan akses:
- **HDFS NameNode Web UI:** [http://localhost:9870](http://localhost:9870)
- **YARN Resource Manager:** [http://localhost:8088](http://localhost:8088)
- **Spark Master Web UI:** [http://localhost:8080](http://localhost:8080)

**Screenshot: Hadoop NameNode Overview**

![Hadoop Overview — NameNode aktif di port 8020 dengan versi 3.3.6](Assets/Hadoop_Overview.png)
> *Halaman Overview HDFS NameNode (`localhost:9870`) menunjukkan klaster Hadoop versi 3.3.6 berstatus active.*

**Screenshot: DataNode Information**

![DataNode Information — 1 node aktif dengan kapasitas 1006 GB](Assets/datanode_information.png)
> *Halaman Datanodes menampilkan 1 DataNode aktif (In service) dengan kapasitas 1006.85 GB, 146 blocks tersimpan, dan usage histogram.*

**Screenshot: DFS Storage Types**

![DFS Storage — NameNode Journal dan Storage Types](Assets/DFS_Storage.png)
> *Detail NameNode Journal Status (802 transactions) dan DFS Storage menunjukkan 2.99 MB data tersimpan di HDFS dengan 932 GB kapasitas tersisa.*

---

## Menjalankan Pipeline End-to-End

Setelah Docker berjalan, eksekusi pipeline secara berurutan:

### Step 1: Ingestion — Tarik Berita dari Google News

```bash
python 00_ingestion_api.py
```

> **Apa yang terjadi?** Skrip ini menarik berita real-time dari Google News RSS Feed menggunakan *query* pencarian terkait kriminalitas dan kesehatan di Surabaya (contoh: `curanmor+OR+begal+surabaya`, `dbd+OR+keracunan+surabaya`), lalu mengirim setiap berita ke Apache Kafka topic `kecamatras-stream` dalam format JSON. Biarkan berjalan 1-2 siklus hingga ~100+ berita terkumpul.

### Step 2: Bronze Layer — Simpan Data Mentah ke HDFS

```bash
# Berikan izin eksekusi (hanya pertama kali)
chmod +x run_bronze_docker.sh

# Jalankan Bronze Layer (batch + streaming)
./run_bronze_docker.sh
```

> **Apa yang terjadi?** PySpark membaca CSV kesehatan & penyakit, hardcode kriminal dari paper, dan berita dari Kafka, lalu menyimpan semuanya **apa adanya** (tanpa modifikasi) ke HDFS dalam format Delta Lake di path `hdfs://namenode:8020/kecamatras/delta/bronze/`.

**4 tabel yang dihasilkan:**
| Tabel | Sumber |
|-------|--------|
| `tbl_raw_faskes_baseline` | CSV fasilitas kesehatan |
| `tbl_raw_disease_baseline` | CSV data penyakit |
| `tbl_static_crime_baseline` | Hardcode dari paper |
| `tbl_raw_news` | Berita streaming dari Kafka |

### Step 3: Silver Layer — Bersihkan & Standardisasi Data

```bash
chmod +x run_silver_docker.sh
./run_silver_docker.sh
```

> **Apa yang terjadi?** PySpark membaca data mentah dari Bronze, melakukan:
> - **Text Preprocessing:** Lowercasing, hapus HTML tags, karakter spesial, URL
> - **Geo-Parsing:** Regex matching untuk mendeteksi nama 31 kecamatan dari teks berita
> - **Kategorisasi:** Menandai berita sebagai "Kriminalitas" atau "Kesehatan"
> - **Standardisasi:** Casting tipe data kolom numerik dan formatting tanggal

**4 tabel yang dihasilkan:**
| Tabel | Proses |
|-------|--------|
| `tbl_clean_faskes` | Agregasi faskes per kecamatan |
| `tbl_clean_disease` | Agregasi kasus penyakit per kecamatan |
| `tbl_clean_crime_baseline` | Normalisasi nama kecamatan |
| `tbl_clean_news` | Berita bersih + kecamatan terdeteksi |

### Step 4: Gold Layer — Hitung Indeks Final dengan ML

```bash
chmod +x run_gold_docker.sh
./run_gold_docker.sh
```

> **Apa yang terjadi?** Ini adalah tahap paling kompleks di mana PySpark menjalankan:
> 1. **Spark MLlib LDA** — Topic modeling untuk mengklasifikasikan berita ke kategori Kriminalitas atau Kesehatan
> 2. **Dynamic Topic Mapping** — Menentukan label klaster secara otomatis berdasarkan *keyword intersection* (anti *Topic Flipping*)
> 3. **StopWordsRemover** — Ratusan *stop words* bahasa Indonesia diinjeksi manual
> 4. **Analytical Joins** — Menggabungkan data berita + baseline + demografi
> 5. **Min-Max Normalization** — Menghitung indeks final skala 0-100

**2 tabel indeks final:**
| Tabel | Deskripsi |
|-------|-----------|
| `tbl_index_kriminalitas` | Ranking kecamatan berdasarkan tingkat kerawanan kriminal (0-100) |
| `tbl_index_kesehatan` | Ranking kecamatan berdasarkan risiko kesehatan lingkungan (0-100) |

### Step 5: Verifikasi — Lihat Hasil dari HDFS

```bash
chmod +x ambil_dokumentasi.sh
./ambil_dokumentasi.sh
```

> **Apa yang terjadi?** Script ini menjalankan PySpark di dalam Docker network, membaca ketiga layer (Bronze, Silver, Gold) dari HDFS, dan mencetak cuplikan data di terminal Anda untuk keperluan dokumentasi.

---

## Dokumentasi Screenshot

### Infrastruktur HDFS — Struktur Direktori Medallion

**Browse Directory `/kecamatras/delta`** — Tiga folder layer (bronze, silver, gold):

![Browse Directory — Struktur folder Medallion Architecture di HDFS](Assets/Browse_Directory.png)
> *HDFS File Browser menunjukkan 3 direktori utama: `bronze`, `gold`, dan `silver` di dalam path `/kecamatras/delta`. Ini adalah implementasi nyata dari Medallion Architecture di atas Hadoop Distributed File System.*

---

### 🥉 Bronze Layer — Data Mentah (Raw Data)

**Direktori Bronze di HDFS:**

![Bronze Directory — 4 tabel raw data tersimpan di HDFS](Assets/Bronze_Directory.png)
> *HDFS path `/kecamatras/delta/bronze` berisi 4 tabel Delta Lake: `tbl_raw_disease_baseline` (data penyakit), `tbl_raw_faskes_baseline` (fasilitas kesehatan), `tbl_raw_news` (berita streaming dari Kafka), and `tbl_static_crime_baseline` (data kriminal hardcode dari paper).*

**Output Terminal — Verifikasi Data Bronze:**

![Dokumentasi Bronze — 152 berita berhasil masuk dari Kafka ke HDFS](Assets/dokumentasi_bronze.png)
> *Hasil eksekusi `ambil_dokumentasi.sh` menampilkan 5 sampel data dari `tbl_raw_news`. Setiap baris memiliki `kafka_key` (ID unik berita), `kafka_topic` (nama topik: `kecamatras-stream`), and `kafka_timestamp` (waktu masuk ke Kafka). Total: **152 baris** data streaming berhasil disimpan ke HDFS.*

---

### 🥈 Silver Layer — Data Bersih (Cleaned & Parsed)

**Direktori Silver di HDFS:**

![Silver Directory — 4 tabel bersih tersimpan di HDFS](Assets/Silver_Directory.png)
> *HDFS path `/kecamatras/delta/silver` berisi 4 tabel: `tbl_clean_crime_baseline`, `tbl_clean_disease`, `tbl_clean_faskes`, dan `tbl_clean_news`. Semua data telah dibersihkan, distandardisasi, dan diperkaya dengan kolom `kecamatan_terdeteksi` melalui proses Geo-Parsing.*

**Output Terminal — Verifikasi Data Silver:**

![Dokumentasi Silver — Berita terkategorisasi dan kecamatan terdeteksi](Assets/dokumentasi_silver.png)
> *Hasil verifikasi Silver Layer menampilkan 3 kategori output:*
> - *Data **Kriminalitas**: Berita kriminal yang berhasil diekstrak*
> - *Data **Kesehatan**: Berita wabah/penyakit (contoh: Mulyorejo terdeteksi)*
> - *Data dengan **Kecamatan Terdeteksi**: Wiyung, Dukuh Pakis, Wonokromo — berhasil di-parse dari teks berita menggunakan Regex*
> 
> *Total: **152 berita** berhasil dibersihkan dan dikategorisasi.*

---

### 🥇 Gold Layer — Indeks Final (Business Index)

**Direktori Gold di HDFS:**

![Gold Directory — 2 tabel indeks final di HDFS](Assets/Gold_Directory.png)
> *HDFS path `/kecamatras/delta/gold` berisi 2 tabel hasil akhir: `tbl_index_kesehatan` (Indeks Risiko Kesehatan Lingkungan) dan `tbl_index_kriminalitas` (Indeks Kerawanan Kriminalitas). Kedua tabel ini adalah output final dari seluruh pipeline Medallion.*

**Output Terminal — Indeks Kriminalitas & Kesehatan:**

![Dokumentasi Gold — Ranking indeks kriminalitas dan kesehatan per kecamatan](Assets/dokumentasi_gold.png)
> *Hasil akhir Gold Layer menampilkan:*
> - *`tbl_index_kriminalitas`: Kecamatan **Tandes** menduduki peringkat 1 dengan crime rate 192.84 dan indeks **100.0** (paling rawan)*
> - *`tbl_index_kesehatan`: Kecamatan **Semampir** memiliki risiko kesehatan tertinggi dengan incidence rate 1.072.930 dan indeks **79.7***

---

### Proses Resolusi Dependensi Spark

![Dependensi — PySpark mengunduh dan me-resolve Delta Lake dependencies](Assets/dependensi.png)
> *Terminal menunjukkan proses `ambil_dokumentasi.sh`: PySpark secara otomatis mengunduh 3 artifact Maven (delta-spark, delta-storage, antlr4-runtime) sebesar 5230 KB untuk mengaktifkan Delta Lake support. Proses ini terjadi di dalam Docker container yang terhubung ke `kecamatras-net`.*

---

### Visualisasi Web Dashboard & Analisis Geospasial

Berikut dokumentasi antarmuka visual KECAMATRAS Dashboard yang memetakan indeks anomali dan sebaran berita secara interaktif:

**Halaman Ringkasan Dashboard** — Peta 3D Mapbox choropleth indeks kerawanan:
![Halaman Ringkasan](Assets/halaman_ringkasan.png)

**Halaman Informasi Kecamatan** — Detail statistik kasus, faskes, dan daftar berita per kecamatan:
![Halaman Informasi Kecamatan](Assets/halaman_informasi_kecamatan.png)

**Halaman Analisis Kriminalitas** — Grafik tren kriminal, perbandingan antar wilayah, dan ranking:
![Halaman Kriminalitas](Assets/halaman_kriminalitas.png)

**Halaman Analisis Kesehatan** — Data sebaran penyakit puskesmas dan indeks risiko kesehatan:
![Halaman Kesehatan](Assets/halaman_kesehatan.png)

**Halaman Portal Berita Anomali** — Daftar feed berita terklasifikasi otomatis dengan filter rentang waktu:
![Halaman Berita Anomali](Assets/halaman_berita_anomali.png)

**Halaman Prediksi AI** — Integrasi LLM untuk analisis anomali dan rekomendasi kebijakan:
![Halaman Prediksi AI](Assets/halaman_prediksi_ai.png)

**Panel Pengaturan AI & API Key** — Konfigurasi model Gemini API untuk asisten pintar:
![Card Pengaturan AI](Assets/card_pengaturan_ai.png)

---

## Hasil Akhir (Gold Layer Output)

### Indeks Kriminalitas — Top 5 Kecamatan Paling Rawan

| Rank | Kecamatan | Kasus Baseline | Kasus dari Berita | Total | Crime Rate (/100.000) | Indeks (0-100) |
|------|-----------|----------------|-------------------|-------|-----------------------|----------------|
| 1 | **Tandes** | 175 | 2 | 177 | 192,84 | **100,0** |
| 2 | **Kenjeran** | 104 | 0 | 104 | 178,65 | **92,6** |
| 3 | **Asemrowo** | 84 | 0 | 84 | 171,99 | **89,1** |
| 4 | Semampir | 78 | 4 | 82 | 171,41 | 88,8 |
| 5 | Jambangan | 89 | 0 | 89 | 164,17 | 85,1 |

### Indeks Kesehatan Lingkungan — Top 5 Kecamatan Paling Berisiko

| Rank | Kecamatan | Incidence Rate | Health Facility Ratio | Indeks (0-100) |
|------|-----------|---------------|----------------------|----------------|
| 1 | **Semampir** | 1.072.930 | 167,4 | **79,7** |
| 2 | **Kenjeran** | 934.375 | 110,1 | **77,6** |
| 3 | **Krembangan** | 308.303 | 58,1 | **43,1** |
| 4 | Bubutan | 262.367 | 54,4 | 40,5 |
| 5 | Sawahan | 261.215 | 60,4 | 39,7 |

---

## Struktur Direktori

```text
kelompok-1-eas-bigdata/
│
├── README.md                    # Dokumentasi utama (file ini)
├── prd.md                       # Product Requirements Document
├── catatan_terakhir.md          # Catatan lengkap pengerjaan
├── requirements.txt             # Dependensi Python
├── docker-compose.yml           # Definisi 8 service Docker
├── hadoop.env                   # Environment variables Hadoop
│
├── PIPELINE SCRIPTS
│   ├── 00_ingestion_api.py         # Kafka Producer (Google News RSS → Kafka)
│   ├── 01_bronze.py                # Bronze Layer (Raw → HDFS Delta)
│   ├── 02_silver.py                # Silver Layer (Clean + Geo-Parse)
│   └── 03_gold.py                  # Gold Layer (LDA + Index Calculation)
│
├── SHELL WRAPPERS (Eksekusi di Docker Network)
│   ├── run_bronze_docker.sh        # Menjalankan 01_bronze.py
│   ├── run_silver_docker.sh        # Menjalankan 02_silver.py
│   ├── run_gold_docker.sh          # Menjalankan 03_gold.py
│   ├── setup_bronze.sh             # Setup awal direktori HDFS
│   └── ambil_dokumentasi.sh        # Ambil cuplikan data untuk screenshot
│
├── DATASET SUMBER
│   └── raw_data/
│       └── kesehatan/
│           ├── new-faskes_kecamatan_2023_2026.csv        # ~30.891 baris
│           └── new-penyakit_puskesmas_2022_2026-1.csv    # ~67.853 baris
│
├── SCREENSHOT DOKUMENTASI
│   └── Assets/
│       ├── docker_container.png      # 8 container Docker berjalan
│       ├── Hadoop_Overview.png       # HDFS NameNode Overview
│       ├── datanode_information.png   # DataNode detail & histogram
│       ├── DFS_Storage.png           # DFS Storage & Journal
│       ├── Browse_Directory.png      # Struktur folder Medallion
│       ├── Bronze_Directory.png      # 4 tabel Bronze di HDFS
│       ├── Silver_Directory.png      # 4 tabel Silver di HDFS
│       ├── Gold_Directory.png        # 2 tabel Gold di HDFS
│       ├── dependensi.png            # Resolusi dependensi Spark
│       ├── dokumentasi_bronze.png    # Output terminal Bronze
│       ├── dokumentasi_silver.png    # Output terminal Silver
│       └── dokumentasi_gold.png      # Output terminal Gold
│
├── VERIFIKASI
│   ├── dokumentasi_data.py         # Script PySpark untuk verifikasi HDFS
│   ├── verify_gold.py              # Verifikasi output Gold
│   ├── verify_silver.py            # Verifikasi output Silver
│   └── output_dokumentasi_data.txt # Output terminal terakhir
│
└── REFERENSI
    ├── Crime_rate_1.png             # Screenshot paper (halaman 1)
    ├── Crime_rate_2.png             # Screenshot paper (halaman 2)
    └── PENERAPAN STATISTIKA DESKRIPTIF DALAM MEMETAKAN
        TITIK RAWAN KRIMINAL BERDASARKAN KECAMATAN
        PADA KOTA SURABAYA.pdf       # Paper jurnal sumber data kriminal
```

---

## Formula & Metrik

### Indeks Kriminalitas (Street Crime Index)

```text
Crime Rate (CR) = (Total Kasus Baseline + Kasus dari Berita) / Penduduk × 100.000

Indeks Kriminalitas = (CR_kecamatan - CR_min) / (CR_max - CR_min) × 100
```

### Indeks Kesehatan (Environmental Health Risk Index)

```text
Incidence Rate (IR) = Total Kasus Penyakit / Penduduk × 100.000
Health Facility Ratio (HFR) = Total Faskes / Penduduk × 10.000

Indeks Kesehatan = 0.7 × Norm(IR) + 0.3 × (100 - Norm(HFR))
```

> **Catatan:** `Norm()` = Normalisasi Min-Max ke skala 0-100. Komponen HFR dibalik (100 - Norm) karena semakin banyak faskes = semakin rendah risikonya.

---

## Troubleshooting

| Masalah | Penyebab | Solusi |
|---------|----------|--------|
| `bitnami/spark:3.5: not found` | Docker image tidak tersedia di registry | Gunakan `bde2020/spark-master:3.3.0-hadoop3.3` |
| `ModuleNotFoundError: No module named 'numpy'` | Library numpy tidak ada di container Python | Tambahkan `numpy` ke pip install di shell script |
| `ModuleNotFoundError: No module named 'distutils'` | Python 3.12 menghapus distutils | Tambahkan `setuptools` ke pip install |
| `HDFS: Connection refused` | NameNode belum selesai inisialisasi | Tunggu ~60 detik setelah `docker compose up` |
| `Kafka: NoBrokersAvailable` | Kafka broker belum aktif | Tunggu ~30 detik atau restart container kafka |
| Container tidak mau nyala | Port conflict di host | Pastikan port 8020, 9870, 9092, 8080 tidak dipakai |

---

## Web UI Monitoring

Setelah Docker berjalan, Anda bisa memantau klaster melalui browser:

| Service | URL | Fungsi |
|---------|-----|--------|
| HDFS NameNode | [http://localhost:9870](http://localhost:9870) | File browser, storage info, DataNode status |
| YARN ResourceManager | [http://localhost:8088](http://localhost:8088) | Job monitoring, resource allocation |
| Spark Master | [http://localhost:8080](http://localhost:8080) | Worker status, running applications |
| NodeManager | [http://localhost:8042](http://localhost:8042) | Container logs, node health |

---

## Pilar Big Data yang Diimplementasikan

Proyek ini mengintegrasikan **6 pilar utama** materi kuliah Big Data:

| Pilar | Materi | Implementasi di KECAMATRAS |
|-------|--------|---------------------------|
| 1 | Pengantar Big Data | Memahami konsep 5V dan kebutuhan distributed computing |
| 2 | Apache Hadoop | HDFS sebagai distributed storage + YARN sebagai resource manager |
| 3 | Apache Spark | PySpark sebagai engine ETL dan kalkulasi indeks terdistribusi |
| 4 | Spark MLlib | LDA untuk topic modeling pada corpus berita berbahasa Indonesia |
| 5 | Apache Kafka | Event streaming real-time dari Google News RSS ke pipeline analitik |
| 6 | Data Lakehouse | Delta Lake sebagai format ACID-compliant di atas HDFS |

---

## Tim Pengembang

<table align="center">
  <tr>
    <td align="center"><strong>Kelompok 1</strong></td>
  </tr>
  <tr>
    <td align="center">Departemen Teknologi Informasi — Institut Teknologi Sepuluh Nopember (ITS) Surabaya</td>
  </tr>
  <tr>
    <td align="center">Mata Kuliah Big Data — Semester 4 (2026)</td>
  </tr>
</table>

| No | Nama | NRP |
|:--:|------|:---:|
| 1 | Arya Bisma Putra Refman | `5027241036` |
| 2 | Kharisma Fahrun Nisa | `5027231086` |
| 3 | M. Hikari Reiziq Rakhmadinta | `5027241079` |
| 4 | Aras Rizky Ananta | `5027221053` |
| 5 | Ica Zika Hamizah | `5027241058` |

<p align="center">
  <em>Built using Apache Hadoop, Spark, Kafka, and Delta Lake</em>
</p>
