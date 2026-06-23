# CATATAN TERAKHIR — Proyek KECAMATRAS
### Rekap Lengkap Seluruh Proses dari Awal Hingga Akhir

**Project:** KECAMATRAS (Kecamatan Metrics & Anomaly Tracking of Surabaya)  
**Team:** Anti Gravity — Institut Teknologi Sepuluh Nopember (ITS)  
**Tanggal Mulai:** 23 Juni 2026  
**Tanggal Selesai:** 24 Juni 2026  

---

## FASE 1: PERSIAPAN DATA & RISET SUMBER

### 1.1 Identifikasi Dataset
Kami mengumpulkan 3 jenis sumber data utama:

| No | Dataset | File/Sumber | Jumlah Record | Format |
|----|---------|-------------|---------------|--------|
| 1 | Fasilitas Kesehatan (Faskes) | `raw_data/kesehatan/new-faskes_kecamatan_2023_2026.csv` | ~30.891 baris | CSV (`;` separated) |
| 2 | Data Penyakit Puskesmas | `raw_data/kesehatan/new-penyakit_puskesmas_2022_2026-1.csv` | ~67.853 baris | CSV (`;` separated) |
| 3 | Data Kriminal Baseline | Paper: *"Penerapan Statistika Deskriptif dalam Memetakan Titik Rawan Kriminal Berdasarkan Kecamatan pada Kota Surabaya"* | 31 kecamatan | Hardcode (dari screenshot `Crime_rate_1.png` & `Crime_rate_2.png`) |

**Catatan penting:** Data kriminal **tidak** tersedia dalam bentuk CSV/API publik. Data diambil dari tabel paper jurnal akademis yang di-*screenshot*, kemudian ditranskripsi secara manual menjadi Python Dictionary di dalam `01_bronze.py`.

### 1.2 Data Kriminal yang Di-Hardcode (31 Kecamatan)
Berikut data lengkap yang ditranskripsi dari paper:

| Kecamatan | Kasus | Penduduk | Crime Rate |
|-----------|-------|----------|------------|
| Karang Pilang | 77 | 75.503 | 102,0 |
| Jambangan | 89 | 54.212 | 164,2 |
| Gayungan | 50 | 43.846 | 114,0 |
| Wonocolo | 15 | 80.034 | 18,7 |
| Tenggilis Mejoyo | 89 | 58.932 | 151,0 |
| Gunung Anyar | 92 | 62.342 | 147,6 |
| Rungkut | 39 | 123.653 | 31,5 |
| Sukolilo | 0 | 115.913 | 0,0 |
| Mulyorejo | 100 | 88.214 | 113,4 |
| Gubeng | 6 | 132.382 | 4,5 |
| Wonokromo | 75 | 153.563 | 48,8 |
| Dukuh Pakis | 75 | 59.345 | 126,4 |
| Wiyung | 0 | 76.501 | 0,0 |
| Lakarsantri | 23 | 65.013 | 35,4 |
| Sambikerep | 33 | 69.076 | 47,8 |
| Tandes | 175 | 91.784 | **190,8** |
| Sukomanunggal | 82 | 104.166 | 78,7 |
| Sawahan | 167 | 198.516 | 84,1 |
| Tegalsari | 144 | 97.511 | 147,6 |
| Genteng | 70 | 58.216 | 120,3 |
| Tambaksari | 19 | 97.511 | 19,5 |
| Kenjeran | 104 | 58.216 | 178,6 |
| Bulak | 163 | 227.025 | 71,8 |
| Simokerto | 71 | 185.294 | 38,3 |
| Semampir | 78 | 47.839 | 163,1 |
| Pabean Cantian | 71 | 73.931 | 96,0 |
| Bubutan | 23 | 96.704 | 23,8 |
| Krembangan | 72 | 114.866 | 62,7 |
| Asemrowo | 84 | 48.841 | 172,0 |
| Benowo | 92 | 74.933 | 122,8 |
| Pakal | 61 | 64.515 | 94,6 |

---

## FASE 2: SETUP INFRASTRUKTUR DOCKER

### 2.1 Instalasi Dependensi
```bash
pip install -r requirements.txt
```
Dependensi utama:
- `pyspark>=3.3.0` — Engine pemrosesan terdistribusi
- `delta-spark>=2.3.0` — Format tabel ACID di atas HDFS
- `kafka-python-ng>=2.2.2` — Producer Python untuk Kafka
- `feedparser>=6.0.0` — Parser RSS feed
- `beautifulsoup4>=4.12.0` — Ekstraksi teks HTML
- `requests>=2.28.0` — HTTP client

### 2.2 Docker Compose
File `docker-compose.yml` mendefinisikan **8 service** container:

```
┌─────────────────────────────────────────────────┐
│              Docker Network: kecamatras-net       │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │ namenode │  │ datanode │  │resourcemanager│   │
│  │  :9870   │  │          │  │    :8088      │   │
│  │  :8020   │  │          │  │               │   │
│  └──────────┘  └──────────┘  └──────────────┘   │
│                                                   │
│  ┌────────────┐ ┌──────────┐ ┌──────────────┐   │
│  │nodemanager │ │zookeeper │ │    kafka      │   │
│  │   :8042    │ │  :2181   │ │    :9092      │   │
│  └────────────┘ └──────────┘ └──────────────┘   │
│                                                   │
│  ┌──────────────┐  ┌───────────────┐             │
│  │ spark-master │  │ spark-worker  │             │
│  │ :8080, :7077 │  │    :8081      │             │
│  └──────────────┘  └───────────────┘             │
└─────────────────────────────────────────────────┘
```

### 2.3 Cara Menjalankan
```bash
# 1. Nyalakan seluruh infrastruktur
docker compose up -d

# 2. Tunggu HDFS siap (~60 detik)
sleep 60

# 3. Verifikasi 8 container running
docker compose ps
```

### 2.4 Troubleshooting yang Ditemui
| Masalah | Solusi |
|---------|--------|
| `bitnami/spark:3.5` not found | Ganti ke `bde2020/spark-master:3.3.0-hadoop3.3` |
| `bitnami/spark:latest` not found | Sama seperti di atas |
| `ModuleNotFoundError: No module named 'numpy'` | Tambahkan `numpy` ke pip install di `run_gold_docker.sh` |
| `ModuleNotFoundError: No module named 'distutils'` | Tambahkan `setuptools` ke pip install (Python 3.12 compat) |
| `Column 'topic' not found` di Bronze | Kolom sebenarnya bernama `kafka_topic` (alias dari `01_bronze.py`) |

---

## FASE 3: INGESTION (Layer 0 — Data Producer)

### 3.1 Skrip: `00_ingestion_api.py`
**Fungsi:** Menarik berita real-time dari Google News RSS Feed menggunakan query pencarian spesifik terkait kriminalitas dan kesehatan di Surabaya, lalu mengirimkannya ke Kafka topic `kecamatras-stream`.

**Query pencarian contoh:**
- `curanmor+OR+begal+kecamatan+surabaya`
- `narkoba+OR+tawuran+surabaya`
- `dbd+OR+diare+OR+keracunan+surabaya`

**Cara menjalankan:**
```bash
python 00_ingestion_api.py
```

**Output:** ~152 berita JSON masuk ke Kafka topic dalam beberapa siklus.

---

## FASE 4: BRONZE LAYER (Raw Data → HDFS)

### 4.1 Skrip: `01_bronze.py`
**Fungsi:** Memuat seluruh data mentah (CSV + hardcode + Kafka stream) ke HDFS dalam format Delta Lake secara *append-only*.

**Tabel yang dihasilkan di HDFS:**

| Tabel | Sumber | Mode | Path HDFS |
|-------|--------|------|-----------|
| `tbl_raw_faskes_baseline` | `new-faskes_kecamatan_2023_2026.csv` | Batch | `hdfs://namenode:8020/kecamatras/delta/bronze/tbl_raw_faskes_baseline` |
| `tbl_raw_disease_baseline` | `new-penyakit_puskesmas_2022_2026-1.csv` | Batch | `hdfs://namenode:8020/kecamatras/delta/bronze/tbl_raw_disease_baseline` |
| `tbl_static_crime_baseline` | Hardcode Python Dict (dari paper) | Batch | `hdfs://namenode:8020/kecamatras/delta/bronze/tbl_static_crime_baseline` |
| `tbl_raw_news` | Kafka topic `kecamatras-stream` | Streaming | `hdfs://namenode:8020/kecamatras/delta/bronze/tbl_raw_news` |

**Cara menjalankan:**
```bash
# Batch (CSV + Hardcode)
chmod +x run_bronze_docker.sh
./run_bronze_docker.sh

# Streaming (Kafka → HDFS, berjalan terus-menerus)
./run_bronze_docker.sh --stream-only
```

**Hasil:** 152 baris berita berhasil masuk ke `tbl_raw_news` + ribuan baris data statis.

---

## FASE 5: SILVER LAYER (Cleaned & Parsed Data)

### 5.1 Skrip: `02_silver.py`
**Fungsi:** Membersihkan dan menstandarisasi data dari Bronze Layer. Proses utama:

1. **Text Preprocessing:** Lowercasing, penghapusan HTML tags, karakter spesial, dan URL.
2. **Geo-Parsing:** Regex matching untuk mendeteksi nama 31 kecamatan Surabaya dari judul/konten berita.
3. **Kategorisasi:** Menandai berita sebagai "Kriminalitas" atau "Kesehatan" berdasarkan kata kunci.
4. **Standardisasi Tipe Data:** Casting kolom numerik, formatting tanggal.

**Tabel yang dihasilkan:**

| Tabel | Proses | Path HDFS |
|-------|--------|-----------|
| `tbl_clean_faskes` | Agregasi jumlah faskes per kecamatan | `silver/tbl_clean_faskes` |
| `tbl_clean_disease` | Agregasi kasus penyakit per kecamatan | `silver/tbl_clean_disease` |
| `tbl_clean_crime_baseline` | Normalisasi nama kecamatan | `silver/tbl_clean_crime_baseline` |
| `tbl_clean_news` | Text cleaning + geo-parsing + kategorisasi | `silver/tbl_clean_news` |

**Cara menjalankan:**
```bash
chmod +x run_silver_docker.sh
./run_silver_docker.sh
```

**Hasil verifikasi:**
- 152 berita berhasil dibersihkan
- Kategori terdeteksi: Kriminalitas & Kesehatan
- Kecamatan terdeteksi contoh: Wiyung, Dukuh Pakis, Wonokromo, Mulyorejo, Tandes, Semampir, dll.

---

## FASE 6: GOLD LAYER (Business Index & Machine Learning)

### 6.1 Skrip: `03_gold.py`
**Fungsi:** Memuat data bersih dari Silver, menjalankan NLP (Spark MLlib LDA), dan menghitung indeks final.

### 6.2 Proses Machine Learning
1. **Tokenizer:** Memecah teks berita menjadi token kata.
2. **StopWordsRemover:** Menghapus ratusan *stop words* bahasa Indonesia yang diinjeksi secara manual.
3. **CountVectorizer + IDF:** Membangun representasi TF-IDF dari corpus berita.
4. **LDA (Latent Dirichlet Allocation):** Model topic modeling dengan `k=2, seed=42`.
5. **Dynamic Topic Mapping:** Menentukan label klaster berdasarkan skor *intersection* kata kunci referensi:
   - `kriminal_keywords`: narkoba, begal, curanmor, polisi, ungkap, dll.
   - `sehat_keywords`: dbd, diare, keracunan, wabah, penyakit, dll.

### 6.3 Perhitungan Indeks
**Indeks Kriminalitas:**
```
total_kasus_kriminal = kasus_baseline + kasus_dari_berita_LDA
crime_rate = total_kasus / penduduk × 100.000
indeks = min-max normalization(crime_rate) × 100
```

**Indeks Kesehatan:**
```
incidence_rate = total_kasus_penyakit / penduduk × 100.000
hfr = total_faskes / penduduk × 10.000
indeks = 0.7 × norm(IR) + 0.3 × (100 - norm(HFR))
```

**Cara menjalankan:**
```bash
chmod +x run_gold_docker.sh
./run_gold_docker.sh
```

### 6.4 Tabel Output Gold Layer

**`tbl_index_kriminalitas` — Top 5 Kecamatan Paling Rawan:**

| Kecamatan | Kasus Baseline | Kasus Berita | Total | Crime Rate | Indeks |
|-----------|----------------|--------------|-------|------------|--------|
| Tandes | 175 | 2 | 177 | 192,84 | **100,0** |
| Kenjeran | 104 | 0 | 104 | 178,65 | 92,6 |
| Asemrowo | 84 | 0 | 84 | 171,99 | 89,1 |
| Semampir | 78 | 4 | 82 | 171,41 | 88,8 |
| Jambangan | 89 | 0 | 89 | 164,17 | 85,1 |

**`tbl_index_kesehatan` — Top 5 Kecamatan Paling Berisiko:**

| Kecamatan | Incidence Rate | HFR | Indeks |
|-----------|---------------|-----|--------|
| Semampir | 1.072.930 | 167,4 | **79,7** |
| Kenjeran | 934.375 | 110,1 | 77,6 |
| Krembangan | 308.303 | 58,1 | 43,1 |
| Bubutan | 262.367 | 54,4 | 40,5 |
| Sawahan | 261.215 | 60,4 | 39,7 |

---

## FASE 7: DOKUMENTASI & VERIFIKASI

### 7.1 Skrip Dokumentasi
- **`dokumentasi_data.py`** — Script PySpark untuk menampilkan cuplikan data dari setiap layer (Bronze, Silver, Gold) langsung dari HDFS.
- **`ambil_dokumentasi.sh`** — Shell wrapper yang menjalankan `dokumentasi_data.py` di dalam Docker network `kecamatras-net`.

**Cara menjalankan:**
```bash
chmod +x ambil_dokumentasi.sh
./ambil_dokumentasi.sh
```

### 7.2 Output Verifikasi
Output lengkap tersimpan di `output_dokumentasi_data.txt`. Semua layer terverifikasi:
- ✅ **Bronze:** 152 berita streaming dari Kafka berhasil masuk ke HDFS
- ✅ **Silver:** 152 berita berhasil dibersihkan, kategori dan kecamatan terdeteksi
- ✅ **Gold:** Indeks Kriminalitas dan Kesehatan berhasil dihitung (skala 0-100)

---

## DAFTAR FILE PROYEK

### Skrip Utama (Pipeline)
| File | Deskripsi |
|------|-----------|
| `00_ingestion_api.py` | Kafka Producer — menarik berita dari Google News RSS |
| `01_bronze.py` | Bronze Layer — raw ingestion ke HDFS Delta |
| `02_silver.py` | Silver Layer — text cleaning, geo-parsing |
| `03_gold.py` | Gold Layer — LDA, analytical joins, indeks |

### Shell Wrappers (Eksekusi Docker)
| File | Deskripsi |
|------|-----------|
| `run_bronze_docker.sh` | Menjalankan `01_bronze.py` di Docker network |
| `run_silver_docker.sh` | Menjalankan `02_silver.py` di Docker network |
| `run_gold_docker.sh` | Menjalankan `03_gold.py` di Docker network |
| `setup_bronze.sh` | Setup awal HDFS directories |
| `ambil_dokumentasi.sh` | Menjalankan skrip dokumentasi |

### Konfigurasi
| File | Deskripsi |
|------|-----------|
| `docker-compose.yml` | Definisi 8 service Docker |
| `hadoop.env` | Environment variables untuk Hadoop |
| `requirements.txt` | Python dependencies |
| `prd.md` | Product Requirements Document |

### Dataset Sumber
| File | Deskripsi |
|------|-----------|
| `raw_data/kesehatan/new-faskes_kecamatan_2023_2026.csv` | Data faskes per kecamatan (30.891 baris) |
| `raw_data/kesehatan/new-penyakit_puskesmas_2022_2026-1.csv` | Data penyakit puskesmas (67.853 baris) |
| `Crime_rate_1.png` & `Crime_rate_2.png` | Screenshot paper untuk hardcode kriminal |

### Utilitas Verifikasi
| File | Deskripsi |
|------|-----------|
| `dokumentasi_data.py` | Menampilkan cuplikan data dari HDFS |
| `verify_gold.py` | Verifikasi output Gold Layer |
| `verify_silver.py` | Verifikasi output Silver Layer |
| `output_dokumentasi_data.txt` | Output terminal verifikasi terakhir |

---

## ALUR DATA END-TO-END (Ringkasan)

```
┌──────────────────────┐
│  Google News RSS API │
│  (Berita Surabaya)   │
└──────────┬───────────┘
           │ feedparser + kafka-python
           ▼
┌──────────────────────┐
│  Apache Kafka        │
│  topic: kecamatras   │
│  -stream             │
└──────────┬───────────┘
           │ Spark Structured Streaming
           ▼
┌──────────────────────┐    ┌─────────────────────┐
│  BRONZE LAYER        │    │  CSV Files           │
│  (HDFS Delta Lake)   │◄───│  (Faskes, Penyakit,  │
│  • tbl_raw_news      │    │   Crime Baseline)    │
│  • tbl_raw_faskes    │    └─────────────────────┘
│  • tbl_raw_disease   │
│  • tbl_static_crime  │
└──────────┬───────────┘
           │ PySpark (Text Cleaning, Geo-Parsing)
           ▼
┌──────────────────────┐
│  SILVER LAYER        │
│  (HDFS Delta Lake)   │
│  • tbl_clean_news    │
│  • tbl_clean_faskes  │
│  • tbl_clean_disease │
│  • tbl_clean_crime   │
└──────────┬───────────┘
           │ Spark MLlib LDA + Analytical Joins
           ▼
┌──────────────────────────────────────────┐
│  GOLD LAYER (HDFS Delta Lake)            │
│  • tbl_index_kriminalitas (0-100)        │
│  • tbl_index_kesehatan    (0-100)        │
└──────────────────────────────────────────┘
```

---

## KESIMPULAN

Pipeline Big Data **KECAMATRAS** telah berhasil dibangun secara end-to-end menggunakan arsitektur **Medallion (Bronze → Silver → Gold)** di atas ekosistem terdistribusi Docker yang mencakup 6 pilar utama Big Data:

1. ✅ **Pilar 1 (Pengantar Big Data):** Memahami konsep 5V dan kebutuhan *distributed computing*.
2. ✅ **Pilar 2 (Hadoop):** HDFS sebagai *distributed storage* + YARN sebagai *resource manager*.
3. ✅ **Pilar 3 (Spark):** PySpark sebagai engine pemrosesan terdistribusi untuk ETL dan kalkulasi indeks.
4. ✅ **Pilar 4 (Spark MLlib):** LDA untuk *topic modeling* pada corpus berita berbahasa Indonesia.
5. ✅ **Pilar 5 (Kafka):** *Event streaming* real-time dari API RSS ke pipeline analitik.
6. ✅ **Pilar 6 (Data Lakehouse):** Delta Lake sebagai format penyimpanan ACID-compliant di atas HDFS.

**Total data yang berhasil diproses:**
- ~30.891 record faskes
- ~67.853 record penyakit
- 31 kecamatan data kriminal baseline
- 152 berita streaming real-time
- **2 tabel indeks final** siap konsumsi dashboard

***— Catatan Terakhir oleh Tim Anti Gravity, ITS Surabaya —***
