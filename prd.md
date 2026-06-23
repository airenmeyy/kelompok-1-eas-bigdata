# PRODUCT REQUIREMENTS DOCUMENT (PRD)

**Project Name:** KECAMATRAS (Kecamatan Metrics & Anomaly Tracking of Surabaya)  
**Team:** Anti Gravity (M. Hikari Reiziq Rakhmadinta, et al.)  
**Institution:** Institut Teknologi Sepuluh Nopember (ITS) Surabaya  
**Platform:** Web-GIS Dashboard & Time-Series Analytics  
**Document Status:** Final (MVP Version 1.0) — *Implementasi Terverifikasi*  

---

## 1. PROJECT OVERVIEW
### 1.1 Executive Summary
KECAMATRAS adalah platform *Data Lakehouse* yang dirancang untuk memetakan tingkat kerawanan wilayah tingkat kecamatan di Kota Surabaya. Mengingat asimetri informasi yang dialami warga pendatang maupun pengusaha lokal, sistem ini mengintegrasikan data historis statis (kependudukan, rekam medis) dengan data dinamis *real-time* (ekstraksi teks berita) guna menghasilkan dua indeks bahaya berskala 0-100 secara objektif.

### 1.2 Problem Statement
1. **Keterbatasan Pandangan Historis (Human Blindspot):** Masyarakat awam tidak mampu melihat rekam jejak ancaman laten (seperti kawasan rawan begal atau sarang wabah penyakit) hanya melalui survei lokasi fisik yang singkat.
2. **Hambatan Akses Informasi (Data Fragmentation):** Data kejahatan harian dan anomali lingkungan berserakan di portal berita daring dalam bentuk *unstructured data*, sehingga mustahil dievaluasi secara kuantitatif oleh warga biasa.
3. **Siklus Kerugian Berulang (Macro-Pattern Blindness):** Tanpa adanya indikator prediktif atau tren peringatan dari data historis, warga dan pemerintah lokal sering kali terlambat bertindak dan terjebak pada siklus kerugian yang sama.

---

## 2. TARGET AUDIENCE (USER PERSONAS)
1. **Mahasiswa / Pendatang Baru:** Membutuhkan informasi wilayah yang aman (bebas curanmor) dan sehat sebelum memutuskan untuk menyewa kos atau tempat tinggal jangka panjang.
2. **Pemilik Usaha (UMKM/Investor):** Memerlukan validasi risiko lingkungan (keamanan aset & kesehatan kawasan) sebelum membangun ruko atau pabrik.
3. **Pemangku Kebijakan Lokal (Lurah/Camat):** Membutuhkan *dashboard* pemantauan berbasis data (*data-driven policy*) untuk melakukan mitigasi pencegahan sebelum metrik kerawanan mencapai batas kritis.

---

## 3. SCOPE & FEATURES
### 3.1 In-Scope (Minimum Viable Product - V1.0)
Sistem V1.0 difokuskan HANYA pada perhitungan dan visualisasi 2 indeks komposit:
1. **Indeks Kerawanan Kriminalitas Fisik (Street Crime Index)**
2. **Indeks Risiko Kesehatan Lingkungan (Environmental Health Risk Index)**

### 3.2 Core Features
* **Interactive Heatmap (Web-GIS):** Peta spasial 31 kecamatan di Surabaya menggunakan Leaflet.js dengan pewarnaan dinamis (Hijau = Aman $\rightarrow$ Merah = Bahaya).
* **Time-Series Analytics:** Visualisasi grafik tren kerawanan (bukan rata-rata tunggal) dengan titik awal komputasi (*baseline*) mulai dari Januari 2025 hingga waktu terbaru.
* **Historical Data Tab:** Menu retrospektif untuk melihat rekam jejak data statis historis (seperti riwayat penyakit) pada rentang waktu 2022-2024.

### 3.3 Out of Scope (Future Works)
* *LLM Chatbot / RAG AI Agent* (Ditunda ke Fase V2.0).
* *Indeks Bencana Ekologis* & *Indeks Integritas Birokrasi*.
* *Real-time User Report Submission* (Pelaporan insiden langsung oleh warga).

---

## 4. FORMULAS & METRICS DEFINITION
### 4.1 Indeks Kerawanan Kriminalitas Fisik (Street Crime Index)
* **Fokus:** Kejahatan fisik (Curanmor, Begal, Gangster).
* **Metrik Dasar (Crime Rate - CR):**
  $$CR = \frac{Total~Kasus~Kriminalitas~(Baseline + Ekstraksi~Berita)}{Total~Penduduk~Kecamatan} \times 100.000$$
* **Formula Indeks (Min-Max Normalization):**
  $$Indeks~Kriminalitas = \frac{CR_{kec} - CR_{min}}{CR_{max} - CR_{min}} \times 100$$

### 4.2 Indeks Risiko Kesehatan Lingkungan (Environmental Health Risk Index)
* **Fokus:** Menggabungkan "Ancaman Wabah Penyakit" dan "Kapasitas Fasilitas Medis".
* **Metrik Dasar 1: Incidence Rate (IR) - Ancaman:**
  $$IR = \frac{Total~Kasus~(Seluruh~Penyakit)}{Total~Penduduk~Kecamatan} \times 100.000$$
* **Metrik Dasar 2: Health Facility Ratio (HFR) - Kapasitas:**
  $$HFR = \frac{Total~Faskes~(RS + Klinik + Puskesmas)}{Total~Penduduk~Kecamatan} \times 10.000$$
* **Formula Indeks (Weighted Sum Model):**
  *(Lakukan normalisasi Min-Max skala 0-100 untuk nilai IR dan HFR sebelum digabung).*
  $$Indeks~Kesehatan = (0,7 \times Norm(IR)) + (0,3 \times (100 - Norm(HFR)))$$

---

## 5. DATA STRATEGY & TEMPORAL ALIGNMENT
Mengatasi ketidakselarasan dimensi waktu (*Temporal Misalignment*) antar-dataset.

### 5.1 Katalog Sumber Data
| No | Dataset | Tipe | Sumber | Format | Jumlah Record |
|----|---------|------|--------|--------|---------------|
| 1 | **Kapasitas Medis / HFR** | Statis (2023-2026) | Portal Satu Data Indonesia | `new-faskes_kecamatan_2023_2026.csv` (`;` separated) | ~30.891 baris |
| 2 | **Ancaman Penyakit / IR** | Statis (2022-2026) | Portal Satu Data Indonesia | `new-penyakit_puskesmas_2022_2026-1.csv` (`;` separated) | ~67.853 baris |
| 3 | **Kasus Kriminal Baseline** | Statis (Hardcode) | Paper Jurnal: *"Penerapan Statistika Deskriptif dalam Memetakan Titik Rawan Kriminal Berdasarkan Kecamatan pada Kota Surabaya"* | Hardcode Python Dict di `01_bronze.py` | 31 kecamatan |
| 4 | **Berita Real-time** | Dinamis (Streaming) | Google News RSS API | JSON via Kafka topic `kecamatras-stream` | ~152+ berita |

### 5.2 Detail Data Kriminal Baseline (Sumber Paper)
Data kasus kriminal dan jumlah penduduk per kecamatan diambil dari paper penelitian dan di-*hardcode* sebagai *baseline* statis di dalam skrip `01_bronze.py`:

| Kecamatan | Kasus | Penduduk | Crime Rate (/100.000) |
|-----------|-------|----------|----------------------|
| Tandes | 175 | 91.784 | 190,8 |
| Kenjeran* | 104 | 58.216 | 178,6 (2016) |
| Asemrowo | 84 | 48.841 | 172,0 |
| Semampir* | 78 | 47.839 | 163,1 (2021) |
| Jambangan | 89 | 54.212 | 164,2 |
| Tenggilis Mejoyo | 89 | 58.932 | 151,0 |
| ... | ... | ... | ... |

> *Catatan: Kecamatan bertanda (*) menggunakan data tahun terlama yang tersedia karena data terbaru tidak ditemukan dalam paper.*

### 5.3 Strategi Penyelarasan Waktu
1. **Baseline Epoch (2025):** Grafik tren di *dashboard* utama dikunci mulai **Januari 2025**. Penggabungan antar-indeks di bawah tahun tersebut dihindari untuk mencegah grafik anjlok akibat data kosong (*null*).
2. **Forward-Fill Method:** Infrastruktur kesehatan bersifat absolut dan berumur panjang. Kekosongan pembaruan data HFR pasca-2023 diatasi dengan menduplikasi nilai (*forward-fill*) ke tahun 2025-2026.
3. **Time-Series Interpolation:** Metrik tidak disajikan sebagai rata-rata tahunan buta, melainkan pergerakan dinamis bulanan.
4. **Data Normalizing Factor:** Ekstraksi frekuensi berita dari API RSS dikalibrasi ulang sebagai faktor pengali (*multiplier*), bukan sebagai kasus mutlak, agar tidak timpang dengan data absolut dari rekam medis (CSV).

---

## 6. SYSTEM ARCHITECTURE & TECH STACK
Menggunakan arsitektur hibrida (Event Streaming + Batch Processing) dengan prinsip **Medallion Architecture**.

### 6.1 Technology Stack (Implementasi Aktual)
| Komponen | Teknologi | Versi | Docker Image |
|----------|-----------|-------|-------------|
| **Distributed Storage** | Apache Hadoop HDFS | 3.x | `apache/hadoop:3` |
| **Resource Management** | Apache YARN | 3.x | `apache/hadoop:3` |
| **Event Streaming** | Apache Kafka + Zookeeper | 7.5.0 | `confluentinc/cp-kafka:7.5.0` |
| **Distributed Processing** | Apache Spark | 3.3.0 / 3.5.1 | `bde2020/spark-master:3.3.0-hadoop3.3` |
| **ML Engine** | Spark MLlib (LDA) | 3.5.1 | via `python:3.12-slim` |
| **Data Format** | Delta Lake | 3.0.0 | via Maven Package |
| **Ingestion** | Python (feedparser, kafka-python-ng) | 3.12 | Native / Docker |
| **Backend** | Node.js / Express.js | — | *(Planned V2)* |
| **Frontend** | Next.js, Tailwind CSS, Leaflet.js | — | *(Planned V2)* |

### 6.2 Infrastruktur Docker (8 Services)
```
docker-compose.yml
├── namenode          (HDFS NameNode — port 9870, 8020)
├── datanode          (HDFS DataNode)
├── resourcemanager   (YARN RM — port 8088)
├── nodemanager       (YARN NM — port 8042)
├── zookeeper         (Kafka Coordination — port 2181)
├── kafka             (Event Broker — port 9092)
├── spark-master      (Spark Master — port 8080, 7077)
└── spark-worker      (Spark Worker — port 8081)
```
Seluruh service terhubung melalui **Docker Bridge Network** `kecamatras-net`.

### 6.3 Data Pipeline (Medallion Layout — Implementasi Aktual)

#### [BRONZE] Raw Tables → HDFS `hdfs://namenode:8020/kecamatras/delta/bronze/`
| Tabel | Sumber | Mode |
|-------|--------|------|
| `tbl_raw_faskes_baseline` | CSV faskes | Batch |
| `tbl_raw_disease_baseline` | CSV penyakit | Batch |
| `tbl_static_crime_baseline` | Hardcode paper | Batch |
| `tbl_raw_news` | Kafka stream | Streaming |

#### [SILVER] Cleaned Tables → HDFS `hdfs://namenode:8020/kecamatras/delta/silver/`
| Tabel | Proses | Mode |
|-------|--------|------|
| `tbl_clean_faskes` | Standardisasi tipe data, agregasi per kecamatan | Batch |
| `tbl_clean_disease` | Agregasi jumlah kasus per kecamatan | Batch |
| `tbl_clean_crime_baseline` | Normalisasi nama kecamatan | Batch |
| `tbl_clean_news` | Text Preprocessing, Geo-Parsing (Regex 31 kecamatan), Kategorisasi | Streaming |

#### [GOLD] Business Index Tables → HDFS `hdfs://namenode:8020/kecamatras/delta/gold/`
| Tabel | Proses |
|-------|--------|
| `tbl_index_kriminalitas` | LDA Topic Classification → Dynamic Labelling → Analytical Joins → Min-Max Normalization (0-100) |
| `tbl_index_kesehatan` | Weighted Sum Model (0.7×IR + 0.3×(100-HFR)) → Min-Max Normalization (0-100) |

### 6.4 Machine Learning: Dynamic Topic Mapping (Anti-Topic Flipping)
Untuk menghindari *Topic Flipping* pada LDA (sifat *Unsupervised*), digunakan pendekatan **Dynamic Keyword Mapping**:
1. Model LDA (`k=2, seed=42`) dilatih pada corpus berita dari Silver Layer.
2. `describeTopics()` mengekstrak *Top Words* dari setiap klaster.
3. Dua kamus referensi (`kriminal_keywords` dan `sehat_keywords`) berisi ratusan kata kunci bahasa Indonesia.
4. Setiap klaster diberi label berdasarkan skor *intersection* tertinggi dengan kamus referensi.
5. `StopWordsRemover` bahasa Indonesia (ratusan kata) diinjeksi secara manual sebagai array.

---

## 7. NON-FUNCTIONAL REQUIREMENTS (NFR)
1. **Reliability:** Format Delta Lake diimplementasikan pada seluruh layer (Bronze, Silver, Gold) guna mencegah korupsi data apabila terjadi kegagalan sistem saat sinkronisasi API RSS. *(Terverifikasi)*
2. **Containerization:** Seluruh lingkungan sistem (Zookeeper, Kafka, Spark Master/Worker, HDFS NameNode/DataNode, YARN) dijalankan di atas **Docker Compose** dengan 8 service container. *(Terverifikasi)*
3. **Scalability:** Penggunaan Kafka sebagai *broker* penyangga memastikan server tidak *overload* meski menyedot ribuan berita sekaligus dalam satu *batch*. *(Terverifikasi)*
4. **HDFS Compliance:** Seluruh penyimpanan permanen menggunakan **HDFS** (bukan local filesystem), sesuai *best practice* Big Data dan materi Pilar 2 Hadoop. *(Terverifikasi)*

---

## 8. HASIL VERIFIKASI PIPELINE (GOLD LAYER OUTPUT)

### 8.1 Indeks Kriminalitas (Top 5 Kecamatan Paling Rawan)
| Kecamatan | Crime Rate (/100.000) | Indeks (0-100) |
|-----------|----------------------|----------------|
| Tandes | 192,84 | **100,0** |
| Kenjeran | 178,65 | 92,6 |
| Asemrowo | 171,99 | 89,1 |
| Semampir | 171,41 | 88,8 |
| Jambangan | 164,17 | 85,1 |

### 8.2 Indeks Kesehatan Lingkungan (Top 5 Kecamatan Paling Berisiko)
| Kecamatan | Incidence Rate | HFR | Indeks (0-100) |
|-----------|---------------|-----|----------------|
| Semampir | 1.072.930 | 167,4 | **79,7** |
| Kenjeran | 934.375 | 110,1 | 77,6 |
| Krembangan | 308.303 | 58,1 | 43,1 |
| Bubutan | 262.367 | 54,4 | 40,5 |
| Sawahan | 261.215 | 60,4 | 39,7 |

***END OF PRD***