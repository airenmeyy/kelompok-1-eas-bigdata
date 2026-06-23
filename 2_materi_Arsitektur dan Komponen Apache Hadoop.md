# Pertemuan 4: Arsitektur dan Komponen Apache Hadoop

|                           |                                                                   |
| ------------------------- | ----------------------------------------------------------------- |
| **Mata Kuliah**     | Big Data dan Data Lakehouse                                       |
| **Pertemuan**       | 4 (Minggu 4)                                                      |
| **Durasi**          | ± 180 menit (belajar mandiri)                                    |
| **CPMK**            | CPMK-2                                                            |
| **Kemampuan Akhir** | Mahasiswa mampu menjelaskan arsitektur dan komponen Apache Hadoop |
| **Metode**          | 🖥️ Asinkron — Belajar Mandiri, Praktikum Mandiri, Quiz Online  |

---

> 🖥️ **PERTEMUAN ASINKRON**
>
> Pertemuan ini dilakukan secara **asinkron** (belajar mandiri, tidak ada tatap muka). Mahasiswa mempelajari materi teori, mengerjakan hands-on lab, dan mengumpulkan tugas **secara mandiri** sesuai panduan di bawah ini.

---

## 📋 Panduan Belajar Mandiri

Ikuti langkah-langkah berikut secara berurutan. Centang setiap langkah setelah selesai.

| No | Langkah                                            | Estimasi | Keterangan                                             |
| -- | -------------------------------------------------- | -------- | ------------------------------------------------------ |
| 1  | 📖 Baca**Bagian 1**: Pengantar Apache Hadoop | 15 menit | Sejarah, pilar Hadoop, relevansi                       |
| 2  | 📖 Baca**Bagian 2**: HDFS                    | 25 menit | Arsitektur, read/write flow, kelebihan/keterbatasan    |
| 3  | 📖 Baca**Bagian 3**: YARN                    | 20 menit | ResourceManager, NodeManager, Scheduler                |
| 4  | 📖 Baca**Bagian 4**: MapReduce In-Depth      | 15 menit | Flow MapReduce, pseudo-code, perbandingan dengan Spark |
| 5  | 🧠 Kerjakan**Quiz Online** (20 soal)         | 15 menit | Akses link quiz di LMS                                 |
| 6  | 🔧 Kerjakan**Lab 1**: HDFS CLI Operations    | 25 menit | Setup Docker + praktik perintah HDFS                   |
| 7  | 🔧 Kerjakan**Lab 2**: Explore Hadoop Web UI  | 20 menit | Navigasi NameNode & YARN Web UI                        |
| 8  | 🔧 Kerjakan**Lab 3**: MapReduce WordCount    | 30 menit | Jalankan MapReduce job end-to-end                      |
| 9  | 📝 Kerjakan**Tugas** dan submit via LMS      | 15 menit | Lab Report + Refleksi                                  |

> ⏱️ **Total estimasi waktu:** ± 180 menit (3 jam)
> 💡 Kamu bisa mencicil di beberapa sesi — tidak harus selesai sekaligus!

---

# 📖 MATERI TEORI

> ⏱️ **Estimasi waktu baca:** 45–60 menit
> 📌 **Pelajari materi ini sebelum mengerjakan quiz dan lab.**

---

## Bagian 1: Pengantar Apache Hadoop (15 menit)

### 1.1 Apa Itu Apache Hadoop?

**Apache Hadoop** adalah framework open-source untuk **penyimpanan dan pemrosesan data terdistribusi** pada cluster komputer menggunakan model pemrograman sederhana.

> 🏗️ **Analogi: Membangun Gedung Pencakar Langit**
>
> Bayangkan kamu harus membangun **gedung 100 lantai**:
>
> - Satu kru kecil? Butuh **puluhan tahun**
> - 1.000 kru yang bekerja paralel di setiap lantai? Selesai dalam **hitungan bulan**
>
> Hadoop bekerja seperti mandor yang **membagi pekerjaan** ke ribuan komputer biasa (commodity hardware) dan **mengkoordinasi** hasilnya.

### 1.2 Sejarah Singkat Hadoop

```
┌─────────────────────────────────────────────────────────────────┐
│                    TIMELINE APACHE HADOOP                         │
│                                                                 │
│  2003  ──── Google publikasi paper GFS (Google File System)     │
│              └─ Inspirasi untuk HDFS                            │
│                                                                 │
│  2004  ──── Google publikasi paper MapReduce                     │
│              └─ Model pemrograman untuk pemrosesan paralel      │
│                                                                 │
│  2005  ──── Doug Cutting & Mike Cafarella di Yahoo!              │
│              └─ Mulai mengembangkan Hadoop                      │
│              └─ Dinamakan dari boneka gajah anaknya 🐘          │
│                                                                 │
│  2006  ──── Hadoop menjadi proyek Apache                         │
│              └─ Yahoo! menggunakan cluster 10.000 node          │
│                                                                 │
│  2008  ──── Hadoop memproses 1 TB data dalam 209 detik           │
│              └─ Memecahkan rekor sorting dunia!                  │
│                                                                 │
│  2011  ──── Hadoop 1.0 dirilis                                   │
│                                                                 │
│  2013  ──── Hadoop 2.0 + YARN                                    │
│              └─ Tidak lagi terbatas pada MapReduce              │
│                                                                 │
│  2017  ──── Hadoop 3.0                                           │
│              └─ Erasure Coding, Timeline Service v2              │
│                                                                 │
│  Hari ini ── Hadoop tetap jadi fondasi banyak data platform     │
│              └─ HDFS masih digunakan oleh Spark, Hive, dll.     │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Empat Pilar Hadoop

```
┌─────────────────────────────────────────────────────────────────┐
│                   EKOSISTEM APACHE HADOOP                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  APLIKASI & TOOLS                        │    │
│  │  Hive (SQL) │ Pig (Scripting) │ HBase (NoSQL) │ Mahout  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                           │                                      │
│  ┌────────────────────────┴────────────────────────────────┐    │
│  │               MAPREDUCE / TEZ / SPARK                    │    │
│  │              (Processing / Computation)                   │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                           │                                      │
│  ┌────────────────────────┴────────────────────────────────┐    │
│  │                      YARN                                │    │
│  │              (Resource Management)                        │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                           │                                      │
│  ┌────────────────────────┴────────────────────────────────┐    │
│  │                      HDFS                                │    │
│  │              (Distributed Storage)                        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  🐘 Hadoop = HDFS + YARN + MapReduce + Common Utilities         │
└─────────────────────────────────────────────────────────────────┘
```

| Komponen            | Fungsi                                     | Analogi                           |
| ------------------- | ------------------------------------------ | --------------------------------- |
| **HDFS**      | Penyimpanan data terdistribusi             | Gudang raksasa yang tersebar      |
| **YARN**      | Manajemen resource dan penjadwalan job     | Mandor yang membagi tugas         |
| **MapReduce** | Model pemrograman untuk pemrosesan paralel | Pekerja yang mengerjakan tugasnya |
| **Common**    | Utilitas dan library pendukung             | Alat-alat yang dipakai semua      |

### 1.4 Kapan Hadoop Masih Relevan?

| Skenario                             | Hadoop? | Alternatif Modern              |
| ------------------------------------ | ------- | ------------------------------ |
| Storage petabyte dengan biaya rendah | ✅      | Cloud Object Storage (S3, GCS) |
| ETL batch skala besar                | ✅      | Spark standalone, Databricks   |
| On-premise data platform             | ✅      | —                             |
| Real-time streaming                  | ❌      | Kafka + Flink                  |
| Interactive SQL queries              | ⚠️    | Trino, Presto, BigQuery        |
| Machine Learning iteratif            | ⚠️    | Spark MLlib, cloud ML services |

> 💡 **Penting:** Meskipun ada banyak alternatif modern, **HDFS masih menjadi fondasi** banyak data platform. Bahkan Spark, Hive, dan Presto sering berjalan **di atas HDFS**. Memahami Hadoop = memahami fondasi Big Data.

---

## Bagian 2: HDFS — Hadoop Distributed File System (25 menit)

### 2.1 Apa Itu HDFS?

**HDFS** adalah sistem file terdistribusi yang dirancang untuk menyimpan **file berukuran sangat besar** (GB–PB) di cluster komputer biasa dengan **fault tolerance** tinggi.

> 📚 **Analogi: Perpustakaan Raksasa**
>
> Bayangkan sebuah **perpustakaan dengan 1 juta buku**:
>
> - Satu gedung tidak muat → buku disebar ke **100 cabang** di seluruh kota
> - Setiap buku punya **3 salinan** di cabang berbeda (kalau satu kebakaran, masih ada 2)
> - Ada satu **katalog pusat** yang tahu buku apa ada di cabang mana
>
> HDFS bekerja persis seperti ini:
>
> - File dipecah jadi **block** dan disebar ke banyak node
> - Setiap block punya **3 replika** (default)
> - **NameNode** = katalog pusat yang tahu lokasi semua block

### 2.2 Arsitektur HDFS

```
┌─────────────────────────────────────────────────────────────────┐
│                     ARSITEKTUR HDFS                               │
│                                                                 │
│                 ┌──────────────────┐                             │
│                 │    NAMENODE      │  ← Master node              │
│                 │   (Metadata)     │                             │
│                 │                  │                             │
│                 │ • Namespace      │  Menyimpan informasi:       │
│                 │ • Block mapping  │  - Nama file               │
│                 │ • Replication    │  - Lokasi block             │
│                 │                  │  - Permission               │
│                 └────────┬─────────┘                             │
│                          │                                       │
│            ┌─────────────┼─────────────┐                         │
│            │             │             │                         │
│            ▼             ▼             ▼                         │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │
│  │  DATANODE 1  │ │  DATANODE 2  │ │  DATANODE 3  │             │
│  │              │ │              │ │              │             │
│  │ ┌──┐┌──┐┌──┐│ │ ┌──┐┌──┐┌──┐│ │ ┌──┐┌──┐┌──┐│             │
│  │ │B1││B3││B5││ │ │B1││B2││B4││ │ │B2││B3││B5││             │
│  │ └──┘└──┘└──┘│ │ └──┘└──┘└──┘│ │ └──┘└──┘└──┘│             │
│  │              │ │              │ │              │             │
│  │ ┌──┐┌──┐    │ │ ┌──┐┌──┐    │ │ ┌──┐┌──┐    │             │
│  │ │B6││B4│    │ │ │B6││B5│    │ │ │B1││B6│    │             │
│  │ └──┘└──┘    │ │ └──┘└──┘    │ │ └──┘└──┘    │             │
│  └──────────────┘ └──────────────┘ └──────────────┘             │
│                                                                 │
│  B1–B6 = Data blocks (default 128 MB per block)                 │
│  Setiap block direplikasi 3× di DataNode berbeda                │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Komponen Utama HDFS

| Komponen                     | Peran                                                              |
| ---------------------------- | ------------------------------------------------------------------ |
| **NameNode**           | Master — menyimpan metadata (namespace, lokasi block, izin akses) |
| **DataNode**           | Worker — menyimpan data block aktual di disk lokal                |
| **Secondary NameNode** | Melakukan checkpoint metadata secara periodik                      |
| **Block**              | Unit terkecil penyimpanan (default 128 MB di Hadoop 2+)            |
| **Replication Factor** | Jumlah salinan per block (default 3)                               |

> ⚠️ **Miskonsepsi umum:** Secondary NameNode **BUKAN** standby/backup NameNode! Fungsinya hanya melakukan merge edit log dengan fsimage. Untuk High Availability, Hadoop 2+ menggunakan **Standby NameNode** terpisah.

### 2.4 Bagaimana File Disimpan di HDFS

```
┌─────────────────────────────────────────────────────────────────┐
│              PROSES PENYIMPANAN FILE DI HDFS                      │
│                                                                 │
│  File: log_transaksi.csv (512 MB)                                │
│                                                                 │
│  STEP 1: File dipecah menjadi block (128 MB per block)           │
│                                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Block 1  │ │ Block 2  │ │ Block 3  │ │ Block 4  │           │
│  │ 128 MB   │ │ 128 MB   │ │ 128 MB   │ │ 128 MB   │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
│                                                                 │
│  STEP 2: Setiap block direplikasi 3× ke DataNode berbeda        │
│                                                                 │
│  Block 1 → DataNode A, DataNode C, DataNode E                   │
│  Block 2 → DataNode B, DataNode D, DataNode A                   │
│  Block 3 → DataNode C, DataNode E, DataNode B                   │
│  Block 4 → DataNode D, DataNode A, DataNode C                   │
│                                                                 │
│  STEP 3: NameNode mencatat mapping block → DataNode              │
│                                                                 │
│  📝 Metadata: "log_transaksi.csv"                                │
│     Block 1 → [A, C, E]                                         │
│     Block 2 → [B, D, A]                                         │
│     Block 3 → [C, E, B]                                         │
│     Block 4 → [D, A, C]                                         │
└─────────────────────────────────────────────────────────────────┘
```

### 2.5 Write Flow — Bagaimana Data Ditulis ke HDFS

```
┌─────────────────────────────────────────────────────────────────┐
│                    HDFS WRITE FLOW                                │
│                                                                 │
│  Client                    NameNode                              │
│    │                          │                                  │
│    │─── 1. "Mau tulis file"──▶│                                  │
│    │                          │                                  │
│    │◀── 2. "Tulis ke DN       │                                  │
│    │       A, B, C" ─────────│                                  │
│    │                                                             │
│    │     DataNode A    DataNode B    DataNode C                   │
│    │         │              │             │                       │
│    │── 3. ──▶│              │             │  Pipeline             │
│    │  Data   │── 4. ──────▶│             │  Replication          │
│    │         │              │── 5. ─────▶│                       │
│    │         │              │             │                       │
│    │         │◀── 6. ACK ──│◀── ACK ────│                       │
│    │◀────────│              │             │                       │
│    │                                                             │
│    │── 7. "Tulis selesai" ──▶ NameNode                           │
│    │                          (update metadata)                   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.6 Read Flow — Bagaimana Data Dibaca dari HDFS

```
┌─────────────────────────────────────────────────────────────────┐
│                     HDFS READ FLOW                                │
│                                                                 │
│  Client                    NameNode                              │
│    │                          │                                  │
│    │─── 1. "Mau baca file"──▶│                                  │
│    │                          │                                  │
│    │◀── 2. "Block 1 di DN A, │                                  │
│    │       Block 2 di DN B"──│                                  │
│    │                                                             │
│    │     DataNode A    DataNode B                                 │
│    │         │              │                                     │
│    │── 3. ──▶│              │   Baca block terdekat              │
│    │◀─ data ─│              │   (data locality!)                 │
│    │                        │                                     │
│    │── 4. ─────────────────▶│                                     │
│    │◀─────── data ──────────│                                     │
│    │                                                             │
│    │   Client menggabungkan semua block → file utuh               │
└─────────────────────────────────────────────────────────────────┘
```

> 💡 **Data Locality:** HDFS berusaha membaca data dari DataNode yang **paling dekat** dengan client (idealnya di node yang sama). Ini mengurangi network traffic dan meningkatkan performa secara signifikan.

### 2.7 Kelebihan dan Keterbatasan HDFS

| Kelebihan                              | Keterbatasan                                    |
| -------------------------------------- | ----------------------------------------------- |
| ✅ Sangat scalable (petabyte+)         | ❌ Latensi tinggi (tidak untuk real-time)       |
| ✅ Fault-tolerant (replication)        | ❌ Tidak efisien untuk file kecil (< 128 MB)    |
| ✅ Cost-effective (commodity hardware) | ❌ Write-once, read-many (tidak support update) |
| ✅ High throughput untuk data besar    | ❌ NameNode = Single Point of Failure (pre-HA)  |
| ✅ Data locality untuk performa        | ❌ Tidak cocok untuk banyak file kecil          |

---

## Bagian 3: YARN — Yet Another Resource Negotiator (20 menit)

### 3.1 Mengapa YARN Dibuat?

```
┌─────────────────────────────────────────────────────────────────┐
│              HADOOP 1.x vs HADOOP 2.x (YARN)                    │
│                                                                 │
│  HADOOP 1.x:                    HADOOP 2.x (YARN):              │
│  ┌────────────────────┐         ┌────────────────────┐          │
│  │    MapReduce        │         │   MapReduce │ Spark│          │
│  │  (Processing +      │         │   Tez │ Flink │ ...│          │
│  │   Resource Mgmt)    │         └────────┬───────────┘          │
│  └─────────┬──────────┘                  │                      │
│            │                    ┌────────┴───────────┐          │
│  ┌─────────┴──────────┐         │       YARN         │          │
│  │       HDFS          │         │  (Resource Mgmt)   │          │
│  └────────────────────┘         └────────┬───────────┘          │
│                                 ┌────────┴───────────┐          │
│  ❌ Hanya MapReduce              │       HDFS         │          │
│  ❌ Tidak fleksibel              └────────────────────┘          │
│  ❌ Bottleneck di JobTracker                                     │
│                                 ✅ Multi-engine support          │
│                                 ✅ Lebih scalable                │
│                                 ✅ Lebih fleksibel               │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Arsitektur YARN

```
┌─────────────────────────────────────────────────────────────────┐
│                     ARSITEKTUR YARN                               │
│                                                                 │
│  ┌─────────────────────────────────────────────────┐            │
│  │              RESOURCE MANAGER (RM)               │            │
│  │  ┌───────────────┐  ┌───────────────────────┐   │            │
│  │  │   Scheduler   │  │ Application Manager   │   │            │
│  │  └───────────────┘  └───────────────────────┘   │            │
│  └────────────────────────┬────────────────────────┘            │
│                           │                                      │
│        ┌──────────────────┼──────────────────┐                   │
│        ▼                  ▼                  ▼                   │
│  ┌───────────┐     ┌───────────┐     ┌───────────┐              │
│  │NODE MGR 1 │     │NODE MGR 2 │     │NODE MGR 3 │              │
│  │┌─────────┐│     │┌─────────┐│     │┌─────────┐│              │
│  ││Container││     ││Container││     ││Container││              │
│  ││(AppMstr)││     ││ (Task)  ││     ││ (Task)  ││              │
│  │└─────────┘│     │└─────────┘│     │└─────────┘│              │
│  │┌─────────┐│     │┌─────────┐│     │┌─────────┐│              │
│  ││Container││     ││Container││     ││Container││              │
│  ││ (Task)  ││     ││ (Task)  ││     ││ (Task)  ││              │
│  │└─────────┘│     │└─────────┘│     │└─────────┘│              │
│  └───────────┘     └───────────┘     └───────────┘              │
│                                                                 │
│  Container = Unit alokasi resource (CPU + RAM)                   │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 Komponen Utama YARN

| Komponen                       | Peran                                                            |
| ------------------------------ | ---------------------------------------------------------------- |
| **ResourceManager (RM)** | Master — mengelola seluruh resource di cluster                  |
| **Scheduler**            | Bagian dari RM — mengalokasikan resource (FIFO, Fair, Capacity) |
| **NodeManager (NM)**     | Worker — mengelola resource di satu node, menjalankan container |
| **ApplicationMaster**    | Per-job coordinator — negosiasi resource untuk satu aplikasi    |
| **Container**            | Unit alokasi resource (CPU cores + RAM)                          |

### 3.4 Scheduler di YARN

| Scheduler                    | Karakteristik                                            | Cocok Untuk                |
| ---------------------------- | -------------------------------------------------------- | -------------------------- |
| **FIFO Scheduler**     | First-in, first-out — sederhana tapi tidak adil         | Development / testing      |
| **Capacity Scheduler** | Membagi cluster jadi queue dengan kapasitas tetap        | Multi-tenant organizations |
| **Fair Scheduler**     | Membagi resource secara merata ke semua job yang running | Keadilan antar pengguna    |

---

## Bagian 4: MapReduce In-Depth (15 menit)

### 4.1 Detail Flow MapReduce

```
┌─────────────────────────────────────────────────────────────────┐
│                  DETAIL FLOW MAPREDUCE                            │
│                                                                 │
│  INPUT (file di HDFS)                                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ "big data is big data processing big data analytics"    │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                     SPLIT (per block)                             │
│            ┌───────────────┼───────────────┐                     │
│            ▼               ▼               ▼                     │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │
│  │ "big data    │ │ "big data    │ │ "big data    │             │
│  │  is big"     │ │  processing" │ │  analytics"  │             │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘             │
│         │                │                │                      │
│         ▼ MAP            ▼ MAP            ▼ MAP                  │
│  (big,1)          (big,1)          (big,1)                       │
│  (data,1)         (data,1)         (data,1)                      │
│  (is,1)           (processing,1)   (analytics,1)                 │
│  (big,1)                                                         │
│         │                │                │                      │
│         └────────────────┼────────────────┘                      │
│                          │                                       │
│                   SHUFFLE & SORT                                  │
│         ┌────────────────┼────────────────┐                      │
│         ▼                ▼                ▼                      │
│  (analytics,[1])  (big,[1,1,1,1])  (data,[1,1,1])               │
│  (is,[1])         (processing,[1])                               │
│         │                │                │                      │
│         ▼ REDUCE         ▼ REDUCE         ▼ REDUCE              │
│  (analytics,1)    (big,4)          (data,3)                      │
│  (is,1)           (processing,1)                                 │
│                                                                 │
│  OUTPUT: analytics=1, big=4, data=3, is=1, processing=1         │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Pseudo-code: Word Count

```
// === MAPPER ===
function map(key: lineNumber, value: lineText):
    words = value.split(" ")
    for each word in words:
        emit(word, 1)

// === REDUCER ===
function reduce(key: word, values: list[int]):
    total = 0
    for each count in values:
        total = total + count
    emit(word, total)
```

### 4.3 Keterbatasan MapReduce vs. Spark

| Aspek               | MapReduce                       | Apache Spark                    |
| ------------------- | ------------------------------- | ------------------------------- |
| **I/O**       | Disk I/O setiap stage           | In-memory (RAM)                 |
| **Kecepatan** | Lambat (10–100× lebih lambat) | Cepat (in-memory computing)     |
| **API**       | Verbose (Java-centric)          | Concise (Python, Scala, SQL, R) |
| **Streaming** | Tidak didukung                  | Spark Structured Streaming      |

---

# 🧠 QUIZ ONLINE

> ⏱️ **Estimasi waktu:** 15 menit
> 📋 **Kerjakan quiz SETELAH membaca semua materi teori (Bagian 1–4).**

Akses quiz melalui **LMS** (tautan disediakan oleh dosen di halaman pertemuan 4). Quiz terdiri dari 3**0 soal pilihan ganda** yang menguji pemahaman materi teori.

**Ketentuan Quiz:**

- ⏰ Batas waktu: 15 **menit** sejak quiz dibuka
- 🔄 Quiz ini dapat dikerjakan lebih dari 1x.
- 📊 Nilai langsung muncul setelah submit
- 📅 Deadline: sesuai jadwal di LMS

> 💡 **Tips:** Kamu bisa kerjakan menggunakan AI, tapi apa serunya? Quiz ini didesain supaya bisa dikerjakan lebih dari satu kali sebagai tantangan.

---

# 🔧 HANDS-ON LABS (Mandiri)

> ⏱️ **Total estimasi lab:** 75 menit
> **Prasyarat:** Docker Desktop sudah terinstall di laptop.

## 🛠️ Setup Awal: Menjalankan Hadoop via Docker Compose

Kita akan menggunakan **Docker Compose** untuk menjalankan cluster Hadoop multi-container (NameNode, DataNode, ResourceManager, NodeManager). Setup ini sudah diuji dan berjalan di **macOS Apple Silicon (ARM64)** maupun Intel/AMD.

### Langkah 1: Pastikan Docker Berjalan

```bash
# Cek Docker sudah terinstall dan berjalan
docker --version
docker compose version
```

> ⚠️ **Belum punya Docker?** Download di https://www.docker.com/products/docker-desktop/ dan install sesuai OS kamu. Pastikan Docker Desktop mendapat alokasi **minimal 4 GB RAM** (Settings → Resources).

### Langkah 2: Download File Konfigurasi

Download dua file konfigurasi dari repository materi (**cek LMS** untuk link terbaru), atau buat manual:

**File 1: `docker-compose.yml`**

```yaml
services:
  namenode:
    image: apache/hadoop:3
    platform: linux/amd64
    hostname: namenode
    container_name: hadoop-namenode
    ports:
      - "9870:9870"   # NameNode Web UI
      - "8020:8020"   # NameNode RPC
    environment:
      ENSURE_NAMENODE_DIR: /tmp/hadoop-hadoop/dfs/name
    env_file:
      - ./hadoop.env
    command: ["hdfs", "namenode"]

  datanode:
    image: apache/hadoop:3
    platform: linux/amd64
    hostname: datanode
    container_name: hadoop-datanode
    depends_on:
      namenode:
        condition: service_started
    env_file:
      - ./hadoop.env
    command: ["hdfs", "datanode"]

  resourcemanager:
    image: apache/hadoop:3
    platform: linux/amd64
    hostname: resourcemanager
    container_name: hadoop-resourcemanager
    depends_on:
      datanode:
        condition: service_started
    ports:
      - "8088:8088"   # ResourceManager Web UI
    env_file:
      - ./hadoop.env
    command: ["yarn", "resourcemanager"]

  nodemanager:
    image: apache/hadoop:3
    platform: linux/amd64
    hostname: nodemanager
    container_name: hadoop-nodemanager
    depends_on:
      resourcemanager:
        condition: service_started
    ports:
      - "8042:8042"   # NodeManager Web UI
    env_file:
      - ./hadoop.env
    command: ["yarn", "nodemanager"]
```

**File 2: `hadoop.env`** (taruh di folder yang sama dengan `docker-compose.yml`)

```env
CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020
CORE-SITE.XML_fs.default.name=hdfs://namenode:8020

HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=1

MAPRED-SITE.XML_mapreduce.framework.name=yarn
MAPRED-SITE.XML_mapreduce.application.classpath=/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/mapreduce/lib/*

YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager
YARN-SITE.XML_yarn.nodemanager.pmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled=false
YARN-SITE.XML_yarn.nodemanager.aux-services=mapreduce_shuffle
YARN-SITE.XML_yarn.nodemanager.resource.memory-mb=2048
YARN-SITE.XML_yarn.scheduler.minimum-allocation-mb=512
YARN-SITE.XML_yarn.scheduler.maximum-allocation-mb=2048
```

> ℹ️ Replication factor diset **1** karena kita hanya punya 1 DataNode di cluster ini.

### Langkah 3: Jalankan Cluster Hadoop

```bash
# Masuk ke folder tempat docker-compose.yml berada
# Jalankan semua service
docker compose up -d

# Tunggu ~60 detik agar semua service siap
sleep 60

# Cek semua container berjalan (harus ada 4 container)
docker compose ps
```

> ✅ **Expected output:** 4 container dengan status `running`:
> `hadoop-namenode`, `hadoop-datanode`, `hadoop-resourcemanager`, `hadoop-nodemanager`

### Langkah 4: Masuk ke Container NameNode

```bash
# Masuk ke shell container NameNode
docker exec -it hadoop-namenode bash

# Verifikasi HDFS aktif
hdfs dfs -ls /
```

> ✅ Jika muncul output (meskipun kosong), berarti HDFS sudah siap!
> ❌ Jika error `Connection refused`, tunggu 30 detik lagi lalu coba ulang.

### Langkah 5: Verifikasi Web UI

Buka browser dan pastikan kedua halaman ini bisa diakses:

- **NameNode Web UI:** http://localhost:9870
- **YARN ResourceManager:** http://localhost:8088

> 🆘 **Mengalami masalah?** Cari referensi di Stack Overflow / tanya AI. Kalau masih belum menemukan solusi, tanyakan ke **grup WhatsApp kelas**. Sertakan screenshot error yang muncul.

### Cara Mematikan Cluster

Setelah selesai lab, matikan cluster dengan:

```bash
docker compose down
```

> 💡 Data akan hilang saat container dihapus. Ini normal untuk lab.

---

## 🔧 Lab 1: HDFS CLI Operations (25 menit)

### Tujuan

Menguasai perintah-perintah dasar HDFS CLI untuk mengelola file di Hadoop Distributed File System.

### Setup

> ℹ️ Pastikan kamu sudah menyelesaikan **Setup Awal** (Docker Compose) di atas sebelum memulai lab ini.

```bash
# Jika belum di dalam container, masuk terlebih dahulu:
docker exec -it hadoop-namenode bash

# Verifikasi HDFS aktif
hdfs dfs -ls /
```

### Task 1.1: Membuat Direktori di HDFS

```bash
# Buat direktori untuk data kamu
hdfs dfs -mkdir -p /user/mahasiswa/kelompok1

# Buat struktur direktori proyek
hdfs dfs -mkdir -p /user/mahasiswa/kelompok1/input
hdfs dfs -mkdir -p /user/mahasiswa/kelompok1/output

# Verifikasi
hdfs dfs -ls /user/mahasiswa/kelompok1
```

> 💡 Jika `mkdir` gagal dengan "Input/output error", pastikan DataNode sudah berjalan: `docker compose ps` harus menunjukkan `hadoop-datanode` status `running`. Tunggu 30 detik lalu coba lagi.

**📝 Catat:** Apa output dari perintah `-ls`?

### Task 1.2: Upload File ke HDFS

```bash
# Buat file contoh di lokal
echo "big data is big data processing big data analytics" > sample.txt
echo "hadoop hdfs yarn mapreduce spark" >> sample.txt
echo "distributed computing cluster node" >> sample.txt

# Upload ke HDFS
hdfs dfs -put sample.txt /user/mahasiswa/kelompok1/input/

# Verifikasi file ter-upload
hdfs dfs -ls /user/mahasiswa/kelompok1/input/
```

### Task 1.3: Membaca File dari HDFS

```bash
# Tampilkan isi file
hdfs dfs -cat /user/mahasiswa/kelompok1/input/sample.txt

# Tampilkan beberapa baris pertama
hdfs dfs -head /user/mahasiswa/kelompok1/input/sample.txt

# Download file dari HDFS ke lokal
hdfs dfs -get /user/mahasiswa/kelompok1/input/sample.txt downloaded.txt
cat downloaded.txt
```

### Task 1.4: Cek Info File & Storage

```bash
# Cek ukuran file
hdfs dfs -du -h /user/mahasiswa/kelompok1/input/

# Cek status sistem HDFS
hdfs dfsadmin -report

# Cek replication factor file
hdfs dfs -stat "%r" /user/mahasiswa/kelompok1/input/sample.txt
```

**📝 Pertanyaan:**

1. Berapa replication factor file `sample.txt`?
2. Berapa total ruang disk yang digunakan HDFS?
3. Berapa jumlah DataNode yang aktif?

### Task 1.5: Operasi Lanjutan

```bash
# Copy file di dalam HDFS
hdfs dfs -cp /user/mahasiswa/kelompok1/input/sample.txt /user/mahasiswa/kelompok1/input/sample_copy.txt

# Rename/Move file
hdfs dfs -mv /user/mahasiswa/kelompok1/input/sample_copy.txt /user/mahasiswa/kelompok1/input/backup.txt

# Hapus file
hdfs dfs -rm /user/mahasiswa/kelompok1/input/backup.txt

# Cek replication factor (harusnya 1 karena setting cluster kita)
hdfs dfs -stat "%r" /user/mahasiswa/kelompok1/input/sample.txt
```

**📝 Pertanyaan:** Mengapa replication factor di cluster kita adalah 1, bukan 3 seperti default? Apa implikasinya terhadap fault tolerance?

### Cheat Sheet HDFS CLI

| Perintah                         | Fungsi                       |
| -------------------------------- | ---------------------------- |
| `hdfs dfs -ls <path>`          | List isi direktori           |
| `hdfs dfs -mkdir -p <path>`    | Buat direktori (recursive)   |
| `hdfs dfs -put <local> <hdfs>` | Upload file lokal ke HDFS    |
| `hdfs dfs -get <hdfs> <local>` | Download file HDFS ke lokal  |
| `hdfs dfs -cat <path>`         | Tampilkan isi file           |
| `hdfs dfs -rm <path>`          | Hapus file                   |
| `hdfs dfs -du -h <path>`       | Ukuran file/direktori        |
| `hdfs dfs -setrep <n> <path>`  | Ubah replication factor      |
| `hdfs dfs -stat "%r" <path>`   | Tampilkan replication factor |
| `hdfs dfsadmin -report`        | Laporan status cluster       |

---

## 🔧 Lab 2: Explore Hadoop Web UI (20 menit)

### Tujuan

Memahami cara memonitor cluster Hadoop melalui web interface.

### Task 2.1: NameNode Web UI

Buka browser → `http://localhost:9870`

**Eksplorasi:**

1. **Overview** — Catat:
   - Versi Hadoop yang berjalan
   - Jumlah DataNode yang aktif (Live Nodes)
   - Total kapasitas HDFS dan yang sudah terpakai
2. **Datanodes** — Tab "Datanodes":
   - Cek status setiap DataNode
   - Catat kapasitas masing-masing DataNode
3. **Utilities → Browse the file system**:
   - Navigasi ke `/user/mahasiswa/kelompok1/input/`
   - Klik file `sample.txt`
   - Lihat informasi block: block ID, lokasi DataNode, replication

**📝 Screenshot yang harus dikumpulkan:**

1. Halaman Overview NameNode
2. Detail file `sample.txt` (block info)

### Task 2.2: YARN ResourceManager Web UI

Buka browser → `http://localhost:8088`

**Eksplorasi:**

1. **Cluster Metrics** — Catat:
   - Jumlah NodeManager aktif
   - Total memory dan vCores
2. **Scheduler** — Klik tab "Scheduler":
   - Jenis scheduler apa yang digunakan?
   - Berapa queue yang ada?

**📝 Screenshot:** Halaman Cluster Metrics

---

## 🔧 Lab 3: Jalankan MapReduce WordCount (30 menit)

### Tujuan

Menjalankan MapReduce job end-to-end dengan dataset nyata: dari input di HDFS hingga output.

### Task 3.1: Download Dataset dari Internet

Kita akan menggunakan dataset teks nyata — **The Complete Works of William Shakespeare** (~5.5 MB, 124.000+ baris) dari Project Gutenberg.

```bash
# Pastikan sudah di dalam container NameNode
docker exec -it hadoop-namenode bash

# Download karya lengkap Shakespeare (~5.5 MB)
curl -L -o shakespeare.txt https://www.gutenberg.org/cache/epub/100/pg100.txt

# Cek ukuran file
ls -lh shakespeare.txt
wc -l shakespeare.txt

# Lihat beberapa baris isi file
head -20 shakespeare.txt
```

> 📊 **Ukuran dataset:** ~5.5 MB, 124.000+ baris — jauh lebih besar dari file 10 baris sebelumnya! Ini akan memberi gambaran lebih realistis tentang pemrosesan data di Hadoop.

### Task 3.2: Upload Dataset ke HDFS

```bash
# Upload ke HDFS
hdfs dfs -put shakespeare.txt /user/mahasiswa/kelompok1/input/

# Verifikasi
hdfs dfs -ls -h /user/mahasiswa/kelompok1/input/

# Cek berapa block yang terbentuk
hdfs fsck /user/mahasiswa/kelompok1/input/shakespeare.txt -blocks
```

**📝 Pertanyaan:** Berapa block yang terbentuk dari file shakespeare.txt? Mengapa hanya sejumlah itu? (Hint: bandingkan ukuran file dengan block size 128 MB)

### Task 3.3: Jalankan WordCount MapReduce Job

```bash
# Pastikan output directory belum ada
hdfs dfs -rm -r /user/mahasiswa/kelompok1/output/wordcount 2>/dev/null

# Jalankan WordCount (bawaan Hadoop)
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount \
    /user/mahasiswa/kelompok1/input/shakespeare.txt \
    /user/mahasiswa/kelompok1/output/wordcount
```

> ⏱️ Job ini akan memakan waktu **1–3 menit** tergantung spesifikasi laptop.

### Task 3.4: Monitor Job di YARN Web UI

Saat job berjalan, buka `http://localhost:8088`:

1. Klik **Applications** → temukan job WordCount
2. Catat:
   - Application ID
   - Status (RUNNING → FINISHED)
   - Elapsed time
   - Jumlah Map tasks dan Reduce tasks

### Task 3.5: Lihat Hasil Output

```bash
# List output files
hdfs dfs -ls /user/mahasiswa/kelompok1/output/wordcount/

# Tampilkan 30 baris pertama hasil word count
hdfs dfs -cat /user/mahasiswa/kelompok1/output/wordcount/part-r-00000 | head -30

# Cari kata-kata yang paling sering muncul
hdfs dfs -cat /user/mahasiswa/kelompok1/output/wordcount/part-r-00000 | sort -t$'\t' -k2 -nr | head -20

# Download hasil ke lokal
hdfs dfs -get /user/mahasiswa/kelompok1/output/wordcount/part-r-00000 wordcount_result.txt
```

**📝 Pertanyaan:**

1. Sebutkan **5 kata yang paling sering muncul** beserta frekuensinya!
2. Berapa jumlah Map tasks yang dijalankan? Mengapa sejumlah itu?
3. Berapa jumlah Reduce tasks? Mengapa?
4. Sebutkan file output yang dihasilkan di direktori output. Apa fungsi file `_SUCCESS`?
5. Berapa lama waktu yang dibutuhkan job untuk selesai (elapsed time)?

### Task 3.6: Tantangan Bonus — Bandingkan File Kecil vs. Besar

```bash
# Jalankan WordCount pada SEMUA file di folder input (sample.txt + shakespeare.txt)
hdfs dfs -rm -r /user/mahasiswa/kelompok1/output/wordcount_all 2>/dev/null

hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount \
    /user/mahasiswa/kelompok1/input/ \
    /user/mahasiswa/kelompok1/output/wordcount_all

# Bandingkan hasilnya
hdfs dfs -cat /user/mahasiswa/kelompok1/output/wordcount_all/part-r-00000 | sort -t$'\t' -k2 -nr | head -20
```

**📝 Pertanyaan bonus:**

1. Apa perbedaan jumlah Map tasks jika input berupa satu file vs. satu direktori?
2. Apakah kata-kata yang paling sering muncul berubah? Mengapa?

---

## Rangkuman Pertemuan 4

```
┌─────────────────────────────────────────────────────────────────┐
│              RANGKUMAN PERTEMUAN 4 (ASINKRON)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  📖 TEORI: Arsitektur Apache Hadoop                              │
│     • HDFS: NameNode + DataNode, block 128MB, replication 3     │
│     • YARN: ResourceManager + NodeManager + Container            │
│     • MapReduce: Map → Shuffle & Sort → Reduce                  │
│                                                                 │
│  🔧 PRAKTIKUM MANDIRI: Hands-on Hadoop                           │
│     • Lab 1: HDFS CLI (mkdir, put, get, cat, setrep, dll.)      │
│     • Lab 2: NameNode & YARN Web UI monitoring                   │
│     • Lab 3: MapReduce WordCount end-to-end                      │
│                                                                 │
│  🎯 KEY TAKEAWAY                                                 │
│     🐘 Hadoop = fondasi ekosistem Big Data                      │
│     💻 Hands-on > teori: memahami lewat praktik langsung         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Preview Pertemuan 5

- **Topik:** Apache Spark — Arsitektur dan Pemrosesan Data Terdistribusi
- **Materi teori:** Arsitektur Spark, RDD, DataFrame, Spark SQL
- **Hands-on:** PySpark via Google Colab atau Docker
- **Persiapkan:** Pastikan bisa akses Google Colab atau Docker!

---

## Tugas Pertemuan 4

> **Jenis:** Individu
> **Deadline:** Sebelum Pertemuan 5 (via LMS)
> **Format:** PDF atau Google Docs (share link)

### Tugas: "Lab Report — Hadoop Hands-on"

Submit laporan praktikum yang mencakup:

1. **Screenshot** semua output dari Lab 1, 2, dan 3
2. **Jawaban** semua pertanyaan yang ditandai 📝
3. **Refleksi** singkat (3–5 kalimat):
   - Apa yang paling kamu pahami setelah hands-on?
   - Apa yang masih membingungkan?
   - Kendala teknis apa yang kamu temui saat setup/lab, dan bagaimana kamu mengatasinya?

> 💡 **Tips submission:**
>
> - Pastikan screenshot jelas dan terbaca (bukan blur)
> - Sertakan perintah yang kamu jalankan beserta outputnya
> - Jika ada error, screenshot juga error-nya dan jelaskan apa yang kamu lakukan untuk menyelesaikannya

### Kriteria Penilaian

| Kriteria                          | Bobot |
| --------------------------------- | ----- |
| Kelengkapan screenshot dan output | 40%   |
| Ketepatan jawaban pertanyaan      | 35%   |
| Kualitas refleksi                 | 15%   |
| Kerapian laporan                  | 10%   |

> 🆘 **Ada pertanyaan atau kendala?** Hubungi dosen via forum diskusi LMS atau grup WhatsApp kelas. Jangan ragu untuk bertanya!

---

## Referensi

1. White, T. (2015). *Hadoop: The Definitive Guide* (4th ed.). O'Reilly Media.
2. Apache Hadoop Documentation. https://hadoop.apache.org/docs/current/
3. HDFS Commands Guide. https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html
4. Vavilapalli, V. K., et al. (2013). "Apache Hadoop YARN." *ACM SoCC '13*.
5. Dean, J., & Ghemawat, S. (2004). "MapReduce: Simplified Data Processing on Large Clusters." *OSDI '04*.