# Pertemuan 1: Pengantar Big Data



|                           |                                                                                |

| ------------------------- | ------------------------------------------------------------------------------ |

| **Mata Kuliah**     | Big Data dan Data Lakehouse                                                    |

| **Pertemuan**       | 1 (Minggu 1)                                                                   |

| **Durasi**          | 120 menit                                                                      |

| **CPMK**            | CPMK-1                                                                         |

| **Kemampuan Akhir** | Mahasiswa mampu menjelaskan definisi, sejarah, dan karakteristik Big Data (5V) |

| **Metode**          | Ceramah, Diskusi, Tanya Jawab                                                  |



---



## Agenda Perkuliahan



| Waktu          | Durasi | Kegiatan                             | Keterangan                                           |

| -------------- | ------ | ------------------------------------ | ---------------------------------------------------- |

| 00:00 – 00:10 | 10 min | Pembukaan & Kontrak Kuliah           | Perkenalan dosen, overview mata kuliah, aturan kelas |

| 00:10 – 00:30 | 20 min | Bagian 1: Apa Itu Big Data?          | Definisi, mengapa penting, contoh sehari-hari        |

| 00:30 – 00:60 | 30 min | Bagian 2: Sejarah & Evolusi Big Data | Timeline perkembangan, milestone penting             |

| 00:60 – 01:00 | 30 min | Bagian 3: Karakteristik 5V           | Volume, Velocity, Variety, Veracity, Value           |

| 01:30 – 01:45 | 15 min | Bagian 4: Studi Kasus & Diskusi      | Contoh nyata Big Data di industri                    |

| 01:45 – 02:00 | 15 min | Penutup & Penugasan                  | Rangkuman, tugas, dan preview minggu depan           |



---



## Pembukaan & Kontrak Kuliah (10 menit)



### Perkenalan Dosen



- Nama, latar belakang, dan bidang keahlian

- Kontak dan jam konsultasi



### Overview Mata Kuliah



- **Tujuan:** Memahami dan menguasai teknologi Big Data & Data Lakehouse

- **16 pertemuan:** Konsep → Teknologi → Implementasi → Proyek

- **Penilaian:**

  - Keaktifan & Partisipasi: 5%

  - Tugas Individu: 20%

  - UTS: 20%

  - Proyek Kelompok: 25%

  - Final Project: 30%



### Kontrak Kuliah



- Kehadiran minimal 80%

- Tugas dikumpulkan tepat waktu via LMS

- Plagiarisme = nilai 0

- Mahasiswa diharapkan aktif berdiskusi



> 💡 **Ice Breaker:** "Berapa banyak data yang kalian hasilkan hari ini?"

>

> - Berapa kali membuka media sosial?

> - Berapa pesan WhatsApp yang dikirim?

> - Berapa foto/video yang diambil?



---



## Bagian 1: Apa Itu Big Data? (20 menit)



### 1.1 Definisi Big Data



**Big Data** adalah kumpulan data yang sangat besar, kompleks, dan bertumbuh dengan cepat sehingga **tidak dapat dikelola** menggunakan alat pemrosesan data tradisional.



> *"Big Data is data that exceeds the processing capacity of conventional database systems. The data is too big, moves too fast, or doesn't fit the structures of your database architectures."*

> — **Dumbill, 2012**



> *"Big Data refers to datasets whose size is beyond the ability of typical database software tools to capture, store, manage, and analyze."*

> — **McKinsey Global Institute, 2011**



#### Analogi Sederhana



| Analogi        | Data Tradisional         | Big Data                                          |

| -------------- | ------------------------ | ------------------------------------------------- |

| 🚰 Air         | Mengambil air dari keran | Menghadapi tsunami                                |

| 📚 Buku        | Membaca satu buku        | Membaca seluruh perpustakaan nasional dalam 1 jam |

| 🚗 Lalu Lintas | Jalan kampung            | 20 jalur tol padat di jam sibuk                   |



### 1.2 Mengapa Big Data Penting?



Dunia menghasilkan data dalam jumlah yang **eksponensial**:



| Tahun | Data yang Dihasilkan per Hari |

| ----- | ----------------------------- |

| 2010  | ~2 Exabytes                   |

| 2015  | ~15 Exabytes                  |

| 2020  | ~2.5 Quintillion bytes        |

| 2025  | ~463 Exabytes (estimasi)      |



> 📊 **Konteks:** 1 Exabyte = 1 miliar Gigabyte ≈ 250 juta DVD



#### Mengapa kita perlu peduli?



1. **Pengambilan keputusan berbasis data** — bisnis yang menggunakan big data analytics 5x lebih cepat mengambil keputusan

2. **Keunggulan kompetitif** — perusahaan data-driven 23% lebih profitabel (McKinsey)

3. **Inovasi produk** — Netflix menghemat $1 miliar/tahun dari sistem rekomendasi

4. **Efisiensi operasional** — UPS menghemat 10 juta galon bahan bakar/tahun menggunakan analisis rute big data



### 1.3 Data Tradisional vs. Big Data



| Aspek                 | Data Tradisional           | Big Data                                 |

| --------------------- | -------------------------- | ---------------------------------------- |

| **Ukuran**      | Megabytes – Gigabytes     | Terabytes – Petabytes – Exabytes       |

| **Struktur**    | Terstruktur (tabel, SQL)   | Terstruktur + Semi + Tidak terstruktur   |

| **Kecepatan**   | Batch (harian/mingguan)    | Real-time / Near real-time               |

| **Sumber**      | Sistem internal (ERP, CRM) | IoT, media sosial, sensor, log, dll.     |

| **Penyimpanan** | RDBMS (MySQL, PostgreSQL)  | HDFS, NoSQL, Cloud Storage               |

| **Pemrosesan**  | Single server              | Cluster terdistribusi                    |

| **Nilai**       | Laporan standar            | Prediksi, insight, dan pattern discovery |



---



## Bagian 2: Sejarah dan Evolusi Big Data (30 menit)



### 2.1 Timeline Perkembangan



```

┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐

│  1960s  │───▶│  1980s  │───▶│  2000s  │───▶│  2010s  │───▶│  2020s  │

│Mainframe│    │ RDBMS & │    │Google & │    │Cloud &  │    │ AI &    │

│& Batch  │    │   SQL   │    │ Hadoop  │    │Real-time│    │Lakehouse│

└─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘

```



### 2.2 Era-by-Era



#### 🏛️ Era 1: Komputasi Awal (1960–1980)



- **1960s:** Mainframe IBM untuk sensus dan keuangan

- **1970:** Edgar F. Codd memperkenalkan **model relasional**

- **1979:** Oracle merilis RDBMS komersial pertama

- Data masih kecil dan terpusat

- Pemrosesan **batch** (kartu punch → magnetic tape)



#### 🗄️ Era 2: Database & Data Warehouse (1980–2000)



- **1983:** IBM merilis DB2

- **1989:** Tim Berners-Lee menemukan **World Wide Web**

- **1990s:** Konsep **Data Warehouse** oleh Bill Inmon

- **1996:** Konsep **OLAP** untuk analisis multidimensi

- Tantangan: Data mulai tumbuh, tapi masih bisa ditangani RDBMS



#### 🌐 Era 3: Ledakan Internet & Hadoop (2000–2010)



- **2001:** Doug Laney memperkenalkan konsep **3V** (Volume, Velocity, Variety)

- **2003:** Google mempublikasikan paper **Google File System (GFS)**

- **2004:** Google mempublikasikan paper **MapReduce**

- **2006:** Yahoo! merilis **Apache Hadoop** (open-source)

- **2007:** iPhone diluncurkan → ledakan data mobile

- **2008:** Facebook mencapai 100 juta pengguna

- Data tumbuh **eksponensial** → RDBMS tidak cukup



#### ☁️ Era 4: Cloud & Real-time (2010–2020)



- **2010:** Apache Spark dimulai di UC Berkeley (AMPLab)

- **2011:** Apache Kafka dikembangkan di LinkedIn

- **2012:** Istilah **"Big Data"** menjadi mainstream

- **2014:** Apache Spark menjadi proyek top-level Apache

- **2015:** Kubernetes 1.0, era container & microservices

- **2017:** TensorFlow dan PyTorch populer → AI + Big Data

- Cloud computing (AWS, GCP, Azure) membuat Big Data lebih aksesibel



#### 🏗️ Era 5: Data Lakehouse & AI (2020–sekarang)



- **2020:** Databricks memperkenalkan konsep **Data Lakehouse**

- **2021:** Delta Lake, Apache Iceberg, Apache Hudi → table formats modern

- **2023:** ChatGPT & Generative AI → kebutuhan data semakin besar

- **2025:** Edge computing, real-time AI, vector databases

- Tren: **Data Lakehouse** menggabungkan keunggulan Data Warehouse + Data Lake



### 2.3 Faktor Pendorong Pertumbuhan Big Data



```

┌──────────────────────────────────────────────────────────┐

│               FAKTOR PENDORONG BIG DATA                  │

├──────────────┬───────────────┬───────────────────────────┤

│  TEKNOLOGI   │   BISNIS      │   SOSIAL                  │

├──────────────┼───────────────┼───────────────────────────┤

│ • IoT/Sensor │ • E-commerce  │ • Media sosial            │

│ • Cloud      │ • Fintech     │ • Smartphone              │

│ • 5G Network │ • Healthtech  │ • Streaming (Netflix, YT) │

│ • AI/ML      │ • Logistik    │ • Wearable devices        │

│ • Edge Comp. │ • Periklanan  │ • Smart city              │

└──────────────┴───────────────┴───────────────────────────┘

```



---



## Bagian 3: Karakteristik Big Data — The 5V's (30 menit)



> Awalnya ada **3V** (Laney, 2001), kemudian berkembang menjadi **5V**, dan beberapa literatur menyebut hingga **7V** atau **10V**. Dalam kuliah ini kita fokus pada **5V** utama.



### 3.1 Volume — Ukuran Data



**Volume** mengacu pada **jumlah data** yang dihasilkan dan disimpan.



#### Skala Ukuran Data



```

Byte → KB → MB → GB → TB → PB → EB → ZB → YB

                              ↑

                        Kita di sini sekarang

```



| Satuan                   | Ukuran       | Contoh                                               |

| ------------------------ | ------------ | ---------------------------------------------------- |

| **Kilobyte (KB)**  | 10³ bytes   | 1 halaman teks                                       |

| **Megabyte (MB)**  | 10⁶ bytes   | 1 lagu MP3                                           |

| **Gigabyte (GB)**  | 10⁹ bytes   | 1 film HD                                            |

| **Terabyte (TB)**  | 10¹² bytes | 500 jam video HD                                     |

| **Petabyte (PB)**  | 10¹⁵ bytes | Seluruh akta perpustakaan kongres AS × 50           |

| **Exabyte (EB)**   | 10¹⁸ bytes | Seluruh kata yang pernah diucapkan oleh umat manusia |

| **Zettabyte (ZB)** | 10²¹ bytes | Seluruh data di dunia saat ini                       |



#### Contoh Volume di Dunia Nyata



- **Facebook:** ~4 Petabytes data baru per hari

- **YouTube:** 500+ jam video diunggah setiap menit

- **CERN (Large Hadron Collider):** ~1 Petabyte per detik (sebelum difilter)

- **Walmart:** ~2.5 Petabytes data transaksi per jam

- **Gojek/Grab:** Jutaan transaksi dan data GPS per hari di Indonesia



### 3.2 Velocity — Kecepatan Data



**Velocity** mengacu pada **kecepatan data dihasilkan, dikumpulkan, dan diproses**.



#### Spektrum Kecepatan



```

    ┌────────────┐      ┌─────────────┐      ┌─────────────┐

    │   BATCH    │ ───▶ │ NEAR REAL-  │ ───▶ │  REAL-TIME  │

    │            │      │    TIME     │      │             │

    │ Jam/Hari   │      │ Menit/Detik │      │ Milidetik   │

    │            │      │             │      │             │

    │ Laporan    │      │ Dashboard   │      │ Deteksi     │

    │ bulanan    │      │ monitoring  │      │ fraud       │

    └────────────┘      └─────────────┘      └─────────────┘

```



#### Contoh Velocity



| Sumber                      | Kecepatan                     |

| --------------------------- | ----------------------------- |

| **Twitter/X**         | ~6.000 tweet per detik        |

| **Google Search**     | ~100.000+ pencarian per detik |

| **Bursa Saham NYSE**  | ~1 miliar transaksi per hari  |

| **Sensor IoT pabrik** | Ribuan data poin per detik    |

| **WhatsApp**          | 100 miliar pesan per hari     |



#### Tantangan Velocity



- Bagaimana memproses data yang datang **sangat cepat**?

- Bagaimana mengambil keputusan **dalam hitungan milidetik**? (misal: deteksi fraud kartu kredit)

- Solusi: **Stream processing** (Apache Kafka, Apache Flink, Spark Streaming)



### 3.3 Variety — Keragaman Data



**Variety** mengacu pada **jenis dan format data** yang bervariasi.



#### Tiga Kategori Struktur Data



```

┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────┐

│   TERSTRUKTUR   │  │ SEMI-TERSTRUKTUR │  │ TIDAK TERSTRUKTUR   │

│                 │  │                  │  │                     │

│ • Tabel SQL     │  │ • JSON           │  │ • Teks bebas        │

│ • Spreadsheet   │  │ • XML            │  │ • Gambar / Foto     │

│ • CSV           │  │ • Log files      │  │ • Video / Audio     │

│                 │  │ • Email (header)  │  │ • Posting sosmed    │

│                 │  │ • YAML           │  │ • Dokumen PDF       │

│                 │  │                  │  │ • Sensor IoT        │

│    ~20% data    │  │    ~10% data     │  │    ~70% data        │

│    di dunia     │  │    di dunia      │  │    di dunia         │

└─────────────────┘  └──────────────────┘  └─────────────────────┘

```



> ⚠️ **Fakta penting:** Sekitar **80% data di dunia adalah tidak terstruktur** — dan ini adalah data yang paling sulit untuk dikelola tapi seringkali paling bernilai.



#### Contoh Variety di Satu Perusahaan E-Commerce



| Jenis             | Contoh Data                            | Format              |

| ----------------- | -------------------------------------- | ------------------- |

| Terstruktur       | Data transaksi, profil pelanggan       | SQL Tables (MySQL)  |

| Semi-terstruktur  | Log server, API response               | JSON, XML           |

| Tidak terstruktur | Review pelanggan, foto produk, chat CS | Teks, Gambar, Audio |



### 3.4 Veracity — Kebenaran/Keakuratan Data



**Veracity** mengacu pada **kualitas, keakuratan, dan kepercayaan** terhadap data.



#### Masalah Umum Veracity



```

┌──────────────────────────────────────────┐

│         MASALAH KUALITAS DATA            │

├──────────────┬───────────────────────────┤

│ Missing Data │ Data tidak lengkap        │

│ Noisy Data   │ Data mengandung error     │

│ Biased Data  │ Data tidak representatif  │

│ Duplicate    │ Data duplikat/redundan    │

│ Outdated     │ Data sudah kedaluwarsa    │

│ Inconsistent │ Format/nilai tidak konsisten│

└──────────────┴───────────────────────────┘

```



#### Contoh Veracity



- **Media sosial:** Akun bot membuat ~15% aktivitas Twitter — data valid?

- **IoT sensor:** Sensor rusak mengirimkan data salah — bagaimana mendeteksinya?

- **Survei:** Responden memberikan jawaban tidak jujur

- **Data duplikat:** Satu pelanggan terdaftar 3× di database karena typo nama



#### Dampak Veracity yang Buruk



> *"Garbage In, Garbage Out (GIGO)"*

>

> Analisis terbaik pun tidak berguna jika datanya buruk!



- IBM memperkirakan **bad data** menghabiskan $3.1 triliun per tahun di ekonomi AS

- 27% eksekutif mengatakan tidak yakin seberapa akurat data mereka



### 3.5 Value — Nilai Data



**Value** mengacu pada **manfaat dan nilai bisnis** yang dapat diekstrak dari data.



> Volume besar + Velocity tinggi + Variety beragam **tidak ada artinya** jika kita tidak bisa mengekstrak **nilai** darinya.



#### Piramida Nilai Data



```

           ╱╲

          ╱  ╲

         ╱ AI ╲         → Prediksi & Otomasi

        ╱ /ML  ╲

       ╱────────╲

      ╱ Analytics╲      → Insight & Pattern

     ╱────────────╲

    ╱  Information ╲    → Konteks & Makna

   ╱────────────────╲

  ╱    Raw Data      ╲  → Data Mentah

 ╱────────────────────╲

```



#### Contoh Nilai dari Big Data



| Industri               | Data                         | Value yang Dihasilkan                                  |

| ---------------------- | ---------------------------- | ------------------------------------------------------ |

| **Kesehatan**    | Rekam medis + genomik        | Deteksi penyakit lebih awal, personalized medicine     |

| **Retail**       | Transaksi + perilaku belanja | Rekomendasi produk, optimasi stok                      |

| **Transportasi** | GPS + lalu lintas            | Rute optimal, estimasi waktu tiba (Gojek, Google Maps) |

| **Perbankan**    | Transaksi kartu kredit       | Deteksi fraud real-time                                |

| **Manufaktur**   | Sensor mesin + produksi      | Predictive maintenance, mengurangi downtime            |

| **Pemerintah**   | Data penduduk + ekonomi      | Perencanaan kebijakan berbasis data                    |



### 3.6 Ringkasan 5V



| V                  | Pertanyaan Kunci            | Contoh                       |

| ------------------ | --------------------------- | ---------------------------- |

| **Volume**   | Seberapa besar datanya?     | Petabytes data per hari      |

| **Velocity** | Seberapa cepat datanya?     | Real-time streaming          |

| **Variety**  | Seberapa beragam formatnya? | SQL + JSON + Video + Sensor  |

| **Veracity** | Seberapa akurat datanya?    | Data cleaning & validation   |

| **Value**    | Apa manfaatnya?             | Prediksi, insight, efisiensi |



---



## Bagian 4: Studi Kasus & Diskusi (15 menit)



### Studi Kasus: Penerapan Big Data di Indonesia



#### 🚗 Gojek / Grab



- **Volume:** Jutaan order per hari di seluruh Indonesia

- **Velocity:** Matching driver-penumpang dalam hitungan detik

- **Variety:** GPS, teks chat, foto, rating, transaksi keuangan

- **Veracity:** Validasi lokasi GPS yang tidak akurat (GPS drift)

- **Value:** Dynamic pricing, estimasi waktu tiba, fraud detection



#### 🏦 Bank Mandiri / BCA



- **Volume:** Jutaan transaksi per hari

- **Velocity:** Deteksi fraud kartu kredit dalam milidetik

- **Variety:** Transaksi ATM, mobile banking, kartu kredit, log akses

- **Veracity:** Memastikan transaksi benar vs. palsu

- **Value:** Pencegahan fraud, credit scoring, customer segmentation



#### 🛒 Tokopedia / Shopee



- **Volume:** Miliaran listing produk + review

- **Velocity:** Flash sale dengan jutaan request per detik

- **Variety:** Foto produk, deskripsi teks, video, rating, chat

- **Veracity:** Review palsu, produk tipuan

- **Value:** Personalized recommendation, search optimization



### 📝 Diskusi Kelompok (10 menit)



> Bagi kelas menjadi kelompok kecil (3–4 orang). Setiap kelompok mendiskusikan:



**Pertanyaan Diskusi:**



1. Identifikasi **satu contoh Big Data** dari kehidupan sehari-hari kalian (selain yang sudah disebutkan)

2. Jelaskan bagaimana **5V** berlaku pada contoh tersebut

3. Apa **tantangan** terbesar dalam mengelola data tersebut?



**Waktu:** 5 menit diskusi → 5 menit presentasi singkat (2–3 kelompok)



---



## Penutup (15 menit)



### Rangkuman Pertemuan 1



```

┌─────────────────────────────────────────────────┐

│                RANGKUMAN HARI INI                │

├─────────────────────────────────────────────────┤

│                                                 │

│  1. Big Data = data yang sangat besar,          │

│     kompleks, dan cepat → tidak bisa            │

│     ditangani tools tradisional                 │

│                                                 │

│  2. Big Data berkembang dari era mainframe       │

│     hingga era Data Lakehouse & AI              │

│                                                 │

│  3. 5V Karakteristik:                           │

│     • Volume  → ukuran data                     │

│     • Velocity → kecepatan data                 │

│     • Variety → keragaman data                  │

│     • Veracity → keakuratan data                │

│     • Value   → nilai bisnis data               │

│                                                 │

│  4. Big Data diterapkan di berbagai industri     │

│     untuk menghasilkan insight bernilai          │

│                                                 │

└─────────────────────────────────────────────────┘

```



### Preview Pertemuan 2



- **Topik:** Ekosistem Big Data dan Use Case di Industri

- Kita akan melihat lebih dalam berbagai teknologi dalam ekosistem Big Data

- Mahasiswa akan mempresentasikan hasil analisis studi kasus industri



### Tugas Pertemuan 1



> **Jenis:** Individual

> **Deadline:** Sebelum Pertemuan 2 (via LMS)



**Tugas:**

Pilih **satu industri** (kesehatan, pendidikan, pemerintahan, pertanian, energi, pariwisata, atau lainnya) dan tulis laporan singkat (1–2 halaman) yang menjawab:



1. **Identifikasi** minimal 3 sumber data Big Data dalam industri tersebut

2. **Jelaskan** bagaimana 5V berlaku pada data tersebut

3. **Analisis** apa potensi value yang bisa dihasilkan dari data tersebut

4. **Sebutkan** tantangan apa yang mungkin dihadapi dalam pengelolaannya



**Format:** Markdown atau PDF, dinilai berdasarkan kualitas analisis dan pemahaman konsep.



---



## Referensi



1. Laney, D. (2001). "3D Data Management: Controlling Data Volume, Velocity, and Variety." META Group Research Note.

2. Mayer-Schönberger, V., & Cukier, K. (2013). *Big Data: A Revolution That Will Transform How We Live, Work, and Think*. Houghton Mifflin Harcourt.

3. White, T. (2015). *Hadoop: The Definitive Guide* (4th ed.). O'Reilly Media.

4. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly Media.

5. McKinsey Global Institute. (2011). "Big Data: The Next Frontier for Innovation, Competition, and Productivity."

6. IDC. (2024). "The Global DataSphere Report."