# Pertemuan 8: Apache Kafka — Event Streaming Platform

|                           |                                                                                   |
| ------------------------- | --------------------------------------------------------------------------------- |
| **Mata Kuliah**     | Big Data dan Data Lakehouse                                                       |
| **Pertemuan**       | 8 (Minggu 8)                                                                      |
| **Durasi**          | ± 210 menit (belajar mandiri)                                                    |
| **CPMK**            | CPMK-2                                                                            |
| **Kemampuan Akhir** | Mahasiswa mampu mengimplementasikan pipeline streaming data dengan Apache Kafka   |
| **Metode**          | 🖥️ Asinkron — Belajar Mandiri, Hands-on Lab, Kuis Online, Tugas Kelompok (PBL) |

---

> 🖥️ **PERTEMUAN ASINKRON**
>
> Pertemuan ini dilakukan secara **asinkron** (belajar mandiri, tidak ada tatap muka). Pelajari materi teori, kerjakan hands-on lab, dan selesaikan tugas kelompok sesuai panduan di bawah ini.

---

## 📋 Panduan Belajar Mandiri

Ikuti langkah-langkah berikut secara **berurutan**. Centang setiap langkah setelah selesai.

| No | Langkah                                                         | Estimasi | Keterangan                                       |
| -- | --------------------------------------------------------------- | -------- | ------------------------------------------------ |
| 1  | 📖 Baca**Bagian 1**: Event Streaming & Filosofi Kafka     | 20 menit | Konsep dasar, mengapa Kafka lahir                |
| 2  | 📖 Baca**Bagian 2**: Arsitektur & Komponen Inti           | 25 menit | Broker, Topic, Partition, Offset, Consumer Group |
| 3  | 📖 Baca**Bagian 3**: Desain Internal Kafka                | 20 menit | Log storage, replication, delivery semantics     |
| 4  | 📖 Baca**Bagian 4**: Producer & Consumer API              | 15 menit | Konfigurasi kritis, batching, offset management  |
| 5  | 📖 Baca**Bagian 5**: Ekosistem — Kafka Connect & Streams | 15 menit | Integrasi dengan sistem eksternal                |
| 6  | 🧠 Kerjakan**Kuis Online** (25 soal)                      | 20 menit | Akses link kuis di LMS                           |
| 7  | 🛠️ Setup Docker Kafka                                         | 10 menit | Jalankan Kafka lewat Docker Compose              |
| 8  | 🔧 Kerjakan**Lab 1**: Eksplorasi CLI & Topic Design       | 25 menit | Internals: partition, offset, consumer lag       |
| 9  | 🔧 Kerjakan**Lab 2**: Pipeline Cuaca IoT Real-Time        | 30 menit | Simulasi sensor multi-kota, windowing manual     |
| 10 | 🔧 Kerjakan**Lab 3**: Exactly-Once & Idempotent Producer  | 20 menit | Uji delivery guarantees, offset commit manual    |
| 11 | 📝 Kerjakan**Tugas Kelompok (PBL)** dan submit via LMS    | —       | Rilis hari ini, deadline 2 minggu                |

> ⏱️ **Total estimasi waktu:** ± 200–210 menit
> 💡 Kamu bisa mencicil — tidak harus selesai sekaligus. Disarankan Lab 1 sebelum Lab 2.

---

# 📖 MATERI TEORI

> ⏱️ **Estimasi waktu baca:** 90–100 menit
> 📌 **Pelajari semua bagian sebelum mengerjakan kuis dan lab.**
> 📚 **Referensi utama:** [Apache Kafka 4.2 Documentation](https://kafka.apache.org/42/documentation/)

---

## Bagian 1: Event Streaming & Filosofi Kafka (20 menit)

### 1.1 Apa Itu Event Streaming?

Menurut dokumentasi resmi Apache Kafka:

> *"Event streaming is the digital equivalent of the human body's central nervous system. It is the technological foundation for the 'always-on' world where businesses are increasingly software-defined and automated."*
> — [kafka.apache.org/intro](https://kafka.apache.org/intro)

Secara teknis, **event streaming** adalah praktik:

1. **Menangkap** data secara real-time dari sumber event (database, sensor, aplikasi mobile)
2. **Menyimpan** stream of events secara tahan lama untuk diambil kembali kelak
3. **Memproses & bereaksi** terhadap event stream secara real-time maupun retrospektif
4. **Merutekan** event stream ke berbagai sistem tujuan sesuai kebutuhan

> 🧠 **Apa itu EVENT?**
>
> Sebuah event merekam fakta bahwa *"sesuatu telah terjadi"* di dunia nyata atau dalam sistem bisnis. Menurut dokumentasi Kafka, setiap event memiliki empat komponen:
>
> | Komponen                     | Contoh                                          |
> | ---------------------------- | ----------------------------------------------- |
> | **Key**                | `"user-alice"`                                |
> | **Value**              | `"made a payment of Rp 500.000 to Tokopedia"` |
> | **Timestamp**          | `"2026-04-19T14:32:00+07:00"`                 |
> | **Headers** (opsional) | `{"source": "mobile-app", "version": "3.2"}`  |

### 1.2 Mengapa Kafka Lahir?

Kafka dikembangkan oleh tim engineering **LinkedIn** pada **2010** untuk memecahkan masalah fundamental: bagaimana menghubungkan puluhan sistem internal yang terus bertambah tanpa menciptakan kekacauan integrasi?

```
┌───────────────────────────────────────────────────────────────────┐
│             MASALAH INTEGRASI SEBELUM KAFKA                        │
│          (LinkedIn, ~2010: N×M koneksi titik-ke-titik)            │
│                                                                   │
│  DB_Transaksi ──────────────────────────▶ Analytics Engine       │
│  Web_Activity ──────────────────────────▶ Recommendation Svc     │
│  Search_Log   ──────────────────────────▶ HDFS / Data Warehouse  │
│  Profile_Svc  ──────────────────────────▶ Monitoring Dashboard   │
│  Ads_Engine   ──────────────────────────▶ Email/Notif Service    │
│                                                                   │
│  ❌ 5 sumber × 5 tujuan = 25 pipeline custom                     │
│  ❌ Setiap pipeline = kode terpisah + monitoring terpisah        │
│  ❌ Jika Analytics Engine down → data dari semua sumber hilang   │
│  ❌ Tidak ada durabilitas — pesan hilang jika consumer lambat   │
│                                                                   │
│             SOLUSI: KAFKA (Hub-and-Spoke)                         │
│                                                                   │
│  DB_Transaksi ──┐                      ┌── Analytics Engine      │
│  Web_Activity ──┤                      ├── Recommendation Svc    │
│  Search_Log   ──┼──▶  ┌───────────┐──▶├── HDFS / DataWarehouse  │
│  Profile_Svc  ──┤     │   KAFKA   │   ├── Monitoring Dashboard  │
│  Ads_Engine   ──┘     └───────────┘   └── Email/Notif Service   │
│                                                                   │
│  ✅ N+M koneksi (bukan N×M)                                       │
│  ✅ Producer & consumer sepenuhnya decoupled                     │
│  ✅ Pesan disimpan — consumer bisa baca kapan saja               │
│  ✅ Satu topic, banyak consumer group berbeda                    │
└───────────────────────────────────────────────────────────────────┘
```

Kafka open-source ke Apache pada **2011**, dan sejak itu digunakan oleh lebih dari **80% perusahaan Fortune 100** [[kafka.apache.org/powered-by](https://kafka.apache.org/powered-by)].

### 1.3 Tiga Kemampuan Utama Kafka

Kafka menggabungkan **tiga kemampuan** dalam satu platform terintegrasi:

```
┌─────────────────────────────────────────────────────────────────┐
│              TIGA KEMAMPUAN INTI APACHE KAFKA                     │
│          (Sumber: kafka.apache.org/intro, 2024)                  │
│                                                                 │
│  1. PUBLISH & SUBSCRIBE                                          │
│     Menulis (publish) dan membaca (subscribe) stream of events  │
│     termasuk import/export data dari sistem eksternal secara    │
│     terus-menerus.                                              │
│                                                                 │
│  2. STORE (Durable & Reliable)                                   │
│     Menyimpan stream of events secara tahan lama dan andal      │
│     selama waktu yang dikonfigurasi (dari detik hingga selamanya│
│     — tergantung kebutuhan). Kafka's performance is effectively │
│     constant with respect to data size.                         │
│                                                                 │
│  3. PROCESS (Real-time & Retrospective)                          │
│     Memproses event stream saat terjadi (real-time) maupun      │
│     secara retrospektif (membaca ulang event lama).             │
│                                                                 │
│  Semua ini tersedia secara distributed, scalable, fault-        │
│  tolerant, elastic, dan secure.                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.4 Use Case Kafka di Industri

Menurut [kafka.apache.org/uses](https://kafka.apache.org/uses), berikut use case utama Kafka:

| Use Case                            | Deskripsi                                                                                                            | Contoh Nyata                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| **Messaging**                 | Pengganti message broker tradisional dengan throughput lebih tinggi, partisi built-in, dan replikasi                 | Decoupling microservices di startup fintech     |
| **Website Activity Tracking** | Original use case Kafka (LinkedIn) — publish setiap page view, klik, pencarian ke topic terpisah per tipe aktivitas | Tokopedia: track user journey sesaat            |
| **Metrics & Monitoring**      | Agregasi statistik dari sistem terdistribusi menjadi feed terpusat                                                   | Monitoring cluster Hadoop/Spark                 |
| **Log Aggregation**           | Pengganti solusi log aggregation: abstraksi log sebagai stream (lebih rendah latensi dari Scribe/Flume)              | Gojek: centralized log dari 500+ microservice   |
| **Stream Processing**         | Pipeline multi-tahap: raw input → transformasi → output ke topic baru                                              | Rekomendasi artikel berita real-time            |
| **Event Sourcing**            | State changes disimpan sebagai time-ordered sequence of records                                                      | Sistem perbankan: audit trail yang immutable    |
| **Commit Log**                | External commit log untuk distributed system — log compaction untuk re-sync node yang gagal                         | Database replication, CDC (Change Data Capture) |

> 🇮🇩 **Studi Kasus Indonesia:**
>
> - **Gojek**: >3 juta event/menit untuk dynamic pricing, driver-rider matching, fraud detection real-time
> - **Tokopedia**: >2 juta event/menit untuk real-time inventory, flash sale management, rekomendasi
> - **Bank BCA**: Streaming transaksi untuk fraud detection — setiap transaksi dievaluasi dalam <100ms

---

## Bagian 2: Arsitektur & Komponen Inti Kafka (25 menit)

### 2.1 Kafka Sebagai Distributed System

Kafka adalah **distributed system** yang terdiri dari server dan klien yang berkomunikasi melalui protokol TCP berkinerja tinggi. Menurut dokumentasi Kafka 4.2:

```
┌──────────────────────────────────────────────────────────────────────┐
│                    ARSITEKTUR KAFKA (MODE KRaft)                      │
│                    Kafka 4.2 — Tanpa ZooKeeper                       │
│                                                                      │
│   PRODUCERS                  KAFKA CLUSTER              CONSUMERS    │
│                                                                      │
│  ┌──────────┐     ┌──────────────────────────────────┐  ┌─────────┐  │
│  │Sensor IoT│────▶│            BROKER 1               │─▶│Group A │  │
│  └──────────┘     │  ┌──────────────────────────────┐ │  │(real-  │  │
│                   │  │  Topic: cuaca-sensor          │ │  │time    │  │
│  ┌──────────┐     │  │  P0:[e0,e1,e2,e3,e4,e5,...]  │ │  │monitor)│  │
│  │Web App   │────▶│  │  P1:[e0,e1,e2,e3,...]        │ │  └────────┘  │
│  └──────────┘     │  │  P2:[e0,e1,e2,...]           │ │             │
│                   │  └──────────────────────────────┘ │  ┌─────────┐  │
│  ┌──────────┐     └──────────────────────────────────┘  │Group B │  │
│  │Mobile App│     ┌──────────────────────────────────┐  │(batch   │  │
│  └──────────┘────▶│            BROKER 2               │─▶│analytics│  │
│                   │  ┌──────────────────────────────┐ │  └────────┘  │
│                   │  │  Topic: user-events           │ │             │
│                   │  │  P0:[e0,e1,e2,e3,...]        │ │             │
│                   │  └──────────────────────────────┘ │             │
│                   └──────────────────────────────────┘             │
│                                                                      │
│                  ┌────────────────────────────┐                      │
│                  │  KRaft Controller Quorum    │                      │
│                  │  (metadata, leader election)│                      │
│                  └────────────────────────────┘                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 Komponen Utama Kafka

#### 🖥️ Server: Broker

**Broker** adalah server Kafka yang membentuk storage layer. Dalam cluster produksi, biasanya ada 3 atau lebih broker yang tersebar di beberapa datacenter. Jika satu broker gagal, broker lain mengambil alih pekerjaannya tanpa kehilangan data.

Dokumentasi Kafka 4.2 menyebutkan:

> *"To let you implement mission-critical use cases, a Kafka cluster is highly scalable and fault-tolerant: if any of its servers fails, the other servers will take over their work to ensure continuous operations without any data loss."*

#### 🔑 KRaft Controller (Mode Kafka 4.x)

Sejak Kafka **3.3** (KIP-833), mode **KRaft** (Kafka Raft) menggantikan ZooKeeper sebagai sistem metadata dan koordinasi cluster. Kafka **4.0** sepenuhnya menghapus dukungan ZooKeeper.

```
┌─────────────────────────────────────────────────────────────────┐
│         EVOLUSI KOORDINASI METADATA KAFKA                         │
│                                                                 │
│  KAFKA ≤ 3.2 (Mode ZooKeeper):                                   │
│  ┌──────────┐    ┌──────────────────────┐                       │
│  │ZooKeeper │───▶│  Kafka Cluster       │                       │
│  │ Cluster  │    │  (Controller dipilih │                       │
│  │(separate)│    │  via ZooKeeper)      │                       │
│  └──────────┘    └──────────────────────┘                       │
│  ❌ Operasional 2 sistem berbeda                                 │
│  ❌ Bottleneck di ZooKeeper untuk cluster besar                  │
│  ❌ ZooKeeper memiliki limit ~200K partisi per cluster           │
│                                                                 │
│  KAFKA ≥ 4.0 (Mode KRaft):                                       │
│  ┌───────────────────────────────────────┐                      │
│  │           Kafka Cluster               │                      │
│  │  ┌─────────────────────────────────┐  │                      │
│  │  │  KRaft Controller Quorum        │  │                      │
│  │  │  (Raft consensus, metadata)     │  │                      │
│  │  └─────────────────────────────────┘  │                      │
│  │  Broker 1 │ Broker 2 │ Broker 3       │                      │
│  └───────────────────────────────────────┘                      │
│  ✅ Single system — lebih sederhana operasional                 │
│  ✅ Skalabilitas lebih baik (jutaan partisi per cluster)         │
│  ✅ Startup lebih cepat, recovery lebih cepat                   │
└─────────────────────────────────────────────────────────────────┘
```

#### 💻 Client: Producer & Consumer

| Komponen               | Peran                                        | Analogi           |
| ---------------------- | -------------------------------------------- | ----------------- |
| **Producer**     | Menulis (publish) event ke topic Kafka       | Pengirim surat    |
| **Consumer**     | Membaca (subscribe) event dari topic Kafka   | Penerima surat    |
| **Admin Client** | Manajemen topic, broker, konfigurasi cluster | Kepala kantor pos |

### 2.3 Topic & Partisi — Inti Penyimpanan Kafka

#### 📁 Topic

Menurut dokumentasi Kafka:

> *"Topics in Kafka are always multi-producer and multi-subscriber: a topic can have zero, one, or many producers that write events to it, as well as zero, one, or many consumers that subscribe to these events. Events in a topic can be read as often as needed — unlike traditional messaging systems, events are not deleted after consumption."*

**Retensi** dikontrol dengan konfigurasi `retention.ms` atau `retention.bytes` per topic.

#### 🪣 Partisi

```
┌─────────────────────────────────────────────────────────────────────┐
│                ANATOMI TOPIC KAFKA: "transaksi-pembayaran"           │
│              (Dokumentasi: kafka.apache.org/intro#main-concepts)    │
│                                                                     │
│  PARTITION 0  (di Broker 1)                                         │
│  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┐                             │
│  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │  ← OFFSET (auto-increment)│
│  │tx1│tx4│tx7│tx9│...│...│...│...│...│  ← Event (immutable)       │
│  └───┴───┴───┴───┴───┴───┴───┴───┴───┘                             │
│  ▲ Oldest (masih ada selama retention period)    ▲ Latest           │
│                                                                     │
│  PARTITION 1  (di Broker 2)                                         │
│  ┌───┬───┬───┬───┬───┬───┐                                         │
│  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │  ← Offset partisi ini independen!       │
│  │tx2│tx5│tx8│...│...│...│                                         │
│  └───┴───┴───┴───┴───┴───┘                                         │
│                                                                     │
│  PARTITION 2  (di Broker 3)                                         │
│  ┌───┬───┬───┬───┬───┐                                             │
│  │ 0 │ 1 │ 2 │ 3 │ 4 │                                             │
│  │tx3│tx6│...│...│...│                                             │
│  └───┴───┴───┴───┴───┘                                             │
│                                                                     │
│  💡 KEY POINTS dari dokumentasi Kafka:                               │
│  • Event dengan KEY yang sama → selalu ke PARTISI yang sama         │
│  • Dalam satu partisi: urutan penulisan = urutan pembacaan (strict) │
│  • Antar partisi: TIDAK ada jaminan urutan                          │
│  • Offset di setiap partisi INDEPENDEN satu sama lain               │
│  • Event TIDAK dihapus setelah dibaca (berbeda dari queue biasa)   │
└─────────────────────────────────────────────────────────────────────┘
```

#### 🔢 Bagaimana Event Ditempatkan ke Partisi?

Kafka menggunakan algoritma berikut untuk menentukan partisi tujuan:

```
JIKA event memiliki KEY:
    partisi = hash(key) % jumlah_partisi
    → Event dengan key yang sama → partisi yang sama SELALU
    → Menjamin ordering per-entity (misal: per user, per device)

JIKA event TANPA KEY:
    Kafka 4.x: Gunakan "Sticky Partitioner" (batch per partisi
    sebelum pindah) untuk efisiensi — berbeda dari round-robin!
  
JIKA producer menentukan partisi secara eksplisit:
    partisi = partisi yang ditentukan producer
```

> ⚠️ **Gotcha penting:** Jika kamu **mengubah jumlah partisi** setelah data ada, mapping `hash(key) % jumlah_partisi` berubah! Event yang sebelumnya ke partisi 0 bisa pindah ke partisi lain. Rencanakan jumlah partisi dengan baik sejak awal.

### 2.4 Replikasi — Fault Tolerance

```
┌─────────────────────────────────────────────────────────────────┐
│            REPLIKASI TOPIC PARTISI DI KAFKA                       │
│         replication-factor = 3  (setting produksi umum)          │
│                                                                 │
│  Topic "transaksi", Partition 0:                                 │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   BROKER 1   │  │   BROKER 2   │  │   BROKER 3   │           │
│  │              │  │              │  │              │           │
│  │  [P0 LEADER] │  │ [P0 FOLLOWER]│  │ [P0 FOLLOWER]│           │
│  │  ← Semua    │  │  Replika     │  │  Replika     │           │
│  │    Read &   │  │  (sinkron)   │  │  (sinkron)   │           │
│  │    Write    │  │              │  │              │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│         ▲                                                       │
│         │ Producer menulis ke Leader saja                        │
│         │ Follower menarik data dari Leader (pull-based)        │
│                                                                 │
│  ISR (In-Sync Replicas): set broker yang sudah sinkron          │
│  dengan leader. Default: commit hanya sah jika SEMUA ISR ack.   │
│                                                                 │
│  SKENARIO BROKER 1 MATI:                                         │
│  → Kafka Controller memilih leader baru dari ISR (Broker 2/3)  │
│  → Tidak ada data loss jika min.insync.replicas terpenuhi       │
│  → Producer & Consumer otomatis redirect ke leader baru         │
└─────────────────────────────────────────────────────────────────┘
```

### 2.5 Consumer Groups & Paralelisme

Ini adalah salah satu **fitur paling powerful** Kafka: kemampuan untuk memiliki banyak consumer group independen yang masing-masing membaca semua data dari topic yang sama.

```
┌─────────────────────────────────────────────────────────────────────┐
│              CONSUMER GROUPS — PARALELISME & MULTI-SUBSCRIBER        │
│                                                                     │
│  Topic "log-aktivitas-pengguna" (4 partisi: P0, P1, P2, P3)        │
│                                                                     │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐                                       │
│  │ P0 │ │ P1 │ │ P2 │ │ P3 │                                       │
│  └──┬─┘ └──┬─┘ └──┬─┘ └──┬─┘                                       │
│     │      │      │      │                                         │
│     ├──────┴──────┴──────┴──────▶ CONSUMER GROUP "realtime-alert"  │
│     │                              C1(P0,P1) + C2(P2,P3) — 2 inst  │
│     │                                                               │
│     ├──────────────────────────▶  CONSUMER GROUP "ml-training"     │
│     │                              C_ML1(P0) + C_ML2(P1) +         │
│     │                              C_ML3(P2) + C_ML4(P3) — 4 inst  │
│     │                                                               │
│     └──────────────────────────▶  CONSUMER GROUP "data-archive"    │
│                                    C_Arch1(P0,P1,P2,P3) — 1 inst   │
│                                                                     │
│  💡 Setiap consumer group MEMBACA SEMUA DATA secara independen!    │
│  💡 Offset tiap group disimpan TERPISAH di topic __consumer_offsets │
│  💡 Satu partisi hanya bisa di-consume oleh SATU consumer per group│
│                                                                     │
│  ATURAN: consumers_aktif > partisi → consumer berlebih = IDLE ⚠️   │
│  ATURAN: consumers_aktif = partisi → OPTIMAL ✅                    │
│  ATURAN: consumers_aktif < partisi → satu consumer baca >1 part    │
└─────────────────────────────────────────────────────────────────────┘
```

#### Consumer Offset Management

Kafka menyimpan offset setiap consumer group di **internal topic** bernama `__consumer_offsets`. Dua strategi commit:

| Strategi                                               | Cara Kerja                                   | Risiko                                                                   |
| ------------------------------------------------------ | -------------------------------------------- | ------------------------------------------------------------------------ |
| **Auto commit** (`enable.auto.commit=true`)    | Kafka commit offset secara periodik otomatis | Bisa terjadi**data loss** jika consumer crash sebelum auto-commit  |
| **Manual commit** (`enable.auto.commit=false`) | Developer tentukan kapan commit offset       | Lebih aman, tapi perlu `commitSync()` atau `commitAsync()` eksplisit |

---

## Bagian 3: Desain Internal Kafka (20 menit)

### 3.1 Kafka sebagai Distributed Commit Log

Fondasi desain Kafka adalah konsep **distributed commit log** — sebuah append-only, ordered, dan immutable sequence of records. Jay Kreps (salah satu creator Kafka) menulis:

> *"A log is perhaps the simplest possible storage abstraction. It is an append-only, totally-ordered sequence of records ordered by time."*
> — [The Log: What every software engineer should know about real-time data&#39;s unifying abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)

```
┌─────────────────────────────────────────────────────────────────┐
│              KAFKA LOG: APPEND-ONLY DISK STRUCTURE               │
│                                                                 │
│  Partition = Sequential segment files di disk                   │
│                                                                 │
│  segments/                                                      │
│  ├── 00000000000000000000.log    ← Binary data (batch of events)│
│  ├── 00000000000000000000.index  ← Sparse offset index         │
│  ├── 00000000000000000000.timeindex ← Waktu event             │
│  ├── 00000000000001234567.log    ← Segment baru setelah 1GB    │
│  └── ...                                                        │
│                                                                 │
│  KENAPA DISK SEQUENTIAL I/O BISA SANGAT CEPAT?                  │
│  • Sequential write: disk mendekati kecepatan network           │
│  • Modern OS: page cache menyimpan hot data di RAM              │
│  • Kafka memanfaatkan sendfile() syscall (zero-copy I/O)        │
│  • Tidak perlu index per-record — cukup binary search di index  │
│                                                                 │
│  💡 Benchmark Kafka: sustain 2 juta writes/detik dari satu     │
│     partition di hardware biasa                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Delivery Guarantees (Semantics)

Dari [dokumentasi Kafka — Semantics](https://kafka.apache.org/42/documentation/#semantics):

```
┌─────────────────────────────────────────────────────────────────┐
│                 DELIVERY SEMANTICS KAFKA                          │
│                                                                 │
│  AT-MOST-ONCE (maksimal 1 kali diproses)                         │
│  Config: acks=0, no retry                                        │
│  ┌──────────┐──────▶┌──────────┐──────▶┌──────────┐            │
│  │ Producer │       │ Broker   │       │ Consumer │            │
│  └──────────┘       └──────────┘       └──────────┘            │
│  → Kirim tanpa menunggu ACK → Mungkin HILANG jika broker crash   │
│  → Offset di-commit SEBELUM diproses → Mungkin TERLEWAT         │
│  → Use case: metrics tidak kritis, log non-penting              │
│                                                                 │
│  AT-LEAST-ONCE (minimal 1 kali diproses)                         │
│  Config: acks=all, retries>0, enable.auto.commit=false          │
│  → Producer retry jika tidak ada ACK → Event MUNGKIN DUPLIKAT   │
│  → Offset di-commit SETELAH diproses → Tidak ada data loss!     │
│  → Use case: kebanyakan pipeline data, logging, analytics       │
│                                                                 │
│  EXACTLY-ONCE (tepat 1 kali diproses) — Kafka 4.x               │
│  Config: enable.idempotence=true, transactional.id=<id>,        │
│          isolation.level=read_committed                          │
│  → Idempotent producer: Kafka assign sequence number per PID    │
│  → Broker reject duplikat berdasarkan (producer-id, seq-number) │
│  → Transactional API: atomic write ke banyak partisi sekaligus  │
│  → Use case: fintech, billing, sistem kritis                    │
└─────────────────────────────────────────────────────────────────┘
```

**Idempotent Producer** (Kafka default sejak versi 3.0):

Setiap producer mendapat **Producer ID (PID)** unik dari broker. Setiap batch event mendapat **sequence number**. Jika producer mengirim ulang (retry), broker mendeteksi duplikat berdasarkan `(PID, partition, seq_number)` dan membuang duplikat tersebut.

### 3.3 Log Compaction

Selain time-based retention, Kafka mendukung **log compaction** — mempertahankan hanya **event terbaru per key**:

```
┌─────────────────────────────────────────────────────────────────┐
│                 LOG COMPACTION                                     │
│         (Sumber: kafka.apache.org/documentation#compaction)      │
│                                                                 │
│  Sebelum compaction (topic "profil-pengguna"):                   │
│  Offset: [0]      [1]      [2]      [3]      [4]      [5]       │
│  Key:    user-A   user-B   user-A   user-C   user-B   user-A    │
│  Value:  v1       v1       v2       v1       v2       v3        │
│                                                                 │
│  Setelah compaction:                                             │
│  Offset: [2]      [4]      [3]      [5]                         │
│  Key:    user-A*  user-B*  user-C   user-A* (latest per key)    │
│  *dihapus, yang tersisa hanya yang terbaru                       │
│                                                                 │
│  ✅ Use case sempurna: database changelog, config store          │
│  ✅ Consumer yang baru bergabung bisa mendapat "state terkini"   │
│  ✅ Mirip seperti materialized view dari event history           │
│                                                                 │
│  Konfig: cleanup.policy=compact (default: delete)               │
└─────────────────────────────────────────────────────────────────┘
```

### 3.4 Performa Kafka: Mengapa Sangat Cepat?

| Teknik                       | Penjelasan                                                                     | Dampak                                                |
| ---------------------------- | ------------------------------------------------------------------------------ | ----------------------------------------------------- |
| **Sequential I/O**     | Kafka hanya append ke ujung log — tidak ada random write                      | Throughput disk mendekati throughput jaringan         |
| **Zero-Copy Transfer** | Menggunakan OS sendfile() — data dari disk ke socket tanpa copy via userspace | Menghemat 2 kali copy memori + 2 syscall              |
| **Batching**           | Producer dan consumer bekerja dalam batch, bukan per-event                     | Amortisasi overhead jaringan; lebih efisien CPU       |
| **Compression**        | Support GZIP, Snappy, LZ4, ZSTD per topic/producer                             | Mengurangi bandwidth hingga 4-5×                     |
| **Page Cache**         | Kafka menyerahkan caching ke OS page cache — lazy allocation                  | Consumer baca data langsung dari RAM jika masih fresh |

---

## Bagian 4: Producer & Consumer API (15 menit)

### 4.1 Konfigurasi Kritis Producer

Berikut parameter yang paling berpengaruh terhadap perilaku producer:

```
┌─────────────────────────────────────────────────────────────────┐
│              KONFIGURASI PRODUCER KAFKA 4.2                       │
│    (Sumber: kafka.apache.org/42/documentation/#producerconfigs)  │
│                                                                 │
│  bootstrap.servers = "broker1:9092,broker2:9092"                 │
│      → Daftar broker awal untuk terhubung (tidak perlu semua!)  │
│                                                                 │
│  acks = "all"  (recommended untuk produksi)                     │
│      → 0: kirim tanpa tunggu ACK (fastest, paling berisiko)    │
│      → 1: tunggu ACK dari leader saja                           │
│      → all / -1: tunggu ACK dari SEMUA ISR (paling aman)       │
│                                                                 │
│  enable.idempotence = true  (default Kafka 3.0+, wajib aktif)  │
│      → Guarantees exactly-once delivery per partisi             │
│      → Otomatis set acks=all, max.in.flight.requests.per.       │
│        connection=5, retries=Integer.MAX_VALUE                  │
│                                                                 │
│  linger.ms = 5  (default: 0)                                    │
│      → Tunggu hingga 5ms sebelum kirim batch ke broker          │
│      → Trade-off: latency naik sedikit, throughput naik banyak  │
│                                                                 │
│  batch.size = 16384  (16 KB, default)                           │
│      → Ukuran maksimum batch per partisi sebelum dikirim        │
│      → Naikkan ke 64KB-1MB untuk high-throughput               │
│                                                                 │
│  compression.type = "lz4"  (default: none)                     │
│      → Kompresi per batch — sangat disarankan untuk produksi   │
│      → LZ4: cepat; ZSTD: rasio terbaik (Kafka 2.1+)           │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Konfigurasi Kritis Consumer

```
┌─────────────────────────────────────────────────────────────────┐
│              KONFIGURASI CONSUMER KAFKA 4.2                       │
│    (Sumber: kafka.apache.org/42/documentation/#consumerconfigs)  │
│                                                                 │
│  group.id = "nama-consumer-group"                               │
│      → Wajib jika mau pakai consumer group                      │
│                                                                 │
│  auto.offset.reset = "earliest" | "latest" | "none"            │
│      → earliest: mulai dari awal log jika tidak ada offset saved│
│      → latest: mulai dari event terbaru (default)               │
│      → none: throw exception jika tidak ada offset saved        │
│                                                                 │
│  enable.auto.commit = false  (SANGAT DIREKOMENDASIKAN!)         │
│      → Kontrol eksplisit kapan offset di-commit                 │
│      → Dengan true: offset di-commit tiap auto.commit.interval  │
│        tanpa mempedulikan apakah event sudah diproses!          │
│                                                                 │
│  max.poll.records = 500  (default)                              │
│      → Jumlah record maksimum per satu poll()                   │
│      → Turunkan jika processing per record lambat               │
│                                                                 │
│  session.timeout.ms = 45000  (default: 45 detik)               │
│      → Jika broker tidak terima heartbeat dalam waktu ini,     │
│        consumer dianggap mati → rebalance dipicu               │
│      → Harus < group.max.session.timeout.ms di broker          │
│                                                                 │
│  isolation.level = "read_committed"  (untuk exactly-once)      │
│      → Consumer hanya baca event dari transaksi yang sudah      │
│        di-commit (tidak baca event dari transaksi aborted)      │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 Rebalancing Consumer Group

**Rebalancing** terjadi ketika:

- Consumer baru bergabung ke group
- Consumer aktif mati (session timeout)
- Partisi baru ditambahkan ke topic
- Admin memicu rebalancing secara manual

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUMER GROUP REBALANCING                     │
│                                                                 │
│  STATE AWAL: 2 consumers, 4 partisi                              │
│  C1 → [P0, P1]    C2 → [P2, P3]                                 │
│                                                                 │
│  EVENT: C3 bergabung → GROUP COORDINATOR memicu REBALANCE        │
│                                                                 │
│  1. Semua consumer BERHENTI membaca (STOP THE WORLD!)            │
│  2. Semua consumer melepas partisi mereka                        │
│  3. Group Coordinator membagikan ulang:                          │
│     C1 → [P0, P1]   C2 → [P2]   C3 → [P3]   (atau variasi)    │
│  4. Consumer melanjutkan membaca dari offset terakhir            │
│                                                                 │
│  ⚠️ BIAYA REBALANCING: Selama proses, tidak ada event diproses! │
│                                                                 │
│  SOLUSI Kafka 4.x — COOPERATIVE INCREMENTAL REBALANCING:        │
│  partition.assignment.strategy = CooperativeStickyAssignor       │
│  → Hanya partisi yang BERPINDAH yang di-revoke                  │
│  → Consumer lain terus membaca tanpa interrupt                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Bagian 5: Ekosistem — Kafka Connect & Kafka Streams (15 menit)

### 5.1 Lima API Inti Kafka

Menurut [dokumentasi Kafka](https://kafka.apache.org/intro#kafka-apis), Kafka menyediakan 5 API inti:

| API                         | Fungsi                                                       | Bahasa                     |
| --------------------------- | ------------------------------------------------------------ | -------------------------- |
| **Admin API**         | Kelola dan inspeksi topic, broker, konfigurasi               | Java/Scala + CLI           |
| **Producer API**      | Publish stream of events ke satu atau banyak topic           | Java/Scala + banyak client |
| **Consumer API**      | Subscribe dan proses stream of events dari topic             | Java/Scala + banyak client |
| **Kafka Streams API** | Stream processing — transformasi, agregasi, join, windowing | Java/Scala                 |
| **Kafka Connect API** | Build koneksi import/export data dengan sistem eksternal     | Java/Scala + konfigurasi   |

### 5.2 Kafka Connect

**Kafka Connect** adalah framework untuk membangun dan menjalankan **connector** — komponen yang secara otomatis membaca dari atau menulis ke sistem eksternal.

```
┌─────────────────────────────────────────────────────────────────┐
│                      KAFKA CONNECT                               │
│                                                                 │
│  SOURCE CONNECTORS (menarik data DARI sistem eksternal):        │
│  MySQL / PostgreSQL ──────▶ Debezium CDC ──────▶ Kafka Topic   │
│  S3 / GCS / HDFS   ──────▶ FileSource   ──────▶ Kafka Topic   │
│  Twitter API        ──────▶ Twitter Src  ──────▶ Kafka Topic   │
│  MongoDB            ──────▶ Mongo Src    ──────▶ Kafka Topic   │
│                                                                 │
│  SINK CONNECTORS (mendorong data KE sistem eksternal):          │
│  Kafka Topic ──────▶ Elasticsearch Sink ──────▶ Elasticsearch  │
│  Kafka Topic ──────▶ BigQuery Sink      ──────▶ Google BigQuery│
│  Kafka Topic ──────▶ HDFS Sink          ──────▶ HDFS          │
│  Kafka Topic ──────▶ S3 Sink            ──────▶ Amazon S3      │
│                                                                 │
│  ✅ 200+ connector siap pakai di Confluent Hub                  │
│  ✅ Tidak perlu menulis kode producer/consumer manual           │
│  ✅ Fault-tolerant, scalable, cukup konfigurasi JSON            │
│  ✅ Exactly-once semantics tersedia untuk beberapa connector    │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 Kafka Streams API

**Kafka Streams** adalah library Java/Scala ringan untuk membangun **stateful stream processing application** langsung di atas Kafka — tanpa cluster pemrosesan terpisah (seperti Flink atau Spark).

```
┌─────────────────────────────────────────────────────────────────┐
│                  KAFKA STREAMS — TOPOLOGY                         │
│                                                                 │
│  Input Topic                                                     │
│  "raw-orders" ──▶ SOURCE ──▶ FILTER ──▶ MAP ──▶ AGGREGATE ──▶   │
│                                         (windowed count)         │
│                                                              │   │
│                                              ┌──────────────┘   │
│                                              ▼                   │
│                                         SINK ──▶ Output Topic    │
│                                                "order-summary"   │
│                                                                 │
│  Operasi tersedia (dari dokumentasi Kafka Streams):             │
│  • Stateless: filter, map, flatMap, branch, merge               │
│  • Stateful: count, aggregate, reduce, join (stream-stream,     │
│    stream-table, table-table)                                   │
│  • Windowing: Tumbling, Hopping (Sliding), Session Window       │
│  • Time: Event Time, Processing Time, Ingestion Time            │
│                                                                 │
│  ✅ Tidak butuh cluster terpisah — deploy sebagai JVM process   │
│  ✅ Exactly-once semantics end-to-end                           │
│  ✅ Built-in fault tolerance via Kafka state stores             │
│  ✅ Interactive queries — baca state dari luar aplikasi         │
└─────────────────────────────────────────────────────────────────┘
```

### 5.4 Kafka vs. Messaging Systems Lain

| Aspek                          | RabbitMQ                        | ActiveMQ             | Apache Kafka                                              |
| ------------------------------ | ------------------------------- | -------------------- | --------------------------------------------------------- |
| **Model**                | Push (broker push ke consumer)  | Push                 | Pull (consumer request)                                   |
| **Pesan setelah dibaca** | Dihapus                         | Dihapus              | **Disimpan** (configurable retention)               |
| **Throughput**           | Ratusan ribu/detik              | Ratusan ribu/detik   | **Jutaan/detik**                                    |
| **Ordering**             | Per queue                       | Per queue            | **Per partition** (lebih kuat)                      |
| **Replay**               | ❌ Tidak bisa                   | ❌ Tidak bisa        | **✅ Bisa** (replay dari offset manapun)            |
| **Multi-consumer**       | Fan-out exchange                | Per destination      | **Consumer groups independen**                      |
| **State Management**     | Stateless                       | Stateless            | **Stateful** (offset per group)                     |
| **Ekosistem**            | Plugin                          | Plugin               | **Connect + Streams built-in**                      |
| **Cocok untuk**          | Task queue, RPC, job scheduling | Messaging enterprise | **Event streaming, data pipeline, log aggregation** |

---

# 🧠 KUIS ONLINE

> ⏱️ **Estimasi waktu:** 20 menit
> 📋 **Kerjakan kuis SETELAH membaca semua materi teori (Bagian 1–5).**

Akses kuis melalui **LMS** (tautan disediakan di halaman pertemuan 8). Kuis terdiri dari **25 soal** — kombinasi pilihan ganda dan benar/salah.

**Ketentuan:**

- ⏰ Batas waktu: 20 menit sejak kuis dibuka
- 🔄 Dapat dikerjakan lebih dari 1 kali (nilai diambil yang terbaik)
- 📊 Nilai langsung muncul setelah submit
- 📅 Deadline: sesuai jadwal di LMS

> 💡 **Tips:** Fokus pada konsep partisi, offset, consumer group, dan delivery semantics — bagian yang paling sering keliru.

---

# 🛠️ SETUP: MENJALANKAN KAFKA VIA DOCKER

> ⏱️ **Estimasi:** 10 menit
> **Prasyarat:** Docker Desktop terinstall dan berjalan. Alokasi minimal **2 GB RAM** ke Docker.

### Langkah 1: Buat Folder Lab

```bash
mkdir kafka-lab && cd kafka-lab
```

### Langkah 2: Buat `docker-compose.yml`

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka-broker
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_LOG_RETENTION_HOURS: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```

> ℹ️ `KAFKA_LOG_RETENTION_HOURS: 1` — agar storage tidak penuh saat lab. Di produksi, default 168 jam (7 hari).
> ℹ️ `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — best practice: topic harus dibuat eksplisit.

### Langkah 3: Jalankan Kafka

```bash
docker compose up -d

# Tunggu ~15 detik lalu cek status
docker compose ps

# Verifikasi dengan cek log
docker compose logs kafka | grep -i "started"
```

> ✅ **Expected:** Container `kafka-broker` berstatus `running` dan log menampilkan `Kafka Server started`.

### Langkah 4: Install Library Python

```bash
pip install kafka-python
```

### Cara Mematikan Kafka

```bash
docker compose down
```

---

# 🔧 HANDS-ON LABS (Mandiri)

> ⏱️ **Total estimasi lab:** 75 menit
> **Pastikan Kafka sudah berjalan sebelum memulai lab!**

---

## 🔧 Lab 1: Eksplorasi CLI & Topic Design (25 menit)

### Tujuan

Memahami internals Kafka melalui CLI: membuat topic dengan desain yang tepat, mengamati distribusi partisi, dan menganalisis consumer lag.

### Task 1.1: Membuat Topic dengan Desain Partisi yang Diperhitungkan

```bash
docker exec -it kafka-broker bash

# Topic 1: Data GPS angkutan umum — 6 rute, butuh paralel per rute
kafka-topics.sh --create \
  --topic gps-angkot \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=3600000 \
  --config compression.type=lz4 \
  --bootstrap-server localhost:9092

# Topic 2: Log error sistem — tidak butuh ordering, throughput tinggi
kafka-topics.sh --create \
  --topic error-log \
  --partitions 4 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Topic 3: Profil pengguna — pakai log compaction, bukan time deletion
kafka-topics.sh --create \
  --topic profil-pengguna \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1 \
  --bootstrap-server localhost:9092
```

> 📝 **Perhatikan:** Topic 3 menggunakan `cleanup.policy=compact` (log compaction) bukan `delete` (default). Ini berarti Kafka akan mempertahankan nilai terbaru per key, cocok untuk menyimpan *state* entitas.

### Task 1.2: Inspeksi Detail Topic

```bash
# Lihat konfigurasi lengkap topic gps-angkot
kafka-topics.sh \
  --describe --topic gps-angkot \
  --bootstrap-server localhost:9092

# Lihat semua topic (termasuk internal topic Kafka)
kafka-topics.sh --list --bootstrap-server localhost:9092
```

**📝 Pertanyaan Refleksi:**

1. Berapa partisi topic `gps-angkot`? Mengapa angka tersebut dipilih?
2. Apa yang terjadi jika kita hanya punya 2 consumer untuk 6 partisi `gps-angkot`?
3. Apa isi kolom `Leader` dan `Replicas` pada output `--describe`?

### Task 1.3: Kirim Event Bermakna Lewat CLI

```bash
# Kirim data GPS angkot dengan KEY = nomor rute (jamin ordering per rute)
kafka-console-producer.sh \
  --topic gps-angkot \
  --property "key.separator=|" \
  --property "parse.key=true" \
  --bootstrap-server localhost:9092
```

Ketik data GPS berikut (tekan Enter setelah setiap baris):

```
rute-A|{"rute":"A","lat":-7.2575,"lon":112.7521,"speed":35,"penumpang":12,"ts":"2026-04-19T08:00:00"}
rute-B|{"rute":"B","lat":-7.2612,"lon":112.7488,"speed":22,"penumpang":8,"ts":"2026-04-19T08:00:01"}
rute-A|{"rute":"A","lat":-7.2580,"lon":112.7530,"speed":40,"penumpang":13,"ts":"2026-04-19T08:00:05"}
rute-C|{"rute":"C","lat":-7.2590,"lon":112.7510,"speed":18,"penumpang":20,"ts":"2026-04-19T08:00:06"}
rute-A|{"rute":"A","lat":-7.2588,"lon":112.7545,"speed":38,"penumpang":13,"ts":"2026-04-19T08:00:10"}
rute-B|{"rute":"B","lat":-7.2625,"lon":112.7495,"speed":25,"penumpang":7,"ts":"2026-04-19T08:00:11"}
```

Lalu matikan producer dengan `Ctrl+C`.

### Task 1.4: Baca & Analisis Distribusi Partisi

```bash
# Baca semua pesan, tampilkan key, partisi, dan offset
kafka-console-consumer.sh \
  --topic gps-angkot \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property key.separator=" | " \
  --bootstrap-server localhost:9092 \
  --timeout-ms 5000
```

**📝 Amati dan catat:**

- Apakah semua event dari `rute-A` masuk ke partisi yang sama?
- Apakah urutan event per rute terjaga?
- Berapakah offset masing-masing partisi yang terisi?

### Task 1.5: Simulasi Consumer Lag

```bash
# Di terminal 1: jalankan consumer lambat (group = "monitoring-team")
kafka-console-consumer.sh \
  --topic gps-angkot \
  --group monitoring-team \
  --from-beginning \
  --bootstrap-server localhost:9092

# Di terminal 2 (di dalam container juga): cek consumer lag
docker exec -it kafka-broker \
  kafka-consumer-groups.sh \
    --describe \
    --group monitoring-team \
    --bootstrap-server localhost:9092
```

**📝 Analisis output `--describe`:**

- Kolom `CURRENT-OFFSET`: offset terakhir yang sudah di-commit consumer
- Kolom `LOG-END-OFFSET`: offset terbaru di broker
- Kolom `LAG`: selisihnya — berapa event yang BELUM dibaca!

> 💡 Di produksi, **LAG yang terus meningkat** adalah tanda serius bahwa consumer tidak mampu mengikuti kecepatan producer. Tim harus segera scale out consumer atau optimasi logika pemrosesan.

---

## 🔧 Lab 2: Pipeline Cuaca IoT Real-Time (30 menit)

### Tujuan

Membangun pipeline streaming lengkap: simulasi jaringan sensor cuaca 5 kota, multi-consumer dengan fungsi berbeda, dan agregasi sederhana berbasis waktu.

### Setup Topic

```bash
docker exec -it kafka-broker \
  kafka-topics.sh --create \
    --topic cuaca-sensor \
    --partitions 5 \
    --replication-factor 1 \
    --config retention.ms=3600000 \
    --bootstrap-server localhost:9092
```

Keluar dari container: `exit`

### File 1: `producer_cuaca.py`

```python
"""
producer_cuaca.py — Simulasi jaringan sensor cuaca 5 kota Jawa Timur
Setiap kota mengirim pembacaan sensor setiap ~1 detik.
Key = kode kota → menjamin semua data kota yang sama ke partisi yang sama.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

KOTA = {
    "SBY": {"nama": "Surabaya",  "lat": -7.2575, "lon": 112.7521, "base_temp": 30},
    "MLG": {"nama": "Malang",    "lat": -7.9797, "lon": 112.6304, "base_temp": 24},
    "MDN": {"nama": "Madiun",    "lat": -7.6299, "lon": 111.5238, "base_temp": 27},
    "JMB": {"nama": "Jember",    "lat": -8.1845, "lon": 113.6683, "base_temp": 28},
    "BNY": {"nama": "Banyuwangi","lat": -8.2191, "lon": 114.3691, "base_temp": 29},
}

KONDISI = ["Cerah", "Berawan", "Mendung", "Hujan Ringan", "Hujan Lebat"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    enable_idempotence=True,
    linger_ms=10,
    compression_type="lz4",
)

print("🌤️  Sensor Cuaca Jawa Timur — Producer Aktif")
print("   Mengirim data setiap ~0.2 detik, Ctrl+C untuk berhenti\n")

count = 0
try:
    while True:
        for kode, info in KOTA.items():
            # Fluktuasi suhu berbasis sinusoidal + noise
            jam = datetime.now().hour
            siklus_harian = 3 * (1 - abs(jam - 14) / 14)  # puncak jam 14
            suhu = round(info["base_temp"] + siklus_harian + random.uniform(-1.5, 1.5), 1)
            kelembaban = round(random.uniform(55, 95), 1)
            kecepatan_angin = round(random.uniform(0, 40), 1)
            tekanan = round(random.uniform(1008, 1020), 1)

            # Cuaca lebih mudah hujan jika kelembaban tinggi
            if kelembaban > 85:
                kondisi = random.choice(["Hujan Ringan", "Hujan Lebat", "Mendung"])
            elif kelembaban > 70:
                kondisi = random.choice(["Berawan", "Mendung", "Cerah"])
            else:
                kondisi = random.choice(["Cerah", "Berawan"])

            reading = {
                "kode_kota": kode,
                "nama_kota": info["nama"],
                "latitude": info["lat"],
                "longitude": info["lon"],
                "suhu_celsius": suhu,
                "kelembaban_persen": kelembaban,
                "kecepatan_angin_kmh": kecepatan_angin,
                "tekanan_hpa": tekanan,
                "kondisi": kondisi,
                "timestamp": datetime.now().isoformat(),
                "sensor_id": f"BMKG-{kode}-001",
            }

            producer.send(topic="cuaca-sensor", key=kode, value=reading)
            count += 1

        if count % 25 == 0:
            producer.flush()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {count} pembacaan terkirim")

        time.sleep(0.2)

except KeyboardInterrupt:
    print(f"\n✋ Producer dihentikan. Total terkirim: {count} pembacaan sensor")
finally:
    producer.flush()
    producer.close()
```

### File 2: `consumer_dashboard.py`

```python
"""
consumer_dashboard.py — Dashboard ringkasan cuaca real-time
Consumer Group: "dashboard-publik"
Tampilkan suhu terkini per kota dan peringatan cuaca ekstrem.
"""

import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "cuaca-sensor",
    bootstrap_servers=["localhost:9092"],
    group_id="dashboard-publik",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

# State: suhu terkini per kota
suhu_terkini = {}
alert_count = 0

def cetak_dashboard():
    print("\n" + "═" * 55)
    print("  🌡️  DASHBOARD CUACA JAWA TIMUR — REAL-TIME  ")
    print("═" * 55)
    for kode in sorted(suhu_terkini):
        d = suhu_terkini[kode]
        icon = "⛈️" if "Hujan" in d["kondisi"] else ("🌥️" if "Mendung" in d["kondisi"] else "☀️")
        print(
            f"  {icon} {d['nama']:12s} | "
            f"{d['suhu']:5.1f}°C | "
            f"💧{d['lembab']:5.1f}% | "
            f"🌬️{d['angin']:5.1f}km/h | "
            f"{d['kondisi']}"
        )
    print("═" * 55 + "\n")

print("📺 Dashboard Cuaca Aktif — menunggu data sensor...\n")

n = 0
try:
    for msg in consumer:
        d = msg.value
        kode = d["kode_kota"]

        suhu_terkini[kode] = {
            "nama": d["nama_kota"],
            "suhu": d["suhu_celsius"],
            "lembab": d["kelembaban_persen"],
            "angin": d["kecepatan_angin_kmh"],
            "kondisi": d["kondisi"],
        }

        # Alert cuaca ekstrem
        if d["suhu_celsius"] >= 35:
            alert_count += 1
            print(f"🔴 HEAT ALERT #{alert_count}: {d['nama_kota']} → {d['suhu_celsius']}°C!")
        if d["kecepatan_angin_kmh"] >= 35:
            alert_count += 1
            print(f"🌪️  WIND ALERT #{alert_count}: {d['nama_kota']} → {d['kecepatan_angin_kmh']} km/h!")

        n += 1
        if n % 10 == 0:
            cetak_dashboard()

except KeyboardInterrupt:
    print(f"\n✋ Dashboard dihentikan. Total alert: {alert_count}")
finally:
    consumer.close()
```

### File 3: `consumer_analitik.py`

```python
"""
consumer_analitik.py — Agregasi statistik cuaca per kota
Consumer Group: "tim-analitik-bmkg"
BERBEDA dari dashboard-publik → membaca pesan yang sama dari awal.
Hitung statistik rolling: min, max, avg per kota dari semua data.
"""

import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "cuaca-sensor",
    bootstrap_servers=["localhost:9092"],
    group_id="tim-analitik-bmkg",
    auto_offset_reset="earliest",          # Baca dari awal!
    enable_auto_commit=False,              # Manual commit untuk kontrol penuh
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    max_poll_records=100,
)

stats = defaultdict(lambda: {
    "count": 0, "sum_suhu": 0, "min_suhu": 999, "max_suhu": -999,
    "sum_lembab": 0, "min_lembab": 999, "max_lembab": -999,
    "kondisi_freq": defaultdict(int),
})

print("📊 Analitik BMKG — Membaca semua data historis...\n")

n = 0
try:
    for msg in consumer:
        d = msg.value
        k = d["kode_kota"]
        s = stats[k]

        s["count"] += 1
        s["sum_suhu"] += d["suhu_celsius"]
        s["min_suhu"] = min(s["min_suhu"], d["suhu_celsius"])
        s["max_suhu"] = max(s["max_suhu"], d["suhu_celsius"])
        s["sum_lembab"] += d["kelembaban_persen"]
        s["min_lembab"] = min(s["min_lembab"], d["kelembaban_persen"])
        s["max_lembab"] = max(s["max_lembab"], d["kelembaban_persen"])
        s["kondisi_freq"][d["kondisi"]] += 1

        n += 1

        # Commit manual setiap 50 pesan
        if n % 50 == 0:
            consumer.commit()

        # Laporan setiap 100 pesan
        if n % 100 == 0:
            print(f"\n📈 LAPORAN STATISTIK (total {n} pembacaan)\n" + "─" * 60)
            for kd in sorted(stats):
                sd = stats[kd]
                if sd["count"] == 0:
                    continue
                avg_suhu = sd["sum_suhu"] / sd["count"]
                avg_lembab = sd["sum_lembab"] / sd["count"]
                kondisi_dominan = max(sd["kondisi_freq"], key=sd["kondisi_freq"].get)
                print(
                    f"  {kd} | N={sd['count']:5d} | "
                    f"Suhu: {sd['min_suhu']:.1f}~{sd['max_suhu']:.1f}°C "
                    f"(avg={avg_suhu:.1f}) | "
                    f"Dominan: {kondisi_dominan}"
                )

except KeyboardInterrupt:
    consumer.commit()  # Commit terakhir sebelum berhenti
    print(f"\n✋ Analitik dihentikan (total {n} pembacaan diproses)")
finally:
    consumer.close()
```

### Cara Menjalankan Lab 2

Buka **3 terminal terpisah**:

```bash
# Terminal 1: Producer cuaca
python producer_cuaca.py

# Terminal 2: Dashboard real-time
python consumer_dashboard.py

# Terminal 3: Analitik historis
python consumer_analitik.py
```

**📝 Pertanyaan Refleksi:**

1. Mengapa `consumer_analitik.py` menggunakan `auto_offset_reset="earliest"` sedangkan dashboard menggunakan `"latest"`?
2. Apa yang terjadi jika kamu menjalankan **2 instance** `consumer_dashboard.py` secara bersamaan? Bagaimana partisi dibagi?
3. Coba hentikan `consumer_analitik.py`, tunggu 30 detik, lalu jalankan ulang. Apakah ia melanjutkan dari tempat ia berhenti atau mulai dari awal? Mengapa?

---

## 🔧 Lab 3: Exactly-Once & Idempotent Producer (20 menit)

### Tujuan

Memahami perbedaan delivery semantics secara praktis — mensimulasikan kondisi retry dan mengamati dampaknya pada at-least-once vs exactly-once.

### Setup

```bash
docker exec -it kafka-broker \
  kafka-topics.sh --create \
    --topic transfer-bank \
    --partitions 1 \
    --replication-factor 1 \
    --bootstrap-server localhost:9092
exit
```

### File: `producer_transfer.py`

```python
"""
producer_transfer.py
Simulasi producer transfer bank dengan dua mode:
  Mode A: at-least-once (bisa duplikat)
  Mode B: exactly-once (idempotent producer)

Jalankan dua kali dan bandingkan jumlah record di Kafka!
"""

import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

MODE = sys.argv[1] if len(sys.argv) > 1 else "at-least-once"

TRANSFERS = [
    {"id": "TXF-001", "dari": "ACC-1001", "ke": "ACC-2001", "nominal": 500_000},
    {"id": "TXF-002", "dari": "ACC-1002", "ke": "ACC-2002", "nominal": 1_200_000},
    {"id": "TXF-003", "dari": "ACC-1003", "ke": "ACC-2003", "nominal": 75_000},
    {"id": "TXF-004", "dari": "ACC-1004", "ke": "ACC-2004", "nominal": 3_000_000},
    {"id": "TXF-005", "dari": "ACC-1005", "ke": "ACC-2005", "nominal": 250_000},
]

if MODE == "at-least-once":
    print("⚠️  MODE: AT-LEAST-ONCE (retries aktif, duplikat MUNGKIN terjadi)")
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=False,   # Idempotence OFF → duplikat bisa terjadi!
    )
elif MODE == "exactly-once":
    print("✅  MODE: EXACTLY-ONCE (idempotent producer, duplikat TIDAK mungkin)")
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        enable_idempotence=True,    # Kafka assign PID + sequence number
        max_in_flight_requests_per_connection=5,
    )
else:
    print(f"Unknown mode: {MODE}. Gunakan 'at-least-once' atau 'exactly-once'")
    sys.exit(1)

print(f"\n💸 Mengirim {len(TRANSFERS)} transfer bank...\n")

for tx in TRANSFERS:
    try:
        # Simulasi pengiriman pertama
        future = producer.send(
            topic="transfer-bank",
            key=tx["id"],
            value={**tx, "attempt": 1}
        )
        future.get(timeout=5)
        print(f"  ✅ SENT (attempt 1): {tx['id']} — Rp{tx['nominal']:,}")

        # Simulasi RETRY paksa (simulasi network blip)
        # Dalam skenario nyata, ini terjadi otomatis karena timeout/error
        time.sleep(0.1)
        future2 = producer.send(
            topic="transfer-bank",
            key=tx["id"],
            value={**tx, "attempt": 2, "note": "RETRY setelah network blip"}
        )
        future2.get(timeout=5)
        print(f"  🔄 RETRY (attempt 2): {tx['id']}")

    except KafkaError as e:
        print(f"  ❌ Error: {e}")

producer.flush()
producer.close()
print(f"\n✅ Selesai. Sekarang cek consumer — berapa record yang ada?")
```

### File: `consumer_hitung_transfer.py`

```python
"""
consumer_hitung_transfer.py
Hitung jumlah dan total nominal transfer yang diterima.
Jika ada duplikat: jumlah > 5, atau ada transfer ID yang muncul 2 kali.
"""

import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "transfer-bank",
    bootstrap_servers=["localhost:9092"],
    group_id=None,                         # None = tanpa group, pakai assign manual
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    consumer_timeout_ms=3000,
)

# Assign manual ke semua partisi
from kafka import TopicPartition
consumer.assign([TopicPartition("transfer-bank", 0)])
consumer.seek_to_beginning()

records = []
for msg in consumer:
    records.append(msg.value)

consumer.close()

# Analisis
print(f"\n📊 HASIL ANALISIS TRANSFER BANK")
print(f"{'='*50}")
print(f"  Total record diterima  : {len(records)}")
print(f"  Seharusnya (5 transfer): 5 record\n")

# Cek duplikat
id_counts = defaultdict(int)
total_nominal = 0
for r in records:
    id_counts[r["id"]] += 1
    total_nominal += r["nominal"]

print(f"  {'Transfer ID':15s} | {'Muncul':7s} | {'Nominal':>15s}")
print(f"  {'-'*45}")
for tid, cnt in sorted(id_counts.items()):
    status = "✅ OK" if cnt == 1 else f"⚠️  DUPLIKAT ({cnt}x)!"
    nominal = next(r["nominal"] for r in records if r["id"] == tid)
    print(f"  {tid:15s} | {cnt:7d} | Rp{nominal:>12,.0f} | {status}")

print(f"\n  Total nominal di broker : Rp{total_nominal:>12,.0f}")
print(f"  (Jika duplikat, nominal 'tercatat' lebih besar dari sebenarnya!)")
```

### Cara Menjalankan Lab 3

```bash
# Langkah 1: Reset topic (hapus data lama)
docker exec -it kafka-broker \
  kafka-topics.sh --delete --topic transfer-bank --bootstrap-server localhost:9092
docker exec -it kafka-broker \
  kafka-topics.sh --create --topic transfer-bank --partitions 1 \
  --replication-factor 1 --bootstrap-server localhost:9092

# Langkah 2: Jalankan producer at-least-once
python producer_transfer.py at-least-once

# Langkah 3: Hitung record
python consumer_hitung_transfer.py

# Langkah 4: Reset lagi
# (ulangi langkah 1)

# Langkah 5: Jalankan producer exactly-once
python producer_transfer.py exactly-once

# Langkah 6: Hitung record lagi — bandingkan!
python consumer_hitung_transfer.py
```

**📝 Pertanyaan Refleksi:**

1. Berapa record yang muncul di mode `at-least-once` vs `exactly-once`?
2. Dalam sistem perbankan nyata, apa konsekuensi nyata dari duplikat transfer?
3. Kapan kamu **tidak perlu** exactly-once? (Hint: pikirkan trade-off antara kompleksitas dan kebutuhan bisnis)

---

## Rangkuman Pertemuan 8

```
┌─────────────────────────────────────────────────────────────────┐
│                   RANGKUMAN PERTEMUAN 8                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  📖 MATERI INTI                                                  │
│  • Event = key + value + timestamp + headers                    │
│  • Topic → Partisi → Offset: unit penyimpanan Kafka             │
│  • KEY → partisi konsisten; ordering dijamin PER PARTISI        │
│  • Consumer Group: independen, bisa replay, offset per group    │
│  • KRaft: metadata tanpa ZooKeeper (Kafka 4.x, default)         │
│                                                                 │
│  🔑 DESAIN INTERNAL                                              │
│  • Distributed commit log: append-only, immutable, sequential   │
│  • Performance: zero-copy, batching, compression                │
│  • Replication: ISR, leader election, fault tolerance           │
│  • Log compaction: state terkini per key (vs time-based delete) │
│                                                                 │
│  📦 DELIVERY SEMANTICS                                           │
│  • At-most-once : cepat, bisa hilang                           │
│  • At-least-once: aman, bisa duplikat (PALING UMUM)            │
│  • Exactly-once : idempotent producer + transaction API         │
│                                                                 │
│  🔌 EKOSISTEM                                                    │
│  • Producer/Consumer API: 5 language client resmi + Python dll  │
│  • Kafka Connect: 200+ connector tanpa kode                     │
│  • Kafka Streams: stateful stream processing embedded           │
│                                                                 │
│  🎯 KEY INSIGHT                                                  │
│  Kafka BUKAN sekadar message queue — ini DISTRIBUTED EVENT LOG. │
│  Data TIDAK dihapus setelah dibaca. Siapa pun bisa replay       │
│  ke mana saja dari offset tertentu → fondasi arsitektur modern. │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

# 📋 TUGAS KELOMPOK BIG PROJECT -> [ets-big-data-system](ets-big-data-system.md)

## Referensi Pertemuan 8

1. Apache Kafka 4.2 — Official Documentation. https://kafka.apache.org/42/documentation/
2. Apache Kafka — Introduction. https://kafka.apache.org/intro
3. Apache Kafka — Use Cases. https://kafka.apache.org/uses
4. Apache Kafka — Design: Persistence, Efficiency, Producer, Consumer. https://kafka.apache.org/42/documentation/#design
5. Apache Kafka — Delivery Semantics. https://kafka.apache.org/42/documentation/#semantics
6. Apache Kafka — Log Compaction. https://kafka.apache.org/42/documentation/#compaction
7. Narkhede, N., Shapira, G., & Palino, T. (2017). *Kafka: The Definitive Guide*. O'Reilly Media.
8. Kreps, J. (2013). *The Log: What every software engineer should know about real-time data's unifying abstraction*. LinkedIn Engineering Blog. https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
9. KIP-833: Mark KRaft as Production Ready. https://cwiki.apache.org/confluence/display/KAFKA/KIP-833
10. Stopford, B. (2018). *Designing Event-Driven Systems*. Confluent (Free). https://www.confluent.io/designing-event-driven-systems