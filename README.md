# KECAMATRA: Kecamatan Metrics & Anomaly Tracking

KECAMATRA adalah instrumen pendukung keputusan berbasis data (Data-Driven Policy) untuk memitigasi kerugian material dan melakukan evaluasi risiko laten geologis, infrastruktur, dan keamanan di tingkat mikro (kecamatan) di Surabaya.

## Pendahuluan

Proyek ini menggunakan arsitektur Medallion (Data Lakehouse) untuk memproses Big Data hibrida yang menstrukturkan data mentah tekstual dari lapisan Bronze, Silver, hingga agregasi spasial di lapisan Gold.

Repositori ini berfokus pada **Bronze Layer**, di mana data mentah dari berbagai sumber (API RSS Media Digital, Web Scraping, Open Data Surabaya) diserap dan disimpan dalam format Delta Lake tanpa modifikasi data.

## Struktur Direktori

```text
.
├── .gitignore
├── README.md
├── requirements.txt
├── 00_ingestion_api.py  # Skrip untuk ingest dataset kesehatan & keamanan ke raw_data
├── 01_bronze.py          # Skrip PySpark untuk menyalin data ke lapisan Bronze
├── raw_data/             # Folder untuk menyimpan data mentah (CSV, JSON, XLSX, dll)
│   ├── kesehatan/        # Dataset kesehatan yang distandarkan
│   ├── keamanan/         # Dataset keamanan yang distandarkan
│   └── static/           # Dataset statis atau sumber lama
└── lakehouse/
    └── bronze/           # Folder penyimpanan data dalam format Delta Lake
```

## Persyaratan (Requirements)

Pastikan Anda memiliki Python terinstal di sistem Anda. Instal dependensi yang diperlukan dengan menjalankan perintah berikut:

```bash
pip install -r requirements.txt
```

## Cara Menjalankan Program (Ingestion API)

Sebelum memproses data ke Bronze Layer, jalankan skrip ingest untuk menempatkan dataset kesehatan dan keamanan ke struktur `raw_data/` yang benar.

Contoh untuk dataset BPJS(Keamanan) dan CKAN(Kesehatan):

```bash
python "00_ingestion_api.py"
```

Output default:

- `raw_data/kesehatan/kesehatan_fasilitas_ckan_standard.csv`
- `raw_data/keamanan/keamanan_kriminalitas_bps.csv`
- `raw_data/keamanan/keamanan_kriminalitas_bps_regency.csv`
- `raw_data/keamanan/keamanan_kriminalitas_bps_municipality.csv`


## Cara Menjalankan Program (Bronze Layer)

1. Pastikan data mentah telah diletakkan ke dalam folder `raw_data/`, misalnya `raw_data/kesehatan/`, `raw_data/keamanan/`, atau `raw_data/static/`.
2. Jalankan skrip `01_bronze.py` untuk membaca data mentah dan menyimpannya ke lapisan Bronze dalam format Delta Lake:

```bash
python 01_bronze.py
```

Skrip ini akan:

- membaca semua file `*.csv`, `*.json`, dan `*.parquet` di dalam `raw_data/` secara rekursif,
- menjaga struktur data mentah tanpa modifikasi,
- menyimpan setiap sumber ke `lakehouse/bronze/<nama_file>` dalam format Delta.

Contoh file yang dibaca:

- `raw_data/kesehatan/kesehatan_fasilitas_ckan_standard.csv`
- `raw_data/keamanan/keamanan_kriminalitas_bps.csv`
- `raw_data/keamanan/keamanan_kriminalitas_bps_regency.csv`
- `raw_data/keamanan/keamanan_kriminalitas_bps_municipality.csv`
- file sumber CKAN penyakit di `raw_data/kesehatan/` (CSV tanpa ekstensi yang diunduh secara otomatis)

## Screenshot Hasil Kerja

_(Tambahkan screenshot hasil eksekusi program atau visualisasi data dari anggota lain di sini)_

- **Contoh Hasil Data di Bronze Layer:**
  `[Screenshot folder lakehouse/bronze atau tampilan data Delta Lake]`

- **Contoh Visualisasi Dashboard GIS:**
  `[Screenshot Dashboard Leaflet/OpenStreetMap]`

---
