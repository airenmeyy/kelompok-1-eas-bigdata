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
├── 01_bronze.py          # Skrip PySpark untuk menyalin data ke lapisan Bronze
├── raw_data/             # Folder untuk menyimpan data mentah (CSV, JSON, dll)
└── lakehouse/
    └── bronze/           # Folder penyimpanan data dalam format Delta Lake
```

## Persyaratan (Requirements)

Pastikan Anda memiliki Python terinstal di sistem Anda. Instal dependensi yang diperlukan dengan menjalankan perintah berikut:

```bash
pip install -r requirements.txt
```

## Cara Menjalankan Program (Bronze Layer)

1. Pastikan data mentah telah diletakkan ke dalam folder `raw_data/`.
2. Jalankan skrip `01_bronze.py` untuk membaca data mentah dan menyimpannya ke lapisan Bronze dalam format Delta Lake:

```bash
python 01_bronze.py
```

Skrip ini akan secara otomatis membaca file di `raw_data/` dan menyimpannya di `lakehouse/bronze/` dengan format Delta tanpa melakukan modifikasi pada data aslinya.

## Screenshot Hasil Kerja

*(Tambahkan screenshot hasil eksekusi program atau visualisasi data dari anggota lain di sini)*

- **Contoh Hasil Data di Bronze Layer:**
  `[Screenshot folder lakehouse/bronze atau tampilan data Delta Lake]`

- **Contoh Visualisasi Dashboard GIS:**
  `[Screenshot Dashboard Leaflet/OpenStreetMap]`

---

