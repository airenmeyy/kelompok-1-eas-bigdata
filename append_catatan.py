with open("catatan_terakhir.md", "a", encoding="utf-8") as f:
    f.write("""
---

## FASE 8: DASHBOARD REFINEMENT & API UPDATE

### 8.1 Perbaikan Bug Dashboard (UI & Logika Sinkronisasi)
Beberapa isu kritis pada antarmuka *dashboard* (`app.js`) telah diperbaiki untuk meningkatkan stabilitas dan akurasi pelaporan:
1. **Perbaikan UI Freeze & DOM Error:** Menambahkan pengecekan *null safety* pada elemen `#kpi-total-kec h3` untuk mencegah berhentinya proses inisialisasi (`initDashboard()`) yang sebelumnya membuat *dashboard* tidak bisa diklik.
2. **Perbaikan Data Detail Kecamatan:** Memperbaiki inkonsistensi variabel parameter pada fungsi `selectKecamatan()` yang sebelumnya mencegah pemuatan detail Kriminalitas dan Kesehatan pada *sidebar*.
3. **Penyempurnaan Tampilan Berita Anomali (Deduplikasi Teks):** Memodifikasi fungsi `populateNewsPortal()` agar tidak menampilkan teks yang terulang (double) apabila nilai kolom `deskripsi_mentah` sama persis dengan `judul`. Digantikan dengan *snippet* deskripsi generik dan bersih.
4. **Koreksi Log Pipeline (Deduplikasi Array):** Memperbaiki perhitungan duplikat di `fetchJSONAndUpdateUI()`. Kini *array* berita JSON disaring terlebih dahulu sebelum dihitung terhadap status `seenNewsIds`, sehingga log *Riwayat Pipeline* menunjukkan jumlah berita duplikat secara akurat (sinkron dengan jumlah aktual kartu berita yang tampil).

### 8.2 Konfigurasi Git Ignore
1. **Eksklusi Node Modules:** Menambahkan direktori `node_modules/` ke dalam file `.gitignore` agar ribuan paket *dependencies* lokal tidak ikut *ter-push* dan membebani repositori GitHub.

### 8.3 Ekspansi Ekstraksi RSS API (`00_ingestion_api.py`)
Melakukan penambahan puluhan kata kunci baru yang spesifik pada *query* penarikan RSS Google News agar selaras dengan data turunan metrik dasar:
1. **Kriminalitas (Penyelarasan Tipe Kejahatan):** Ditambahkan kata kunci kejahatan spesifik, antara lain: `penipuan`, `"perbuatan curang"`, `"curanmor r2"`, `"pencurian motor"`, `"pencurian dengan pemberatan"`, `"pencurian biasa"`, `pengeroyokan`, `"mengakibatkan orang mati"`, `judi`, `"penganiayaan berat"`, dan `anirat`.
2. **Kesehatan (Metrik IR, HFR, dan Risiko Lingkungan):** Ditambahkan perluasan kata kunci yang tidak hanya berfokus pada penyakit, namun mencakup:
   - *Incidence Rate (Ancaman)*: `wabah`, `klb`, `infeksi`, `dbd`, `"demam berdarah"`, `diare`, `muntaber`, `tipes`, `ispa`, `pneumonia`, `tbc`, `keracunan`
   - *Health Facility Ratio (Kapasitas)*: `puskesmas`, `"rumah sakit"`, `faskes`, `"pelayanan kesehatan"`
   - *Environmental Health Risk (Risiko Lingkungan)*: `sanitasi`, `pencemaran`, `polusi`, `limbah`, `gizi`, `stunting`
""")
