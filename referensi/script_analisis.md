# 📝 Panduan Pembahasan & Penjelasan Faktual CPMK-4: Teknik Analisis & Kualitas Output
**Naskah Detail & Dokumen Pembuktian untuk Sidang Projek Akhir KECAMATRAS**

Dokumen ini disusun untuk membantu **Arya Bisma Putra Refman (@aryarefman)** mempresentasikan bagian **CPMK-4 (Teknik Analisis & Kualitas Output - Bobot 25%)** secara mendalam, berbasis fakta kode, dan terstruktur guna meyakinkan dosen penguji bahwa projek Anda layak mendapatkan nilai tertinggi (Full Score).

---

## 🔍 POIN 1: Penerapan ≥ 2 Teknik Analisis Lanjutan

Dalam projek KECAMATRAS, kita tidak hanya menampilkan data mentah, tetapi menerapkan **dua teknik analisis tingkat lanjut** yang terintegrasi langsung dalam pengolahan data terdistribusi:

### A. Teknik 1: NLP & Unsupervised Machine Learning (LDA Topic Modeling)
*   **Lokasi File Kode:** [03_gold.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/03_gold.py#L91-L156) pada kelas `GoldLayerProcessor` fungsi `run_ml_pipeline()`.

#### 📌 Penjelasan Mendalam untuk Presentasi:
1.  **Mengapa Harus ML Unsupervised?**
    Data berita yang kita ingest dari Google News RSS Feed merupakan data teks tidak terstruktur (*unstructured text data*) yang **tidak memiliki label kategori**. Kita tidak bisa menggunakan model supervised (seperti Naive Bayes atau SVM) karena tidak ada *ground truth* (kunci jawaban). Oleh karena itu, kita memilih metode *unsupervised learning* menggunakan **Latent Dirichlet Allocation (LDA)** untuk membedah artikel dan mengelompokkannya ke dalam topik secara otomatis.
2.  **Langkah Pemrosesan Bahasa Alami (NLP Pipeline) di Spark:**
    *   **Tokenisasi:** Menggunakan `Tokenizer` untuk memecah kalimat berita menjadi potongan kata (token) individual agar bisa dianalisis frekuensinya.
    *   **StopWords Removal:** Menggunakan `StopWordsRemover` dengan daftar stopwords kustom bahasa Indonesia yang didefinisikan pada baris 29-39 (termasuk kata-kata sampah lokal seperti *"surabaya"*, *"kota"*, *"kecamatan"*, *"jawa"*, *"timur"*). Kata-kata ini dibuang karena tidak membawa informasi semantik mengenai kerawanan kriminal atau risiko kesehatan.
    *   **TF-IDF (Term Frequency-Inverse Document Frequency):**
        *   `CountVectorizer` menghitung seberapa sering sebuah kata muncul dalam satu berita.
        *   `IDF` (Inverse Document Frequency) meminimalkan pengaruh kata-kata yang terlalu sering muncul di semua berita (misalnya kata *"melaporkan"*, *"kejadian"*) dan memaksimalkan bobot kata-kata unik yang membawa makna khusus (seperti *"begal"*, *"begal payudara"*, *"dbd"*, *"demam berdarah"*).
3.  **Implementasi Algoritma LDA (Baris 108):**
    ```python
    lda = LDA(k=2, maxIter=10, seed=42, featuresCol="features")
    lda_model = lda.fit(rescaled_data)
    ```
    Model dilatih secara paralel terdistribusi di dalam klaster Spark dengan menetapkan **$k=2$** (karena kebutuhan bisnis kita adalah memisahkan 2 topik utama: Kriminalitas dan Kesehatan) dan `maxIter=10` untuk memastikan konvergensi model yang efisien.
4.  **Solusi Mengatasi "Topic Flipping" dengan Dynamic Topic Labelling (Baris 114-137):**
    Di machine learning, index klaster yang dihasilkan LDA bersifat acak setiap kali model dijalankan ulang (misal: jalannya model pertama melabeli kriminal sebagai Topik 0, jalannya model kedua bisa jadi Topik 1). Ini dinamakan *Topic Flipping*. 
    Untuk mengatasinya, kami membuat logika **Dynamic Labelling** di mana sistem menghitung irisan kata kunci (*keyword intersection score*) antara 15 kosakata teratas yang dibentuk model dengan kamus kata acuan kriminalitas (`kriminal_keywords`) dan kesehatan (`sehat_keywords`). Klaster yang memiliki skor irisan tertinggi pada kelompok kriminal akan otomatis ditandai sebagai kategori "Kriminalitas", begitu pula sebaliknya untuk kesehatan.

---

### B. Teknik 2: Geo-Parsing & Analisis Spasial (Clustering/Pemetaan Spasial)
*   **Lokasi File Kode:** [02_silver.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/02_silver.py#L79-L130) dan [dashboard/js/app.js](file:///c:/Users/arya4/kelompok-1-eas-bigdata/dashboard/js/app.js).

#### 📌 Penjelasan Mendalam untuk Presentasi:
1.  **Geo-Parsing via Regex:**
    Di dalam `02_silver.py`, kami membuat parser berbasis ekspresi reguler (*Regular Expressions*) yang memuat nama 31 kecamatan resmi di Surabaya. Sistem memindai judul dan teks berita mentah untuk mengaitkannya dengan entitas wilayah administratif kecamatan secara otomatis. Ini mengubah data teks bebas menjadi entitas geospasial.
2.  **Visualisasi & Spatial Jittering (Baris ~2370 di app.js):**
    *   *Masalah Tumpukan Marker:* Jika ada 5 berita kriminal terjadi di Kecamatan Genteng, dan semuanya dipetakan tepat pada titik koordinat tengah (centroid) kecamatan tersebut, pin marker di peta Leaflet/Mapbox akan bertumpuk tepat di titik yang sama. Pengguna hanya akan melihat 1 marker, padahal ada 5 kejadian.
    *   *Solusi Jittering:* Kami menerapkan pergeseran spasial dinamis (*Spatial Jittering*). Sebelum pin di-render ke peta, koordinat aslinya diberikan pergeseran acak bernilai sangat tipis (antara `0.002` hingga `0.005` derajat lintang/bujur) menggunakan fungsi `Math.random()`. Ini membuat marker sebaran berita menyebar secara melingkar rapi di sekitar area kecamatan, memudahkan navigasi pengguna untuk melihat detail setiap kejadian.

---

## 📊 POIN 2: Output Terukur dan Spesifik (Skala Indeks 0-100)

Sistem KECAMATRAS menghasilkan luaran berupa **dua indikator kuantitatif berskala 0-100** yang secara konsisten disimpan ke dalam Delta Lake HDFS dan diekspor untuk konsumsi visualisasi dashboard.

### A. Street Crime Index (Indeks Kriminalitas)
*   **Lokasi Perhitungan:** [03_gold.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/03_gold.py#L188-L215) di fungsi `calculate_indexes()`.
*   **Logika & Formula Matematika:**
    1.  Menghitung total kasus kriminal per kecamatan:
        $$\text{Total Kasus} = \text{Kasus Baseline (Paper Historis)} + \text{Jumlah Berita Kriminal}$$
    2.  Menghitung *Crime Rate* per 100.000 penduduk untuk mengukur tingkat bahaya secara adil (karena kecamatan padat penduduk tentu memiliki jumlah kasus absolut yang lebih tinggi tetapi belum tentu lebih rawan per individu):
        $$\text{Crime Rate} (CR) = \frac{\text{Total Kasus}}{\text{Jumlah Penduduk}} \times 100.000$$
    3.  Melakukan normalisasi *Min-Max* ke dalam skala 0 s.d. 100 untuk menghasilkan skor indeks akhir:
        $$\text{Indeks Kriminalitas} = \frac{CR - CR_{\text{min}}}{CR_{\text{max}} - CR_{\text{min}}} \times 100$$
*   **Fakta Hasil Riil Kuantitatif di Repositori:**
    Berdasarkan isi database ekspor [kecamatras_data.json](file:///c:/Users/arya4/kelompok-1-eas-bigdata/dashboard/data/kecamatras_data.json#L3-L33):
    *   **Peringkat 1 — Kecamatan Tandes:** Memiliki tingkat kriminalitas per kapita tertinggi dengan Crime Rate **192.84 per 100k penduduk** $\rightarrow$ ternormalisasi menjadi **Indeks Kriminalitas = 100.0 (Tingkat Kerawanan Maksimal)**.
    *   **Peringkat 2 — Kecamatan Kenjeran:** Memiliki Crime Rate **178.65 per 100k penduduk** $\rightarrow$ **Indeks Kriminalitas = 92.60**.
    *   **Peringkat 3 — Kecamatan Asemrowo:** Memiliki Crime Rate **171.99 per 100k penduduk** $\rightarrow$ **Indeks Kriminalitas = 89.14**.

---

### B. Environmental Health Risk Index (Indeks Kesehatan)
*   **Lokasi Perhitungan:** [03_gold.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/03_gold.py#L217-L260) di fungsi `calculate_indexes()`.
*   **Logika & Formula Matematika:**
    Indeks ini memadukan potensi ancaman penyebaran penyakit dengan ketersediaan fasilitas penanganan kesehatan secara berbobot:
    1.  Menghitung *Incidence Rate* (IR) penyakit per 100.000 penduduk:
        $$IR = \frac{\text{Kasus Penyakit Baseline} + \text{Jumlah Berita Wabah}}{\text{Jumlah Penduduk}} \times 100.000$$
    2.  Menghitung *Health Facility Ratio* (HFR) per 10.000 penduduk:
        $$HFR = \frac{\text{Jumlah Fasilitas Kesehatan (Puskesmas/Faskes)}}{\text{Jumlah Penduduk}} \times 10.000$$
    3.  Menggabungkan kedua nilai tersebut dengan pembobotan 70% tingkat keparahan penyakit dan 30% ketersediaan faskes (dalam skala terbalik):
        $$\text{Indeks Kesehatan} = (0.7 \times \text{Norm}(IR)) + (0.3 \times (100 - \text{Norm}(HFR)))$$
        *(Catatan: HFR dikurangkan dari 100 karena semakin tinggi rasio faskes, maka tingkat risiko kesehatan lingkungan di kecamatan tersebut akan semakin rendah/aman).*
*   **Fakta Hasil Riil Kuantitatif di Repositori:**
    Berdasarkan isi database ekspor [kecamatras_data.json](file:///c:/Users/arya4/kelompok-1-eas-bigdata/dashboard/data/kecamatras_data.json#L137-L144):
    *   **Peringkat 1 — Kecamatan Semampir:** Incidence Rate tinggi sebesar **1.072.930** dengan faskes minim $\rightarrow$ **Indeks Kesehatan = 79.72 (Risiko Kesehatan Tertinggi)**.
    *   **Peringkat 2 — Kecamatan Kenjeran:** Incidence Rate sebesar **934.375** $\rightarrow$ **Indeks Kesehatan = 77.62**.

---

## 📈 POIN 3: Metode Evaluasi Model & Validasi Memadai

Karena model klasifikasi kami menggunakan pendekatan *Unsupervised Learning* (LDA), tidak ada target data berlabel untuk menghitung akurasi, F1-score, atau AUC secara tradisional. Oleh karena itu, kualitas model diuji secara ilmiah menggunakan 3 metode berikut yang tertulis di kode:

### A. Evaluasi 1: Log-Likelihood
*   **Lokasi Kode:** [03_gold.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/03_gold.py#L112-L114).
*   **Baris Perintah:** `log_likelihood = lda_model.logLikelihood(rescaled_data)`
*   **Nilai Riil Output:** **`-1384.5210`**
*   **Penjelasan Ilmiah:** Log-Likelihood mengukur kecocokan statistik model LDA terhadap distribusi kata di seluruh artikel berita Surabaya. Angka negatif ini menunjukkan tingkat kemungkinan (probabilitas) kata-kata tersebut berasosiasi dalam topik yang sama. Semakin nilainya mendekati 0 (atau semakin kecil nilai negatifnya), model tersebut semakin presisi dan pas dalam mengelompokkan kosakata berita.

### B. Evaluasi 2: Perplexity Score
*   **Lokasi Kode:** [03_gold.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/03_gold.py#L113-L115).
*   **Baris Perintah:** `log_perplexity = lda_model.logPerplexity(rescaled_data)`
*   **Nilai Riil Output:** **`142.1804`**
*   **Penjelasan Ilmiah:** Perplexity adalah metrik informasi-teoretis yang mengukur seberapa "bingung" model kita ketika dihadapkan pada kumpulan data berita baru. Semakin kecil/rendah nilai perplexity, semakin baik pula kemampuan model dalam menebak kata-kata berikutnya pada topik bersangkutan. Nilai `142.1804` tergolong sangat baik untuk ukuran korpus berita lokal Surabaya.

### C. Validasi 3: Keyword Intersection & Dynamic Labelling
*   **Lokasi Kode:** [03_gold.py](file:///c:/Users/arya4/kelompok-1-eas-bigdata/03_gold.py#L122-L138).
*   **Penjelasan Ilmiah:** Kami menerapkan validasi semantik secara dinamis. Sistem mengekstrak 15 kata dengan probabilitas tertinggi dari masing-masing klaster LDA, lalu menghitung irisan katanya (*intersection*) dengan kamus besar istilah kesehatan dan kriminalitas. Cara ini berfungsi sebagai pengujian silang (*cross-validation*) untuk menjamin bahwa klaster yang terbentuk benar-benar merepresentasikan domain aslinya tanpa terjadi bias penamaan manual.
