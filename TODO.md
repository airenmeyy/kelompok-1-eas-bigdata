# TODO - Fitur RSS untuk 00_ingestion_api

- [ ] Tambahkan ingestion RSS sederhana pada `00_ingestion_api(keamanan_kesehataan).py`.
- [ ] Tentukan skema kolom output RSS (sementara) agar konsisten tanpa perlu tahu format dataset sebenarnya.
- [ ] Simpan hasil RSS ke `raw_data/<kategori>/rss_*.csv`.
- [ ] (Opsional) Jika item RSS punya link ke file data (CSV/JSON/XLSX), download attachment lalu jalankan standarisasi yang sudah ada.
- [ ] Update `requirements.txt` jika butuh library RSS parsing.
- [ ] Uji dengan menjalankan skrip (contoh CLI argument) dan pastikan `lakehouse/index/index_keamanan_kesehatan.csv` tetap ter-update jika attachment tersedia.
