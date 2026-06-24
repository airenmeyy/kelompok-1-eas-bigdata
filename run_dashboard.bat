@echo off
echo =================================================================
echo KECAMATRAS Dashboard Launcher
echo =================================================================
echo.
echo Menjalankan server HTTP lokal di port 8000...
echo Silakan buka browser Anda dan akses: http://localhost:8000
echo.
echo Tekan Ctrl+C untuk menghentikan server.
echo.

python -m http.server 8000 --directory dashboard
