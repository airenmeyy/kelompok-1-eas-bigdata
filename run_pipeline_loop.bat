@echo off
title KECAMATRAS Real-time Pipeline Loop
setlocal

set APP_DIR=c:\Users\Asus\Documents\semester 4\BIG DATA\kelompok-1-eas-bigdata
set NET=kelompok-1-eas-bigdata_kecamatras-net
set IMG=kecamatras-pipeline

echo =================================================================
echo KECAMATRAS Real-time Pipeline Scheduler Loop
echo =================================================================
echo.
echo Pastikan Docker Desktop dan container infrastruktur aktif.
echo Tekan Ctrl+C untuk menghentikan loop ini.
echo.

REM Cek apakah image sudah di-build
docker image inspect %IMG% >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Image '%IMG%' belum ada!
    echo [INFO]  Jalankan dulu: docker build -f Dockerfile.pipeline -t kecamatras-pipeline .
    pause
    exit /b 1
)

echo [OK] Image %IMG% siap digunakan.
echo.

:loop
echo [%time%] =====================================================
echo [%time%] Memulai siklus pipeline baru...
echo [%time%] =====================================================

echo [%time%] [1/4] Bronze Layer - Kafka Stream ke HDFS Raw...
docker run --rm --network %NET% ^
  -e HADOOP_USER_NAME=hadoop ^
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 ^
  -e HDFS_NAMENODE=hdfs://namenode:8020 ^
  -e PYTHONUNBUFFERED=1 ^
  -v "%APP_DIR%:/app" ^
  -w /app %IMG% python3 01_bronze.py --stream
echo [%time%] Bronze selesai (exit: %ERRORLEVEL%).

echo [%time%] [2/4] Silver Layer - Text Preprocessing dan Geo-Parsing...
docker run --rm --network %NET% ^
  -e HADOOP_USER_NAME=hadoop ^
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 ^
  -e HDFS_NAMENODE=hdfs://namenode:8020 ^
  -e PYTHONUNBUFFERED=1 ^
  -v "%APP_DIR%:/app" ^
  -w /app %IMG% python3 02_silver.py
echo [%time%] Silver selesai (exit: %ERRORLEVEL%).

echo [%time%] [3/4] Gold Layer - Spark MLlib LDA dan Index Normalization...
docker run --rm --network %NET% ^
  -e HADOOP_USER_NAME=hadoop ^
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 ^
  -e HDFS_NAMENODE=hdfs://namenode:8020 ^
  -e PYTHONUNBUFFERED=1 ^
  -v "%APP_DIR%:/app" ^
  -w /app %IMG% python3 03_gold.py
echo [%time%] Gold selesai (exit: %ERRORLEVEL%).

echo [%time%] [4/4] Export Gold Layer ke Dashboard JSON...
docker run --rm --network %NET% ^
  -e HADOOP_USER_NAME=hadoop ^
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 ^
  -e HDFS_NAMENODE=hdfs://namenode:8020 ^
  -e PYTHONUNBUFFERED=1 ^
  -v "%APP_DIR%:/app" ^
  -w /app %IMG% python3 export_to_json.py
echo [%time%] Export selesai (exit: %ERRORLEVEL%).

echo.
echo =================================================================
echo [%time%] Siklus selesai. Menunggu 2 menit sebelum siklus berikutnya...
echo =================================================================
timeout /t 120
goto loop
