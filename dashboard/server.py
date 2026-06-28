import os
import time
import json
import threading
import subprocess
from http.server import SimpleHTTPRequestHandler, HTTPServer

# Global state
PIPELINE_RUNNING = False
LAST_UPDATE_TIME = None
PIPELINE_PHASE = "Menunggu Sinkronisasi..."

def run_pipeline():
    global PIPELINE_RUNNING, LAST_UPDATE_TIME, PIPELINE_PHASE
    if PIPELINE_RUNNING:
        return
    
    PIPELINE_RUNNING = True
    PIPELINE_PHASE = "Memulai Pipeline Docker..."
    try:
        # Pindah ke directory utama (kelompok-1-eas-bigdata)
        cwd = os.getcwd()
        if os.path.basename(cwd) == "dashboard":
            project_dir = os.path.dirname(cwd)
        else:
            project_dir = cwd

        print(f"[{time.strftime('%H:%M:%S')}] Memulai KECAMATRAS Pipeline...")
        process = subprocess.Popen(
            ["bash", "./run_pipeline.sh"],
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        for line in process.stdout:
            print(line, end='', flush=True)
            if "[0/4]" in line:
                PIPELINE_PHASE = "Tarik RSS Ingestion..."
            elif "[1/4]" in line:
                PIPELINE_PHASE = "Bronze Layer (Kafka->HDFS)..."
            elif "[2/4]" in line:
                PIPELINE_PHASE = "Silver Layer (Geo-Parsing)..."
            elif "[3/4]" in line:
                PIPELINE_PHASE = "Gold Layer (Agregasi Data)..."
            elif "[4/4]" in line:
                PIPELINE_PHASE = "Ekspor JSON Dashboard..."
            
        process.wait()
        
        if process.returncode == 0:
            print(f"[{time.strftime('%H:%M:%S')}] KECAMATRAS Pipeline Selesai Sukses!")
            LAST_UPDATE_TIME = time.time()
            PIPELINE_PHASE = "Sinkronisasi Sukses"
        else:
            print(f"[{time.strftime('%H:%M:%S')}] Error saat menjalankan pipeline. Exit code: {process.returncode}")
            
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Exception saat menjalankan pipeline: {e}")
    finally:
        PIPELINE_RUNNING = False

def auto_update_loop():
    print("Auto-update scheduler berjalan. Pipeline akan dieksekusi tiap 15 menit.")
    while True:
        run_pipeline()
        # Tunggu 900 detik (15 menit) sebelum iterasi berikutnya
        time.sleep(900)

class KecamatrasHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        cwd = os.getcwd()
        # Jika dijalankan di project root, serve directory 'dashboard'
        if os.path.basename(cwd) != "dashboard" and os.path.isdir("dashboard"):
            super().__init__(*args, directory="dashboard", **kwargs)
        else:
            super().__init__(*args, **kwargs)

    def end_headers(self):
        # Disable caching for API & JSON files
        if self.path.endswith('.json') or self.path.startswith('/api/'):
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
        super().end_headers()

    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            response = {
                "running": PIPELINE_RUNNING,
                "last_update": LAST_UPDATE_TIME,
                "phase": PIPELINE_PHASE
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == '/api/update':
            if PIPELINE_RUNNING:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "Pipeline sedang berjalan"}).encode('utf-8'))
            else:
                threading.Thread(target=run_pipeline, daemon=True).start()
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok", "message": "Pipeline dimulai secara manual"}).encode('utf-8'))
        else:
            self.send_error(404, "Endpoint Not Found")


if __name__ == '__main__':
    PORT = 8000
    
    # Start auto-update loop in a background thread
    auto_thread = threading.Thread(target=auto_update_loop, daemon=True)
    auto_thread.start()

    server = HTTPServer(('0.0.0.0', PORT), KecamatrasHandler)
    print("=================================================================")
    print("KECAMATRAS Dashboard Server (with Auto-Update 15 Menit)")
    print(f"Menjalankan server HTTP lokal di port {PORT}...")
    print(f"Silakan buka browser Anda dan akses: http://localhost:{PORT}")
    print("Tekan Ctrl+C untuk menghentikan server.")
    print("=================================================================\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nMematikan server...")
        server.server_close()
