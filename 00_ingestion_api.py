"""
=============================================================================
 00_ingestion_api.py — KECAMATRAS Kafka Producer (RSS News Ingestion)
=============================================================================
 Project  : KECAMATRAS (Kecamatan Metrics & Anomaly Tracking of Surabaya)
 Team     : Anti Gravity — Institut Teknologi Sepuluh Nopember (ITS)
 Layer    : Ingestion (Hulu Pipeline — Sebelum Bronze Layer)
 Role     : Kafka Producer — Menyedot berita dari Google News RSS Feed
            lalu mengirimkan payload JSON ke topik Kafka `kecamatras-stream`.
 ───────────────────────────────────────────────────────────────────────────
 Deskripsi:
   Script ini adalah komponen PERTAMA dari pipeline KECAMATRAS yang
   bertugas mengekstrak data berita dinamis (Kriminalitas & Kesehatan)
   dari RSS Feed Google News, membentuknya menjadi payload JSON
   terstruktur, dan mengirimkannya ke Apache Kafka sebagai message
   broker. Data ini selanjutnya akan dikonsumsi oleh Bronze Layer
   (01_bronze.py) melalui Spark Structured Streaming.

   Alur Pipeline:
     [Google News RSS] → [00_ingestion_api.py (Producer)]
                          → [Kafka Topic: kecamatras-stream]
                            → [01_bronze.py (Consumer/Bronze)]

 ───────────────────────────────────────────────────────────────────────────
 Mapping Kata Kunci RSS → Kategori Penyakit Dataset Puskesmas:
   Sumber: raw_data/kesehatan/new-penyakit_puskesmas_2022_2026-1.csv
   ┌────────────────────────────────┬─────────────────────────────────────┐
   │ Jenis Penyakit (Dataset CSV)   │ Kata Kunci RSS Google News          │
   ├────────────────────────────────┼─────────────────────────────────────┤
   │ Penyakit Infeksi dan parasit   │ wabah, klb, infeksi, dbd,          │
   │                                │ "demam berdarah", tbc, malaria     │
   │ Penyakit sistem pernafasan     │ ispa, pneumonia, "sesak napas"     │
   │ Penyakit sistem pencernaan     │ diare, muntaber, tipes, tifus      │
   │ Keracunan, cedera, dll.        │ keracunan                          │
   └────────────────────────────────┴─────────────────────────────────────┘

 ───────────────────────────────────────────────────────────────────────────
 Cara Menjalankan:
   # Aktivasi virtual environment terlebih dahulu
   source .venv/bin/activate

   # Mode default: loop terus-menerus (Ctrl+C untuk berhenti)
   python3 00_ingestion_api.py

   # Mode terbatas: hanya 3 siklus
   python3 00_ingestion_api.py --max-cycles 3

   # Ubah interval antar siklus (default 60 detik)
   python3 00_ingestion_api.py --cycle-interval 120

   # Ubah delay antar feed (default 7 detik)
   python3 00_ingestion_api.py --feed-delay 10

   # Dry-run (tanpa kirim ke Kafka, cetak ke terminal saja)
   python3 00_ingestion_api.py --dry-run
 =============================================================================
"""

import os
import sys
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any

import feedparser
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════════════════
# KONFIGURASI GLOBAL
# ═══════════════════════════════════════════════════════════════════════════

# ── Kafka ──
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "kecamatras-stream"

# ── RSS Feed URLs ──
# Query dirancang selebar mungkin agar mendeteksi berita kriminalitas fisik
# dan wabah penyakit di tingkat kecamatan Surabaya.
RSS_FEEDS: Dict[str, str] = {
    "Kriminalitas": (
        "https://news.google.com/rss/search?"
        "q=(kriminalitas+OR+curat+OR+curas+OR+curanmor+OR+begal"
        "+OR+gangster+OR+perampokan+OR+penganiayaan+OR+pembunuhan"
        "+OR+narkoba)+kecamatan+surabaya"
        "&hl=id&gl=ID&ceid=ID:id"
    ),
    "Kesehatan": (
        "https://news.google.com/rss/search?"
        "q=(wabah+OR+klb+OR+infeksi+OR+dbd"
        '+OR+"demam+berdarah"+OR+diare+OR+muntaber'
        "+OR+tipes+OR+ispa+OR+pneumonia+OR+tbc"
        "+OR+keracunan)+kecamatan+surabaya"
        "&hl=id&gl=ID&ceid=ID:id"
    ),
}

# ── Timing ──
DEFAULT_CYCLE_INTERVAL = 60   # Detik antar siklus penuh (2 feed)
DEFAULT_FEED_DELAY     = 7    # Detik antar penarikan 1 feed (anti rate-limit)

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KECAMATRAS.Ingestion")


# ═══════════════════════════════════════════════════════════════════════════
# CLASS: KecamatrasNewsProducer
# ═══════════════════════════════════════════════════════════════════════════
class KecamatrasNewsProducer:
    """
    Kafka Producer yang menyedot berita dari Google News RSS Feed
    dan mengirimkannya ke topik Kafka `kecamatras-stream`.

    Payload JSON yang dikirim ke Kafka:
    {
        "id_berita": "sha256-hash-dari-link",
        "judul": "Judul berita asli",
        "link": "https://news.google.com/rss/articles/...",
        "tanggal_publikasi": "2025-06-23T12:00:00",
        "sumber": "detik.com / kompas.com / ...",
        "kategori": "Kriminalitas" | "Kesehatan",
        "deskripsi_mentah": "Teks deskripsi bersih (tanpa tag HTML)",
        "ingested_at": "2025-06-23T12:00:05"
    }
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        feed_delay: int = DEFAULT_FEED_DELAY,
        dry_run: bool = False,
    ):
        """
        Inisialisasi Kafka Producer dan konfigurasi RSS.

        Args:
            bootstrap_servers: Alamat Kafka broker (default: localhost:9092)
            topic: Nama topik Kafka tujuan
            feed_delay: Delay (detik) antar penarikan tiap feed RSS
            dry_run: Jika True, tidak kirim ke Kafka, cetak ke terminal saja
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.feed_delay = feed_delay
        self.dry_run = dry_run
        self.producer = None

        # Set untuk tracking berita yang sudah dikirim (deduplikasi per sesi)
        self._sent_ids: set = set()

        # Statistik per sesi
        self.stats = {
            "total_fetched": 0,
            "total_sent": 0,
            "total_duplicates": 0,
            "total_errors": 0,
            "cycles_completed": 0,
        }

        logger.info("=" * 70)
        logger.info("  KECAMATRAS — RSS News Ingestion (Kafka Producer)")
        logger.info("  Tim Anti Gravity | Institut Teknologi Sepuluh Nopember")
        logger.info("=" * 70)
        logger.info(f"[INIT] Kafka Broker  : {self.bootstrap_servers}")
        logger.info(f"[INIT] Kafka Topic   : {self.topic}")
        logger.info(f"[INIT] Feed Delay    : {self.feed_delay}s")
        logger.info(f"[INIT] Dry Run       : {self.dry_run}")

        if not self.dry_run:
            self._init_kafka_producer()
        else:
            logger.info("[INIT] Mode DRY-RUN aktif — pesan tidak dikirim ke Kafka.")

    # ──────────────────────────────────────────────────────────────────
    # Inisialisasi Kafka Producer
    # ──────────────────────────────────────────────────────────────────
    def _init_kafka_producer(self) -> None:
        """
        Membuat koneksi KafkaProducer dengan konfigurasi:
        - Serialisasi value sebagai JSON (UTF-8 bytes)
        - Serialisasi key sebagai string (UTF-8)
        - Retry 3x jika pengiriman gagal
        - Timeout 10 detik untuk koneksi awal
        """
        try:
            from kafka import KafkaProducer as _KafkaProducer
            from kafka.errors import NoBrokersAvailable

            self.producer = _KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Serialisasi payload JSON → bytes
                value_serializer=lambda v: json.dumps(
                    v, ensure_ascii=False, default=str
                ).encode("utf-8"),
                # Serialisasi key → bytes
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Retry & timeout
                retries=3,
                request_timeout_ms=10000,
                # Kompresi untuk efisiensi bandwidth
                compression_type="gzip",
                # Acknowledgement: tunggu leader Kafka mengonfirmasi
                acks="all",
            )

            logger.info("[INIT] ✅ Kafka Producer berhasil terhubung.")

        except ImportError:
            logger.error(
                "[INIT] ❌ Library 'kafka-python' tidak terinstall.\n"
                "       Jalankan: pip install kafka-python-ng"
            )
            sys.exit(1)

        except Exception as e:
            logger.error(f"[INIT] ❌ Gagal terhubung ke Kafka Broker: {e}")
            logger.error(
                "[INIT] Pastikan Kafka broker sudah berjalan di Docker.\n"
                "       Atau gunakan flag --dry-run untuk testing tanpa Kafka."
            )
            sys.exit(1)

    # ──────────────────────────────────────────────────────────────────
    # Utility: Pembersih HTML
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def clean_html(raw_html: str) -> str:
        """
        Membersihkan tag HTML dari deskripsi RSS menggunakan BeautifulSoup.
        Google News RSS sering menyisipkan tag <a>, <b>, <font>, dll.

        Args:
            raw_html: String mentah yang mungkin mengandung tag HTML.

        Returns:
            Teks bersih tanpa tag HTML, di-strip whitespace berlebih.
        """
        if not raw_html:
            return ""
        soup = BeautifulSoup(raw_html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        # Bersihkan whitespace berlebih (newline, tab, multiple spaces)
        text = " ".join(text.split())
        return text

    # ──────────────────────────────────────────────────────────────────
    # Utility: Generate ID Berita (Deterministik)
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def generate_berita_id(link: str) -> str:
        """
        Menghasilkan ID unik deterministik berdasarkan link berita
        menggunakan SHA-256. Ini memungkinkan deduplikasi lintas sesi
        jika link yang sama muncul lagi di RSS feed.

        Args:
            link: URL berita asli.

        Returns:
            String hash SHA-256 (16 karakter pertama) sebagai ID.
        """
        return hashlib.sha256(link.encode("utf-8")).hexdigest()[:16]

    # ──────────────────────────────────────────────────────────────────
    # Utility: Parse Tanggal Publikasi
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def parse_published_date(entry: Any) -> str:
        """
        Mengekstrak dan memformat tanggal publikasi dari entry RSS.
        feedparser menyediakan `published_parsed` sebagai time.struct_time.

        Args:
            entry: Objek entry dari feedparser.

        Returns:
            String tanggal format ISO 8601 (YYYY-MM-DDTHH:MM:SS),
            atau string kosong jika tidak tersedia.
        """
        try:
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                dt = datetime(*entry.published_parsed[:6])
                return dt.isoformat()
            elif hasattr(entry, "published") and entry.published:
                return entry.published
        except Exception:
            pass
        return datetime.now().isoformat()

    # ──────────────────────────────────────────────────────────────────
    # Utility: Ekstrak Nama Sumber Berita
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def extract_source(entry: Any) -> str:
        """
        Mengekstrak nama sumber media dari entry RSS.
        Google News biasanya menyimpan ini di `entry.source.title`
        atau di akhir judul setelah tanda " - ".

        Args:
            entry: Objek entry dari feedparser.

        Returns:
            Nama sumber media (misal: "detik.com", "Kompas.com").
        """
        # Coba dari field source (Google News menyediakan ini)
        if hasattr(entry, "source") and hasattr(entry.source, "title"):
            return entry.source.title

        # Fallback: ambil dari akhir judul (format: "Judul - Sumber")
        title = getattr(entry, "title", "")
        if " - " in title:
            return title.rsplit(" - ", 1)[-1].strip()

        return "Unknown"

    # ──────────────────────────────────────────────────────────────────
    # Core: Fetch RSS Feed
    # ──────────────────────────────────────────────────────────────────
    def fetch_rss_feed(self, url: str, kategori: str) -> List[Dict]:
        """
        Menarik dan mem-parse RSS feed dari URL Google News.

        Args:
            url: URL RSS feed Google News.
            kategori: Label kategori ("Kriminalitas" atau "Kesehatan").

        Returns:
            List of dict payload berita yang siap dikirim ke Kafka.

        Raises:
            Tidak melempar exception — error di-handle internal dengan logging.
        """
        payloads: List[Dict] = []
        logger.info(f"[RSS] Menarik feed {kategori}...")
        logger.info(f"[RSS] URL: {url[:80]}...")

        try:
            # feedparser menangani HTTP request + XML parsing secara internal
            feed = feedparser.parse(url)

            # Cek apakah feed berhasil diparsing
            if feed.bozo and not feed.entries:
                logger.warning(
                    f"[RSS] ⚠️  Feed {kategori} bermasalah: "
                    f"{getattr(feed, 'bozo_exception', 'Unknown error')}"
                )
                return payloads

            entry_count = len(feed.entries)
            logger.info(f"[RSS] Ditemukan {entry_count} entri berita {kategori}.")

            for entry in feed.entries:
                try:
                    link = getattr(entry, "link", "")
                    if not link:
                        continue

                    # Generate ID deterministik dari link
                    id_berita = self.generate_berita_id(link)

                    # Cek duplikasi dalam sesi ini
                    if id_berita in self._sent_ids:
                        self.stats["total_duplicates"] += 1
                        continue

                    # Bangun payload JSON sesuai spesifikasi PRD
                    payload = {
                        "id_berita": id_berita,
                        "judul": getattr(entry, "title", ""),
                        "link": link,
                        "tanggal_publikasi": self.parse_published_date(entry),
                        "sumber": self.extract_source(entry),
                        "kategori": kategori,
                        "deskripsi_mentah": self.clean_html(
                            getattr(entry, "summary", "")
                            or getattr(entry, "description", "")
                        ),
                        "ingested_at": datetime.now().isoformat(),
                    }

                    payloads.append(payload)
                    self.stats["total_fetched"] += 1

                except Exception as e:
                    logger.warning(
                        f"[RSS] ⚠️  Gagal memproses 1 entri {kategori}: {e}"
                    )
                    continue

        except Exception as e:
            logger.error(
                f"[RSS] ❌ Gagal menarik feed {kategori}: {e}\n"
                f"       Kemungkinan: koneksi internet terputus, "
                f"atau Google melakukan rate limiting."
            )
            self.stats["total_errors"] += 1

        return payloads

    # ──────────────────────────────────────────────────────────────────
    # Core: Kirim ke Kafka
    # ──────────────────────────────────────────────────────────────────
    def send_to_kafka(self, payloads: List[Dict], kategori: str) -> int:
        """
        Mengirim list payload berita ke Kafka topic.

        Args:
            payloads: List of dict payload berita.
            kategori: Label kategori untuk logging.

        Returns:
            Jumlah pesan yang berhasil dikirim.
        """
        if not payloads:
            logger.info(f"[KAFKA] Tidak ada berita baru {kategori} untuk dikirim.")
            return 0

        sent_count = 0

        if self.dry_run:
            # Mode dry-run: cetak payload ke terminal
            for payload in payloads:
                logger.info(
                    f"[DRY-RUN] {kategori} | "
                    f"ID: {payload['id_berita']} | "
                    f"Judul: {payload['judul'][:60]}..."
                )
                self._sent_ids.add(payload["id_berita"])
                sent_count += 1
                self.stats["total_sent"] += 1
            logger.info(
                f"[DRY-RUN] ✅ {sent_count} berita {kategori} "
                f"(dicetak ke terminal, tidak dikirim ke Kafka)."
            )
            return sent_count

        # Mode produksi: kirim ke Kafka
        if self.producer is None:
            logger.error("[KAFKA] ❌ Producer belum diinisialisasi.")
            return 0

        for payload in payloads:
            try:
                # Gunakan id_berita sebagai Kafka message key
                # agar pesan dengan ID yang sama masuk ke partisi yang sama
                future = self.producer.send(
                    topic=self.topic,
                    key=payload["id_berita"],
                    value=payload,
                )

                # Tunggu konfirmasi pengiriman (blocking, timeout 10s)
                record_metadata = future.get(timeout=10)

                # Tandai sebagai sudah dikirim
                self._sent_ids.add(payload["id_berita"])
                sent_count += 1
                self.stats["total_sent"] += 1

                logger.debug(
                    f"[KAFKA] Terkirim → "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}, "
                    f"ID: {payload['id_berita']}"
                )

            except Exception as e:
                logger.error(
                    f"[KAFKA] ❌ Gagal mengirim berita "
                    f"'{payload['judul'][:40]}...': {e}"
                )
                self.stats["total_errors"] += 1

        # Flush buffer agar semua pesan dikirim
        try:
            self.producer.flush(timeout=15)
        except Exception as e:
            logger.warning(f"[KAFKA] ⚠️  Flush timeout: {e}")

        logger.info(
            f"[KAFKA] ✅ Sukses mengirim {sent_count} berita "
            f"{kategori} ke Kafka (topic: {self.topic})."
        )

        return sent_count

    # ──────────────────────────────────────────────────────────────────
    # Core: Satu Siklus Penuh (Fetch + Send untuk semua feed)
    # ──────────────────────────────────────────────────────────────────
    def run_single_cycle(self) -> Dict[str, int]:
        """
        Menjalankan satu siklus penuh:
          1. Fetch RSS feed Kriminalitas → Kirim ke Kafka
          2. Delay antar-feed (anti rate-limit)
          3. Fetch RSS feed Kesehatan → Kirim ke Kafka

        Returns:
            Dict ringkasan: {"Kriminalitas": N, "Kesehatan": M}
        """
        cycle_start = datetime.now()
        self.stats["cycles_completed"] += 1
        cycle_num = self.stats["cycles_completed"]

        logger.info("─" * 60)
        logger.info(f"[CYCLE {cycle_num}] Memulai siklus penarikan RSS...")

        results: Dict[str, int] = {}

        for idx, (kategori, url) in enumerate(RSS_FEEDS.items()):
            # Delay antar feed (kecuali feed pertama)
            if idx > 0:
                logger.info(
                    f"[CYCLE {cycle_num}] ⏳ Delay {self.feed_delay}s "
                    f"(anti rate-limit)..."
                )
                time.sleep(self.feed_delay)

            # Fetch RSS
            payloads = self.fetch_rss_feed(url, kategori)

            # Kirim ke Kafka
            sent = self.send_to_kafka(payloads, kategori)
            results[kategori] = sent

        elapsed = (datetime.now() - cycle_start).total_seconds()
        total_sent = sum(results.values())

        logger.info(f"[CYCLE {cycle_num}] ── Ringkasan Siklus ──")
        for kat, count in results.items():
            logger.info(f"[CYCLE {cycle_num}]   {kat:15s}: {count:>3} berita")
        logger.info(f"[CYCLE {cycle_num}]   {'TOTAL':15s}: {total_sent:>3} berita")
        logger.info(f"[CYCLE {cycle_num}]   Durasi: {elapsed:.1f}s")
        logger.info("─" * 60)

        return results

    # ──────────────────────────────────────────────────────────────────
    # Runner: Loop Utama
    # ──────────────────────────────────────────────────────────────────
    def run(
        self,
        max_cycles: Optional[int] = None,
        cycle_interval: int = DEFAULT_CYCLE_INTERVAL,
    ) -> None:
        """
        Menjalankan loop penarikan RSS secara berulang.

        Args:
            max_cycles: Jumlah siklus maksimum. None = loop tak terbatas.
            cycle_interval: Detik jeda antar siklus penuh.
        """
        logger.info(f"[RUN] Memulai RSS Ingestion Loop...")
        logger.info(
            f"[RUN] Max siklus   : "
            f"{'∞ (tekan Ctrl+C untuk berhenti)' if max_cycles is None else max_cycles}"
        )
        logger.info(f"[RUN] Interval      : {cycle_interval}s antar siklus")
        logger.info(f"[RUN] Feed targets  : {list(RSS_FEEDS.keys())}")

        cycle_count = 0

        try:
            while True:
                cycle_count += 1

                # Cek batas siklus
                if max_cycles is not None and cycle_count > max_cycles:
                    logger.info(
                        f"[RUN] Batas {max_cycles} siklus tercapai. Berhenti."
                    )
                    break

                # Jalankan 1 siklus
                self.run_single_cycle()

                # Cek apakah ini siklus terakhir
                if max_cycles is not None and cycle_count >= max_cycles:
                    logger.info(
                        f"[RUN] Siklus terakhir ({cycle_count}/{max_cycles}) selesai."
                    )
                    break

                # Jeda antar siklus
                logger.info(
                    f"[RUN] 💤 Menunggu {cycle_interval}s sebelum siklus berikutnya...\n"
                )
                time.sleep(cycle_interval)

        except KeyboardInterrupt:
            logger.info("\n[RUN] ⛔ Dihentikan oleh pengguna (Ctrl+C).")

        finally:
            self._print_session_stats()
            self.close()

    # ──────────────────────────────────────────────────────────────────
    # Statistik & Cleanup
    # ──────────────────────────────────────────────────────────────────
    def _print_session_stats(self) -> None:
        """Cetak ringkasan statistik sesi ke terminal."""
        logger.info("=" * 60)
        logger.info("[STATS] ── Ringkasan Sesi Ingestion ──")
        logger.info(f"[STATS]   Siklus selesai  : {self.stats['cycles_completed']}")
        logger.info(f"[STATS]   Total diambil   : {self.stats['total_fetched']}")
        logger.info(f"[STATS]   Total dikirim   : {self.stats['total_sent']}")
        logger.info(f"[STATS]   Duplikat diskip : {self.stats['total_duplicates']}")
        logger.info(f"[STATS]   Error           : {self.stats['total_errors']}")
        logger.info(f"[STATS]   Berita unik     : {len(self._sent_ids)}")
        logger.info("=" * 60)

    def close(self) -> None:
        """Menutup koneksi Kafka Producer dengan aman."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)
                logger.info("[SHUTDOWN] Kafka Producer ditutup dengan aman.")
            except Exception as e:
                logger.warning(f"[SHUTDOWN] ⚠️  Error saat menutup producer: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
def main():
    """
    Entry point utama dengan argument parser.

    Contoh penggunaan:
      python3 00_ingestion_api.py                      # Loop tak terbatas
      python3 00_ingestion_api.py --max-cycles 5       # 5 siklus saja
      python3 00_ingestion_api.py --dry-run             # Tanpa Kafka
      python3 00_ingestion_api.py --dry-run --max-cycles 1  # Test 1x
    """
    parser = argparse.ArgumentParser(
        description=(
            "KECAMATRAS RSS News Ingestion — "
            "Kafka Producer untuk berita Kriminalitas & Kesehatan Surabaya"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh penggunaan:
  python3 00_ingestion_api.py                          # Loop terus-menerus
  python3 00_ingestion_api.py --max-cycles 3           # Hanya 3 siklus
  python3 00_ingestion_api.py --dry-run                # Test tanpa Kafka
  python3 00_ingestion_api.py --dry-run --max-cycles 1 # Quick test
        """,
    )

    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Jumlah siklus maksimum. Default: tak terbatas (Ctrl+C untuk stop).",
    )
    parser.add_argument(
        "--cycle-interval",
        type=int,
        default=DEFAULT_CYCLE_INTERVAL,
        help=f"Jeda (detik) antar siklus penuh. Default: {DEFAULT_CYCLE_INTERVAL}s.",
    )
    parser.add_argument(
        "--feed-delay",
        type=int,
        default=DEFAULT_FEED_DELAY,
        help=f"Jeda (detik) antar penarikan tiap feed RSS. Default: {DEFAULT_FEED_DELAY}s.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode test: cetak payload ke terminal tanpa mengirim ke Kafka.",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=KAFKA_BOOTSTRAP_SERVERS,
        help=f"Alamat Kafka broker. Default: {KAFKA_BOOTSTRAP_SERVERS}.",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help=f"Nama Kafka topic tujuan. Default: {KAFKA_TOPIC}.",
    )

    args = parser.parse_args()

    # Inisialisasi Producer
    producer = KecamatrasNewsProducer(
        bootstrap_servers=args.broker,
        topic=args.topic,
        feed_delay=args.feed_delay,
        dry_run=args.dry_run,
    )

    # Jalankan loop
    producer.run(
        max_cycles=args.max_cycles,
        cycle_interval=args.cycle_interval,
    )


if __name__ == "__main__":
    main()
