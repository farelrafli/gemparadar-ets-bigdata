"""
producer_rss.py - GempaRadar
[NamaAnggota3]: Producer untuk RSS Feed BMKG & Tempo Gempa
Polling setiap 5 menit, kirim ke Kafka topic: gempa-rss
"""

import json
import time
import hashlib
import logging
from datetime import datetime, timezone
import feedparser
from kafka import KafkaProducer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RSS       = "gempa-rss"
POLL_INTERVAL   = 300  # 5 menit

RSS_FEEDS = [
    {
        "url"   : "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.atom",
        "source": "USGS-RSS",
    },
    {
        "url"   : "https://www.cnnindonesia.com/nasional/rss",
        "source": "CNN-Indonesia",
    },
]
# Set ID yang sudah dikirim (hindari duplikat)
sent_ids: set = set()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER-RSS] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        enable_idempotence=True,
        acks="all",
        retries=5,
    )


def make_id(url: str) -> str:
    """Buat ID 8 karakter dari hash URL."""
    return hashlib.md5(url.encode()).hexdigest()[:8]


def parse_entry(entry, source: str) -> dict:
    """Parse satu RSS entry menjadi dict konsisten."""
    url   = getattr(entry, "link", "")
    title = getattr(entry, "title", "")
    summary = getattr(entry, "summary", "")
    published = getattr(entry, "published", "")

    # Coba parse tanggal, fallback ke sekarang
    try:
        pub_struct = entry.get("published_parsed")
        if pub_struct:
            pub_dt = datetime(*pub_struct[:6], tzinfo=timezone.utc)
            published_iso = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            published_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        published_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "article_id" : make_id(url),
        "title"      : title,
        "url"        : url,
        "summary"    : summary[:500] if summary else "",
        "published"  : published_iso,
        "source"     : source,
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def poll_feeds(producer: KafkaProducer):
    """Poll semua RSS feed dan kirim artikel baru ke Kafka."""
    for feed_cfg in RSS_FEEDS:
        url    = feed_cfg["url"]
        source = feed_cfg["source"]
        log.info(f"Polling RSS: {source} → {url}")

        try:
            feed    = feedparser.parse(url)
            entries = feed.get("entries", [])
            log.info(f"  {source}: {len(entries)} artikel ditemukan")
        except Exception as e:
            log.error(f"  Gagal parse {source}: {e}")
            continue

        new_count = 0
        for entry in entries:
            article_url = getattr(entry, "link", "")
            article_id  = make_id(article_url)

            if article_id in sent_ids:
                continue  # skip duplikat

            data = parse_entry(entry, source)
            key  = article_id

            try:
                future = producer.send(TOPIC_RSS, key=key, value=data)
                future.get(timeout=10)
                sent_ids.add(article_id)
                new_count += 1
                log.info(f"  Terkirim → [{source}] {data['title'][:60]}...")
            except Exception as e:
                log.error(f"  Gagal kirim artikel {article_id}: {e}")

        log.info(f"  {source}: {new_count} artikel baru dikirim")


def run():
    log.info("=== GempaRadar Producer RSS dimulai ===")
    producer = create_producer()

    while True:
        poll_feeds(producer)
        producer.flush()
        log.info(f"Total artikel terkirim sesi ini: {len(sent_ids)}")
        log.info(f"Menunggu {POLL_INTERVAL} detik...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
