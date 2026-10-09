"""
ERCOT producer: polls ERCOT's public grid dashboards and publishes one record
per (feed, series, market, interval) to Kafka.

Same shape as metar_producer.py, with two ERCOT-specific differences:

  - Every poll returns the whole operating day so far, so each interval is
    re-sent on every cycle. As with METAR, the producer does not dedupe; the
    stream does.
  - ERCOT revises recent intervals (fuel-mix especially). The record carries
    the document's lastUpdated, and silver keeps the latest revision per key
    instead of the first one seen. That is why ercot_stream.py upserts with
    MERGE where metar_stream.py uses dropDuplicates.

Keyed by feed|series so each series lands on one partition and stays ordered.
"""

import json
import logging
import os
import signal
import sys
import time

import requests
from confluent_kafka import Producer

from ercot_feeds import fetch_all

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("ERCOT_TOPIC", "ercot.raw")
# The dashboards refresh every 5 minutes (prices every 15); polling faster only
# multiplies duplicates.
POLL_SECONDS = int(os.getenv("ERCOT_POLL_SECONDS", "120"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ercot-producer")

_running = True


def _shutdown(signum, frame):
    global _running
    log.info("signal %s received, draining producer", signum)
    _running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "linger.ms": 50,
            "compression.type": "zstd",
            "client.id": "ercot-producer",
        }
    )


def record_key(rec: dict) -> bytes:
    return f"{rec['feed']}|{rec['series']}".encode("utf-8")


def delivery_report(err, msg):
    if err is not None:
        log.error("delivery failed for %s: %s", msg.key(), err)


def main() -> int:
    producer = build_producer()
    session = requests.Session()
    log.info("producing to %s on %s every %ss", TOPIC, BOOTSTRAP, POLL_SECONDS)

    while _running:
        cycle_start = time.monotonic()
        records, warnings = fetch_all(session)
        for w in warnings:
            log.warning(w)

        for rec in records:
            producer.produce(
                topic=TOPIC,
                key=record_key(rec),
                value=json.dumps(rec).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
        producer.flush(30)

        by_feed = {}
        for rec in records:
            by_feed[rec["feed"]] = by_feed.get(rec["feed"], 0) + 1
        log.info("published=%d %s", len(records), by_feed)

        sleep_for = max(0.0, POLL_SECONDS - (time.monotonic() - cycle_start))
        while sleep_for > 0 and _running:
            chunk = min(1.0, sleep_for)
            time.sleep(chunk)
            sleep_for -= chunk

    producer.flush(30)
    log.info("producer stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
