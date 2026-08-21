"""
METAR producer: polls the NOAA Aviation Weather Center API and publishes
observations to Kafka.

Design notes (these are the things interviewers ask about):
  - Keyed by ICAO station id so all observations for one airport land on the
    same partition. That gives us per-station ordering, which the streaming
    job relies on for dedupe.
  - Idempotent producer + acks=all so a retry after a broker hiccup does not
    create duplicate offsets. This is the producer half of exactly-once.
  - The API returns the *current* observation set on every poll, so the same
    observation is re-sent many times. We deliberately do NOT dedupe here --
    the dedupe happens in Spark with a watermark, which is the realistic
    pattern (producers are dumb, the stream is smart).
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

API_URL = "https://aviationweather.gov/api/data/metar"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("METAR_TOPIC", "metar.raw")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("metar-producer")

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
            "client.id": "metar-producer",
        }
    )


# Six tiles covering CONUS. The API silently truncates any single query at 400
# results, so one CONUS-wide request loses most of the country. Tiling also
# means one failing region degrades the batch instead of killing it.
TILES = [
    "24,-125,38,-105",
    "38,-125,50,-105",
    "24,-105,38,-85",
    "38,-105,50,-85",
    "24,-85,38,-66",
    "38,-85,50,-66",
]

TRUNCATION_LIMIT = 400


def fetch_observations() -> list[dict]:
    collected: list[dict] = []
    for tile in TILES:
        try:
            resp = requests.get(
                API_URL,
                params={"format": "json", "bbox": tile},
                timeout=30,
                headers={"User-Agent": "metar-stream/1.0 (portfolio project)"},
            )
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("tile %s failed: %s", tile, exc)
            continue

        if not isinstance(payload, list):
            log.warning("tile %s returned %s, skipping", tile, type(payload))
            continue

        if len(payload) >= TRUNCATION_LIMIT:
            log.warning(
                "tile %s hit the %d-result cap -- it needs splitting",
                tile,
                TRUNCATION_LIMIT,
            )

        collected.extend(payload)
        time.sleep(1)  # the API rate-limits frequent requests

    return collected


def delivery_report(err, msg):
    if err is not None:
        log.error("delivery failed for %s: %s", msg.key(), err)


def normalize(obs: dict) -> dict | None:
    """Flatten one upstream observation, rejecting malformed records safely."""
    if not isinstance(obs, dict):
        return None

    station = obs.get("icaoId")
    obs_time = obs.get("obsTime")
    if not isinstance(station, str) or obs_time is None:
        return None

    station = station.strip().upper()
    if not station:
        return None

    try:
        observed_at = datetime.fromtimestamp(
            float(obs_time), tz=timezone.utc
        ).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        log.warning("station %s has invalid obsTime=%r, skipping", station, obs_time)
        return None

    return {
        "station_id": station,
        "observed_at": observed_at,
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
        "name": obs.get("name"),
        "lat": obs.get("lat"),
        "lon": obs.get("lon"),
        "elevation_m": obs.get("elev"),
        "temp_c": obs.get("temp"),
        "dewpoint_c": obs.get("dewp"),
        "wind_dir_deg": obs.get("wdir"),
        "wind_speed_kt": obs.get("wspd"),
        "wind_gust_kt": obs.get("wgst"),
        "visibility_sm": obs.get("visib"),
        "altimeter_hpa": obs.get("altim"),
        "flight_category": obs.get("fltCat"),
        "raw_text": obs.get("rawOb"),
    }


def main() -> int:
    producer = build_producer()
    log.info("producing to %s on %s every %ss", TOPIC, BOOTSTRAP, POLL_SECONDS)

    while _running:
        cycle_start = time.monotonic()
        try:
            observations = fetch_observations()
        except requests.RequestException as exc:
            log.error("fetch failed, backing off: %s", exc)
            time.sleep(15)
            continue

        published = skipped = 0
        for obs in observations:
            record = normalize(obs)
            if record is None:
                skipped += 1
                continue
            producer.produce(
                topic=TOPIC,
                key=record["station_id"].encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report,
            )
            published += 1
            # Serve delivery callbacks without blocking the produce loop.
            producer.poll(0)

        producer.flush(30)
        log.info("published=%d skipped=%d", published, skipped)

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, POLL_SECONDS - elapsed)
        # Sleep in slices so SIGTERM is responsive.
        while sleep_for > 0 and _running:
            chunk = min(1.0, sleep_for)
            time.sleep(chunk)
            sleep_for -= chunk

    producer.flush(30)
    log.info("producer stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
