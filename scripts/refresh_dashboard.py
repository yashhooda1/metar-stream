"""
Hourly dashboard refresh for GitHub Actions.

This is a BATCH job, not the streaming pipeline. Actions runners are ephemeral,
so there is no Kafka, no Spark, and no Delta lake here. What it does have is
state: each run reads the previous run's output from the data branch, merges
new observations against it, and writes back. That gives real deduplication
counts and a genuine trend series across runs, rather than a stateless snapshot
that forgets everything each hour.

Files it maintains, all on the data branch under docs/:

  metar_seen.json       dedupe keys within the retention window
  metar_history.json    hourly rollups, for the trend series
  metar_dashboard.json  what the website fetches

Run locally with:  python scripts/refresh_dashboard.py
"""

from __future__ import annotations

import json
import pathlib
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests

API_URL = "https://aviationweather.gov/api/data/metar"
DOCS = pathlib.Path("docs")

# Same six tiles as the streaming producer. The API silently truncates any
# single query at 400 results, so one CONUS-wide request loses most of the
# continent. See README finding #2.
TILES = [
    "24,-125,38,-105",
    "38,-125,50,-105",
    "24,-105,38,-85",
    "38,-105,50,-85",
    "24,-85,38,-66",
    "38,-85,50,-66",
]
TRUNCATION_LIMIT = 400

SEEN_RETENTION_HOURS = 36    # dedupe window; must exceed the report cadence
HISTORY_RETENTION_HOURS = 168  # one week of hourly rollups
CATEGORY_ORDER = ["LIFR", "IFR", "MVFR", "VFR"]

UA = {"User-Agent": "metar-stream/1.0 (github actions; portfolio project)"}


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def load(name: str, default):
    path = DOCS / name
    if not path.exists():
        log(f"{name} absent, starting fresh")
        return default
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        log(f"{name} unreadable ({exc}), starting fresh")
        return default


def fetch() -> tuple[list[dict], list[str]]:
    """Returns (observations, warnings). Per-tile isolation: a failing region
    costs one tile, not the whole run."""
    collected: list[dict] = []
    warnings: list[str] = []

    for tile in TILES:
        try:
            r = requests.get(
                API_URL,
                params={"format": "json", "bbox": tile},
                timeout=30,
                headers=UA,
            )
            r.raise_for_status()
            payload = r.json()
        except (requests.RequestException, ValueError) as exc:
            warnings.append(f"tile {tile}: {exc}")
            log(f"  tile {tile} FAILED: {exc}")
            continue

        if not isinstance(payload, list):
            warnings.append(f"tile {tile}: unexpected payload type")
            continue

        if len(payload) >= TRUNCATION_LIMIT:
            warnings.append(f"tile {tile} hit the {TRUNCATION_LIMIT} cap")
            log(f"  tile {tile} HIT CAP — needs splitting")

        collected.extend(payload)
        log(f"  tile {tile}: {len(payload)}")
        time.sleep(1)  # the API rate-limits frequent requests

    return collected, warnings


def normalise(obs: dict) -> dict | None:
    station = obs.get("icaoId")
    ts = obs.get("obsTime")
    if not station or ts is None:
        return None

    vis = obs.get("visib")
    vis_mi = None
    if isinstance(vis, (int, float)):
        vis_mi = float(vis)
    elif isinstance(vis, str):
        cleaned = vis.replace("+", "").strip()
        try:
            vis_mi = float(cleaned)
        except ValueError:
            vis_mi = None

    return {
        "station_id": station,
        "observed_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "name": obs.get("name"),
        "temp_c": obs.get("temp"),
        "wind_speed_kt": obs.get("wspd"),
        "wind_gust_kt": obs.get("wgst") or obs.get("wspd"),
        "visibility_mi": vis_mi,
        "flight_category": obs.get("fltCat"),
    }


def main() -> int:
    DOCS.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    seen: dict[str, str] = load("metar_seen.json", {})
    history: list[dict] = load("metar_history.json", [])
    totals: dict = load("metar_totals.json", {"fetched": 0, "retained": 0, "runs": 0})

    log(f"loaded {len(seen)} dedupe keys, {len(history)} history points")

    log("fetching")
    raw, warnings = fetch()
    if not raw:
        log("no observations fetched; leaving previous snapshot in place")
        return 1

    # Deduplicate against everything seen inside the retention window. This is
    # the batch equivalent of the streaming job's watermarked dropDuplicates:
    # same key, bounded state, same purpose.
    observations: dict[str, dict] = {}
    new_count = 0

    for item in raw:
        rec = normalise(item)
        if rec is None:
            continue
        key = f"{rec['station_id']}|{rec['observed_at']}"
        observations[key] = rec
        if key not in seen:
            seen[key] = rec["observed_at"]
            new_count += 1

    duplicate_count = len(observations) - new_count
    log(f"fetched {len(observations)} unique, {new_count} new, "
        f"{duplicate_count} already seen")

    # Prune the dedupe set. Without this it grows without bound — the same
    # problem a watermark solves in the streaming job.
    cutoff = (now - timedelta(hours=SEEN_RETENTION_HOURS)).isoformat()
    before_prune = len(seen)
    seen = {k: v for k, v in seen.items() if v >= cutoff}
    log(f"pruned dedupe set {before_prune} -> {len(seen)}")

    # Latest observation per station is the current picture.
    latest: dict[str, dict] = {}
    for rec in observations.values():
        cur = latest.get(rec["station_id"])
        if cur is None or rec["observed_at"] > cur["observed_at"]:
            latest[rec["station_id"]] = rec

    categories = Counter(
        r["flight_category"] for r in latest.values() if r.get("flight_category")
    )

    # Station mix by ICAO prefix. K=US, C=Canada, M=Mexico — the tiles cross
    # borders, so "US stations" would be wrong. See README finding #2.
    prefixes = Counter(s[0] for s in latest)

    gusts = sorted(
        (r for r in latest.values() if r.get("wind_gust_kt")),
        key=lambda r: r["wind_gust_kt"],
        reverse=True,
    )[:12]

    alerts = sorted(
        (
            r for r in latest.values()
            if r.get("flight_category") in ("IFR", "LIFR")
            or (r.get("wind_gust_kt") or 0) >= 35
        ),
        key=lambda r: r["observed_at"],
        reverse=True,
    )[:20]

    totals["fetched"] += len(observations)
    totals["retained"] += new_count
    totals["runs"] += 1

    history.append({
        "at": now.isoformat(),
        "stations": len(latest),
        "new_observations": new_count,
        "categories": {c: categories.get(c, 0) for c in CATEGORY_ORDER},
        "max_gust_kt": gusts[0]["wind_gust_kt"] if gusts else None,
    })
    hist_cutoff = (now - timedelta(hours=HISTORY_RETENTION_HOURS)).isoformat()
    history = [h for h in history if h["at"] >= hist_cutoff]

    dedupe_pct = (
        round(100 * (1 - totals["retained"] / totals["fetched"]), 1)
        if totals["fetched"] else 0.0
    )

    dashboard = {
        "generated_at": now.isoformat(),
        "source": "scheduled batch refresh (GitHub Actions)",
        "note": (
            "Hourly batch refresh. The Kafka/Spark streaming pipeline in this "
            "repo runs separately; its benchmarks are in the README."
        ),
        "stats": {
            "stations": len(latest),
            "observations_retained": totals["retained"],
            "messages_ingested": totals["fetched"],
            "dedupe_pct": dedupe_pct,
            "runs": totals["runs"],
        },
        "station_mix": dict(prefixes.most_common()),
        "flight_categories": {c: categories.get(c, 0) for c in CATEGORY_ORDER},
        "top_gusts": [
            {
                "station": g["station_id"],
                "name": g["name"],
                "gust_kt": g["wind_gust_kt"],
                "observed_at": g["observed_at"],
            }
            for g in gusts
        ],
        "alerts": [
            {
                "station": a["station_id"],
                "name": a["name"],
                "category": a.get("flight_category"),
                "gust_kt": a.get("wind_gust_kt"),
                "visibility_mi": a.get("visibility_mi"),
                "observed_at": a["observed_at"],
            }
            for a in alerts
        ],
        "trend": history[-72:],
        "warnings": warnings,
    }

    (DOCS / "metar_dashboard.json").write_text(json.dumps(dashboard, indent=2))
    (DOCS / "metar_seen.json").write_text(json.dumps(seen))
    (DOCS / "metar_history.json").write_text(json.dumps(history, indent=2))
    (DOCS / "metar_totals.json").write_text(json.dumps(totals, indent=2))

    log(f"wrote dashboard: {len(latest)} stations, {dedupe_pct}% cumulative dedupe")
    return 0


if __name__ == "__main__":
    sys.exit(main())
