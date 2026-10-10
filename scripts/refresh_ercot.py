"""
Hourly ERCOT dashboard refresh for GitHub Actions.

The batch twin of ercot_producer.py + ercot_stream.py + ercot_gold.py, in the
same spirit as scripts/refresh_dashboard.py for METAR: Actions runners are
ephemeral, so there is no Kafka, Spark or Delta here. State lives on the data
branch and each run merges into it, so revisions, history and the weather
model accumulate across runs.

Files on the data branch, under docs/:

  ercot_history.json    hourly rows (UTC): demand, generation by fuel, prices,
                        weighted ERCOT temperature. 60 days retained.
  ercot_totals.json     cumulative run counters
  ercot_dashboard.json  what yashhooda.ai fetches

Run locally:  python scripts/refresh_ercot.py   (writes ./docs)
"""

from __future__ import annotations

import json
import pathlib
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ercot_feeds import (  # noqa: E402
    ERCOT_STATIONS,
    fetch_all,
    fit_degree_day_model,
    quality_errors,
)

DOCS = pathlib.Path("docs")
METAR_URL = "https://aviationweather.gov/api/data/metar"
UA = {"User-Agent": "metar-stream-ercot/1.0 (github actions; portfolio project)"}

HISTORY_RETENTION_DAYS = 60
WEATHER_LOOKBACK_HOURS = 6
TODAY_HUBS = ["HB_HOUSTON", "HB_NORTH", "HB_WEST", "HB_HUBAVG"]
FUELS = ["Natural Gas", "Wind", "Solar", "Nuclear", "Coal and Lignite", "Power Storage", "Hydro", "Other"]


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def load(name: str, default):
    path = DOCS / name
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        log(f"{name} unreadable ({exc}), starting fresh")
        return default


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def hour_of(ts: datetime, *, interval_end: bool = False, nearest: bool = False) -> str:
    """UTC hour bucket as ISO. Interval-ending stamps (prices) belong to the
    hour before them; METAR reports at :53 belong to the next hour."""
    if interval_end:
        ts = ts - timedelta(seconds=1)
    if nearest:
        ts = ts + timedelta(minutes=30)
    return ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()


def fetch_temps(session=None) -> tuple[dict[str, dict[str, float]], list[str]]:
    """{hour: {station: temp_c}} for the ERCOT airports over the last few hours."""
    http = session or requests
    try:
        r = http.get(
            METAR_URL,
            params={"ids": ",".join(ERCOT_STATIONS), "format": "json", "hours": WEATHER_LOOKBACK_HOURS},
            headers=UA, timeout=30,
        )
        r.raise_for_status()
        obs = r.json()
    except (requests.RequestException, ValueError) as exc:
        return {}, [f"metar temps: {exc}"]
    return bucket_temps(obs), []


def bucket_temps(obs) -> dict[str, dict[str, float]]:
    sums: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for o in obs if isinstance(obs, list) else []:
        st, t, ts = o.get("icaoId"), o.get("temp"), o.get("obsTime")
        if st not in ERCOT_STATIONS or not isinstance(t, (int, float)) or ts is None:
            continue
        try:
            when = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            continue
        sums[hour_of(when, nearest=True)][st].append(float(t))
    return {h: {st: sum(v) / len(v) for st, v in by.items()} for h, by in sums.items()}


def weighted_f(temps_c: dict[str, float]) -> tuple[float | None, int]:
    num = den = 0.0
    for st, c in temps_c.items():
        w = ERCOT_STATIONS[st][1]
        num += w * (c * 9 / 5 + 32)
        den += w
    return (round(num / den, 2), len(temps_c)) if den else (None, 0)


def _mean(v):
    return round(sum(v) / len(v), 2) if v else None


def hourly_rows(records: list[dict]) -> dict[str, dict]:
    """Aggregate this run's records into hourly rows keyed by UTC hour."""
    acc: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in records:
        ts = parse_iso(r["observed_at"])
        if r["feed"] == "demand" and r["series"] == "system":
            h = acc[hour_of(ts)]
            h["demand_mw"].append(r["value"])
            if r.get("capacity_mw"):
                h["capacity_mw"].append(r["capacity_mw"])
        elif r["feed"] == "demand" and r["series"] == "forecast":
            acc[hour_of(ts, interval_end=True)]["forecast_mw"].append(r["value"])
        elif r["feed"] == "fuel":
            acc[hour_of(ts)][f"fuel:{r['series']}"].append(r["value"])
        elif r["feed"] == "price" and r["series"] in TODAY_HUBS:
            col = f"{'rt' if r['market'] == 'RT' else 'da'}:{r['series']}"
            acc[hour_of(ts, interval_end=True)][col].append(r["value"])
    return {h: {k: _mean(v) for k, v in cols.items()} for h, cols in acc.items()}


def merge_history(history: dict[str, dict], fresh: dict[str, dict], temps: dict[str, dict[str, float]],
                  now: datetime) -> tuple[dict[str, dict], int]:
    """Fresh values replace stored ones column by column (ERCOT revises recent
    intervals). Returns (history, revised_cells)."""
    revised = 0
    for hour, cols in fresh.items():
        row = history.setdefault(hour, {})
        for k, v in cols.items():
            if v is None:
                continue
            if k in row and row[k] != v:
                revised += 1
            row[k] = v
    for hour, by_station in temps.items():
        row = history.setdefault(hour, {})
        merged = dict(row.get("temps_c") or {})
        merged.update(by_station)
        row["temps_c"] = {k: round(v, 2) for k, v in merged.items()}
        row["temp_f"], row["stations"] = weighted_f(merged)
    cutoff = (now - timedelta(days=HISTORY_RETENTION_DAYS)).isoformat()
    return {h: r for h, r in sorted(history.items()) if h >= cutoff}, revised


def today_series(records: list[dict], now: datetime) -> dict:
    """Last 24 hours at native resolution, trimmed for the browser."""
    since = now - timedelta(hours=24)
    demand, forecast, prices = [], [], defaultdict(list)
    for r in records:
        ts = parse_iso(r["observed_at"])
        if r["feed"] == "demand" and r["series"] == "system" and ts >= since and ts.minute % 15 == 0:
            demand.append({"t": r["observed_at"], "mw": r["value"], "cap": r.get("capacity_mw")})
        elif r["feed"] == "demand" and r["series"] == "forecast" and since <= ts <= now + timedelta(hours=24):
            forecast.append({"t": r["observed_at"], "mw": r["value"]})
        elif r["feed"] == "price" and r["market"] == "RT" and r["series"] in TODAY_HUBS and ts >= since:
            prices[r["series"]].append({"t": r["observed_at"], "usd": r["value"]})
    key = lambda p: p["t"]
    return {
        "demand": sorted(demand, key=key),
        "forecast": sorted(forecast, key=key),
        "rt_prices": {k: sorted(v, key=key) for k, v in prices.items()},
    }


def latest_snapshot(records: list[dict]) -> dict | None:
    demand = [r for r in records if r["feed"] == "demand" and r["series"] == "system"]
    if not demand:
        return None
    last = max(demand, key=lambda r: r["observed_at"])
    fuel_at = max((r["observed_at"] for r in records if r["feed"] == "fuel"), default=None)
    fuels = {r["series"]: r["value"] for r in records if r["feed"] == "fuel" and r["observed_at"] == fuel_at}
    gen = sum(v for v in fuels.values() if v is not None)
    ws = (fuels.get("Wind") or 0) + (fuels.get("Solar") or 0)
    price_at = max((r["observed_at"] for r in records if r["feed"] == "price" and r["market"] == "RT"),
                   default=None)
    prices = {r["series"]: r["value"] for r in records
              if r["feed"] == "price" and r["market"] == "RT" and r["observed_at"] == price_at}
    cap = last.get("capacity_mw")
    return {
        "demand_at": last["observed_at"],
        "demand_mw": last["value"],
        "capacity_mw": cap,
        "margin_pct": round(100 * (cap - last["value"]) / last["value"], 1) if cap else None,
        "fuel_at": fuel_at,
        "fuel_mw": {f: round(fuels[f], 1) for f in FUELS if f in fuels},
        "wind_solar_share_pct": round(100 * ws / gen, 1) if gen > 0 else None,
        "net_load_mw": round(last["value"] - ws, 1) if fuels else None,
        "price_at": price_at,
        "rt_prices": {k: prices[k] for k in TODAY_HUBS if k in prices},
    }


def build_dashboard(records, warnings, history, totals, now, revised) -> dict:
    rejected = Counter(e for r in records for e in quality_errors(r))
    clean = [r for r in records if not quality_errors(r)]
    by_feed = Counter(r["feed"] for r in clean)

    points = [(row["temp_f"], row["demand_mw"]) for row in history.values()
              if row.get("temp_f") is not None and row.get("demand_mw") is not None]
    model = fit_degree_day_model(points)

    # History also holds ERCOT's forecast for the week ahead, so anything that
    # describes the past (fuel mix, history length) must stop at "now".
    hours = sorted(history)
    past = [h for h in hours if h <= now.isoformat()]
    fuel_hourly = [
        {"t": h, **{f: history[h].get(f"fuel:{f}") for f in FUELS}}
        for h in past[-48:] if any(history[h].get(f"fuel:{f}") is not None for f in FUELS)
    ]
    return {
        "generated_at": now.isoformat(),
        "source": "ERCOT public dashboards (supply-demand, fuel-mix, systemWidePrices) + NOAA METAR",
        "note": "Hourly batch refresh (GitHub Actions). The Kafka/Spark pipeline in this repo runs separately.",
        "latest": latest_snapshot(clean),
        "today": today_series(clean, now),
        "fuel_hourly": fuel_hourly,
        "weather_load": {
            "points": [{"t": h, "temp_f": history[h]["temp_f"], "demand_mw": history[h]["demand_mw"]}
                       for h in hours if history[h].get("temp_f") is not None
                       and history[h].get("demand_mw") is not None][-720:],
            "model": model,
            "stations": {k: v[0] for k, v in ERCOT_STATIONS.items()},
        },
        "stats": {
            "runs": totals["runs"],
            "records_this_run": dict(by_feed),
            "rejected_this_run": dict(rejected),
            "revised_cells_this_run": revised,
            "revised_cells_total": totals["revised"],
            "history_hours": len(past),
            "history_from": past[0] if past else None,
        },
        "warnings": warnings,
    }


def main() -> int:
    DOCS.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    history: dict = load("ercot_history.json", {})
    totals: dict = load("ercot_totals.json", {"runs": 0, "revised": 0})

    session = requests.Session()
    records, warnings = fetch_all(session)
    log(f"fetched {len(records)} records; warnings={len(warnings)}")
    if not records:
        log("nothing fetched; leaving the previous snapshot in place")
        for w in warnings:
            log(f"  {w}")
        return 1

    temps, temp_warnings = fetch_temps(session)
    warnings += temp_warnings
    clean = [r for r in records if not quality_errors(r)]
    history, revised = merge_history(history, hourly_rows(clean), temps, now)
    totals["runs"] += 1
    totals["revised"] += revised

    dashboard = build_dashboard(records, warnings, history, totals, now, revised)
    (DOCS / "ercot_dashboard.json").write_text(json.dumps(dashboard, separators=(",", ":")))
    (DOCS / "ercot_history.json").write_text(json.dumps(history, separators=(",", ":")))
    (DOCS / "ercot_totals.json").write_text(json.dumps(totals, indent=2))

    m = dashboard["weather_load"]["model"]
    log(f"history {len(history)} h, revised {revised} cells, model "
        f"{'n=%d r2=%s' % (m['n'], m['r2']) if m else 'not yet (need 48 h)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
