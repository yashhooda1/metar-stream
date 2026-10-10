"""
ERCOT grid feeds: fetch and normalize into one long record format.

Shared by the Kafka producer (ercot_producer.py) and the hourly GitHub Actions
refresh (scripts/refresh_ercot.py), so both paths parse ERCOT identically.

Sources are the JSON documents behind ercot.com's public dashboards. They need
no account or key, which is why they were chosen over the official ERCOT Public
API (api.ercot.com requires a registered subscription key). The tradeoff: they
are undocumented and can change without notice. Everything that knows their
shape lives in this one module, and every parser rejects what it does not
recognise instead of guessing, so a format change shows up as rejected records
and warnings rather than wrong numbers.

    supply-demand.json     5-min system demand, committed capacity, hourly forecast
    fuel-mix.json          5-min generation by fuel (MW)
    systemWidePrices.json  15-min real-time and hourly day-ahead settlement point prices

Every record has the same shape regardless of feed:

    feed           "demand" | "fuel" | "price"
    series         what is measured: "system", "forecast", a fuel, or a settlement point
    market         "RT" | "DAM" for prices, else None
    observed_at    interval timestamp as ERCOT reports it, converted to UTC ISO-8601
    value          MW for demand and fuel, $/MWh for price
    unit           "MW" | "USD/MWh"
    capacity_mw    demand feed only: committed capacity for the interval
    available_mw   demand feed only, when ERCOT includes it
    source_updated_at  the document's lastUpdated, UTC; newer wins on revision
    ingested_at    when this process read it, UTC
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Iterable

BASE_URL = "https://www.ercot.com/api/1/services/read/dashboards/"
FEEDS = {
    "demand": "supply-demand.json",
    "fuel": "fuel-mix.json",
    "price": "systemWidePrices.json",
}
USER_AGENT = "metar-stream-ercot/1.0 (portfolio project; github.com/yashhooda1/metar-stream)"

# Settlement points kept from systemWidePrices. hb* are trading hubs, lz* load zones.
SETTLEMENT_POINTS = {
    "hbHubAvg": "HB_HUBAVG",
    "hbBusAvg": "HB_BUSAVG",
    "hbHouston": "HB_HOUSTON",
    "hbNorth": "HB_NORTH",
    "hbSouth": "HB_SOUTH",
    "hbWest": "HB_WEST",
    "hbPan": "HB_PAN",
    "lzHouston": "LZ_HOUSTON",
    "lzNorth": "LZ_NORTH",
    "lzSouth": "LZ_SOUTH",
    "lzWest": "LZ_WEST",
    "lzAen": "LZ_AEN",
    "lzCps": "LZ_CPS",
    "lzLcra": "LZ_LCRA",
    "lzRaybn": "LZ_RAYBN",
}

# fuel-mix reports storage three ways: net ("Power Storage") plus its charging
# and discharging halves. Summing all three would count storage twice.
FUEL_BREAKDOWN_KEYS = {"Power Storage Charging", "Power Storage Discharging"}

# Price bounds for the quality gate. ERCOT's system-wide offer cap is $5,000/MWh
# and real-time prices can go negative; anything far outside is a parse error.
PRICE_MIN, PRICE_MAX = -1000.0, 10000.0
MW_MAX = 200_000.0

log = logging.getLogger("ercot-feeds")


def parse_ercot_time(text) -> datetime | None:
    """'2026-10-08 16:05:00-0500' -> aware datetime. The offset is honoured,
    so CST (-0600) and CDT (-0500) both land on the right UTC instant."""
    if not isinstance(text, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    return None


def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _num(v) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _record(feed, series, observed: datetime, value, unit, updated: str | None,
            ingested: str, market=None, **extra) -> dict:
    rec = {
        "feed": feed,
        "series": series,
        "market": market,
        "observed_at": to_utc_iso(observed),
        "value": value,
        "unit": unit,
        "capacity_mw": None,
        "available_mw": None,
        "source_updated_at": updated,
        "ingested_at": ingested,
    }
    rec.update(extra)
    return rec


def _updated(doc: dict) -> str | None:
    dt = parse_ercot_time(doc.get("lastUpdated"))
    return to_utc_iso(dt) if dt else None


def parse_supply_demand(doc: dict, ingested: str) -> tuple[list[dict], int]:
    """Actual 5-min demand ('system') and the hourly day-ahead forecast ('forecast').

    Returns (records, skipped). Rows flagged forecast=1 inside `data` are
    ERCOT's intraday projection, not actuals, so they are skipped rather than
    mixed into the actual series."""
    out, skipped = [], 0
    updated = _updated(doc)
    updated_dt = parse_ercot_time(doc.get("lastUpdated"))
    for row in doc.get("data") or []:
        if not isinstance(row, dict):
            skipped += 1
            continue
        ts = parse_ercot_time(row.get("timestamp"))
        demand = _num(row.get("demand"))
        # The document spans the whole operating day, so intervals later than
        # lastUpdated have no demand yet. Those are not parse failures.
        if demand is None and ts is not None and updated_dt is not None and ts > updated_dt:
            continue
        if ts is None or demand is None or row.get("forecast") not in (0, "0", None, False):
            skipped += 1
            continue
        out.append(_record("demand", "system", ts, demand, "MW", updated, ingested,
                           capacity_mw=_num(row.get("capacity")),
                           available_mw=_num(row.get("available"))))
    for row in doc.get("forecast") or []:
        if not isinstance(row, dict):
            skipped += 1
            continue
        ts = parse_ercot_time(row.get("timestamp"))
        demand = _num(row.get("forecastedDemand"))
        if ts is None or demand is None:
            skipped += 1
            continue
        out.append(_record("demand", "forecast", ts, demand, "MW", updated, ingested,
                           available_mw=_num(row.get("availCapGen"))))
    return out, skipped


def parse_fuel_mix(doc: dict, ingested: str) -> tuple[list[dict], int]:
    """data[date][timestamp][fuel] = {"gen": MW} -> one record per fuel per interval."""
    out, skipped = [], 0
    updated = _updated(doc)
    days = doc.get("data")
    if not isinstance(days, dict):
        return out, 1
    for intervals in days.values():
        if not isinstance(intervals, dict):
            skipped += 1
            continue
        for stamp, fuels in intervals.items():
            ts = parse_ercot_time(stamp)
            if ts is None or not isinstance(fuels, dict):
                skipped += 1
                continue
            for fuel, cell in fuels.items():
                if fuel in FUEL_BREAKDOWN_KEYS:
                    continue
                gen = _num(cell.get("gen")) if isinstance(cell, dict) else None
                if gen is None:
                    skipped += 1
                    continue
                out.append(_record("fuel", fuel, ts, gen, "MW", updated, ingested))
    return out, skipped


def parse_prices(doc: dict, ingested: str) -> tuple[list[dict], int]:
    """rtSppData (15-min, real time) and damSppData (hourly, day ahead), one
    record per settlement point per interval."""
    out, skipped = [], 0
    updated = _updated(doc)
    for key, market in (("rtSppData", "RT"), ("damSppData", "DAM")):
        for row in doc.get(key) or []:
            if not isinstance(row, dict):
                skipped += 1
                continue
            ts = parse_ercot_time(row.get("timestamp"))
            if ts is None:
                skipped += 1
                continue
            for src, point in SETTLEMENT_POINTS.items():
                price = _num(row.get(src))
                if price is None:
                    continue
                out.append(_record("price", point, ts, price, "USD/MWh", updated,
                                   ingested, market=market))
    return out, skipped


PARSERS = {"demand": parse_supply_demand, "fuel": parse_fuel_mix, "price": parse_prices}


def quality_errors(rec: dict) -> list[str]:
    """The silver contract, in plain Python. ercot_stream.py enforces the same
    rules in Spark; tests keep the two in step."""
    errs = []
    if rec.get("feed") not in PARSERS:
        errs.append("unknown_feed")
    if not rec.get("series"):
        errs.append("missing_series")
    if not rec.get("observed_at"):
        errs.append("missing_observed_at")
    v = rec.get("value")
    if not isinstance(v, (int, float)):
        errs.append("missing_value")
    elif rec.get("feed") == "price" and not PRICE_MIN <= v <= PRICE_MAX:
        errs.append("price_out_of_range")
    elif rec.get("feed") == "demand" and not 0 < v <= MW_MAX:
        errs.append("demand_out_of_range")
    elif rec.get("feed") == "fuel" and not -MW_MAX <= v <= MW_MAX:
        # Storage charging is legitimately negative.
        errs.append("generation_out_of_range")
    return errs


def fetch_all(session=None, timeout: float = 30) -> tuple[list[dict], list[str]]:
    """Fetch all three feeds. Per-feed isolation, same idea as METAR's tiles:
    one failing document costs that feed for one cycle, not the whole batch."""
    import requests  # local import keeps the parsers importable without it

    http = session or requests.Session()
    ingested = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []
    warnings: list[str] = []
    for feed, doc_name in FEEDS.items():
        try:
            resp = http.get(BASE_URL + doc_name, timeout=timeout,
                            headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            resp.raise_for_status()
            doc = resp.json()
        except (requests.RequestException, ValueError) as exc:
            warnings.append(f"{feed}: {exc}")
            log.warning("feed %s failed: %s", feed, exc)
            continue
        if not isinstance(doc, dict):
            warnings.append(f"{feed}: unexpected payload type {type(doc).__name__}")
            continue
        recs, skipped = PARSERS[feed](doc, ingested)
        if not recs:
            warnings.append(f"{feed}: parsed 0 records; the document format may have changed")
        if skipped:
            warnings.append(f"{feed}: skipped {skipped} unparseable entries")
        records.extend(recs)
    return records, warnings


def dedupe_key(rec: dict) -> str:
    return f"{rec['feed']}|{rec['series']}|{rec.get('market') or '-'}|{rec['observed_at']}"


# ── Weather vs load ──────────────────────────────────────────────────────────
# Approximate metro populations (2020 census, millions) inside ERCOT, used to
# weight airport temperatures into one system temperature. Weights are
# renormalised over whichever stations reported, so a missing airport shifts
# the blend instead of dragging it toward zero.
ERCOT_STATIONS = {
    "KDFW": ("Dallas-Fort Worth", 7.6),
    "KIAH": ("Houston", 7.1),
    "KSAT": ("San Antonio", 2.6),
    "KAUS": ("Austin", 2.3),
    "KCRP": ("Corpus Christi", 0.4),
    "KMAF": ("Midland-Odessa", 0.3),
}
BALANCE_POINT_F = 65.0  # conventional degree-day base


def weighted_temp_f(temps_c: dict[str, float]) -> float | None:
    num = den = 0.0
    for station, temp_c in temps_c.items():
        if station in ERCOT_STATIONS and isinstance(temp_c, (int, float)):
            w = ERCOT_STATIONS[station][1]
            num += w * (temp_c * 9 / 5 + 32)
            den += w
    return round(num / den, 2) if den else None


def fit_degree_day_model(points: Iterable[tuple[float, float]], min_points: int = 48) -> dict | None:
    """Least squares: demand = b0 + b1*CDD + b2*HDD, with CDD = max(T-65, 0)
    and HDD = max(65-T, 0) per hour. Returns coefficients, R^2 and n, or None
    when there are too few points or the system is singular (for example,
    every hour so far was above 65 F, so HDD is constant)."""
    pts = [(t, d) for t, d in points if isinstance(t, (int, float)) and isinstance(d, (int, float))]
    if len(pts) < min_points:
        return None
    rows = [(1.0, max(t - BALANCE_POINT_F, 0.0), max(BALANCE_POINT_F - t, 0.0)) for t, _ in pts]
    ys = [d for _, d in pts]

    # Estimate a degree-day term only when there are enough hours on that side
    # of the balance point. In a Texas October, a handful of cool nights would
    # otherwise produce a noisy (even negative) heating slope. Report a term
    # that was not estimated as None rather than as a number.
    min_regime = 24
    active = [0] + [j for j in (1, 2) if sum(r[j] > 0 for r in rows) >= min_regime]
    X = [[r[j] for j in active] for r in rows]
    k = len(active)
    xtx = [[sum(x[i] * x[j] for x in X) for j in range(k)] for i in range(k)]
    xty = [sum(x[i] * y for x, y in zip(X, ys)) for i in range(k)]
    beta = _solve(xtx, xty)
    if beta is None:
        return None
    coef = dict(zip(active, beta))
    preds = [sum(coef[j] * r[j] for j in active) for r in rows]
    mean_y = sum(ys) / len(ys)
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    ss_res = sum((y - p) ** 2 for y, p in zip(ys, preds))
    return {
        "intercept_mw": round(coef[0], 1),
        "mw_per_cooling_degree": round(coef[1], 1) if 1 in coef else None,
        "mw_per_heating_degree": round(coef[2], 1) if 2 in coef else None,
        "r2": round(1 - ss_res / ss_tot, 3) if ss_tot else None,
        "n": len(pts),
        "balance_point_f": BALANCE_POINT_F,
    }


def _solve(a: list[list[float]], b: list[float]) -> list[float] | None:
    """Gaussian elimination with partial pivoting, for the tiny normal equations."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-9:
            return None
        m[col], m[piv] = m[piv], m[col]
        for r in range(n):
            if r != col:
                f = m[r][col] / m[col][col]
                m[r] = [x - f * y for x, y in zip(m[r], m[col])]
    return [m[i][n] / m[i][i] for i in range(n)]
