"""Where briefing data comes from.

METAR  NOAA Aviation Weather Center API (same feed metar_producer.py polls),
       or the Silver Delta table when --lake-path points at a running lake.
TAF    NOAA Aviation Weather Center API.
NOTAM  FAA NOTAM API (free key: api.faa.gov). Without FAA_CLIENT_ID /
       FAA_CLIENT_SECRET, NOTAMs come back as None and the brief says
       "not checked" rather than pretending there are none.

Every METAR is mapped to the Silver row shape (station_id, observed_at,
wind_speed_kt, visibility_sm, raw_text, ...) so the rules engine sees one
schema whichever source produced it.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import requests

from . import wx

log = logging.getLogger("preflight.sources")

AWC = "https://aviationweather.gov/api/data"
FAA_NOTAM = "https://external-api.faa.gov/notamapi/v1/notams"
HEADERS = {"User-Agent": "metar-stream preflight-brief (github.com/yashhooda1/metar-stream)"}
TIMEOUT = 10


def silver_shape(obs: dict) -> dict | None:
    """AWC METAR JSON → Silver-shaped record (+ clouds / wx for the brief)."""
    if not isinstance(obs, dict) or not isinstance(obs.get("icaoId"), str):
        return None
    observed = wx.to_utc(obs.get("obsTime"))
    clouds = obs.get("clouds") or []
    return {
        "station_id": obs["icaoId"].strip().upper(),
        "observed_at": observed.isoformat() if observed else None,
        "name": obs.get("name"),
        "lat": obs.get("lat"),
        "lon": obs.get("lon"),
        "temp_c": obs.get("temp"),
        "dewpoint_c": obs.get("dewp"),
        "wind_dir_deg": obs.get("wdir"),
        "wind_speed_kt": obs.get("wspd"),
        "wind_gust_kt": obs.get("wgst"),
        "visibility_sm": obs.get("visib"),
        "altimeter_hpa": obs.get("altim"),
        "flight_category": obs.get("fltCat"),
        "raw_text": obs.get("rawOb"),
        "clouds": clouds,
        "ceiling_ft": wx.ceiling_from_layers(clouds),
        "wx_string": obs.get("wxString"),
    }


def _get_json(url, params):
    resp = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    if resp.status_code == 204 or not resp.content:
        return []
    return resp.json()


def fetch_metars(ids: list[str]) -> dict[str, dict]:
    rows = _get_json(f"{AWC}/metar", {"ids": ",".join(ids), "format": "json", "hours": 2})
    latest: dict[str, dict] = {}
    for row in rows if isinstance(rows, list) else []:
        rec = silver_shape(row)
        if not rec:
            continue
        prev = latest.get(rec["station_id"])
        if prev is None or (rec["observed_at"] or "") > (prev["observed_at"] or ""):
            latest[rec["station_id"]] = rec
    return latest


def fetch_tafs(ids: list[str] | None = None, bbox: str | None = None) -> dict[str, dict]:
    params = {"format": "json"}
    if ids:
        params["ids"] = ",".join(ids)
    if bbox:
        params["bbox"] = bbox
    rows = _get_json(f"{AWC}/taf", params)
    out = {}
    for row in rows if isinstance(rows, list) else []:
        icao = str(row.get("icaoId", "")).upper()
        if icao and icao not in out:  # API returns newest first
            out[icao] = row
    return out


def fetch_notams(icao: str) -> list[str] | None:
    cid, secret = os.getenv("FAA_CLIENT_ID"), os.getenv("FAA_CLIENT_SECRET")
    if not cid or not secret:
        return None
    try:
        resp = requests.get(
            FAA_NOTAM,
            params={"icaoLocation": icao, "responseFormatType": "geoJson", "pageSize": 100},
            headers={**HEADERS, "client_id": cid, "client_secret": secret},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
    except (requests.RequestException, ValueError) as exc:
        log.warning("NOTAM fetch failed for %s: %s", icao, exc)
        return None
    texts = []
    for item in items:
        notam = (((item or {}).get("properties") or {}).get("coreNOTAMData") or {}).get("notam") or {}
        text = notam.get("text")
        if text:
            texts.append(f"{notam.get('number', '')} {text}".strip())
    return texts


def nearby_taf_stations(lat: float, lon: float, radius_nm: float = 50) -> list[dict]:
    """TAF-issuing stations within radius_nm, nearest first."""
    deg = radius_nm / 60.0
    bbox = f"{lat - deg:.2f},{lon - deg * 1.4:.2f},{lat + deg:.2f},{lon + deg * 1.4:.2f}"
    tafs = fetch_tafs(bbox=bbox)
    out = []
    for icao, taf in tafs.items():
        dist = wx.distance_nm(lat, lon, taf.get("lat"), taf.get("lon"))
        if dist is not None and dist <= radius_nm:
            out.append({"station": icao, "distance_nm": round(dist, 1), "name": taf.get("name"), "taf": taf})
    return sorted(out, key=lambda s: s["distance_nm"])


def read_lake_metars(lake_path: str, ids: list[str]) -> dict[str, dict]:
    """Latest Silver observation per station, straight from the Delta lake."""
    from pyspark.sql import SparkSession, Window, functions as F  # heavy; import lazily

    spark = (
        SparkSession.builder.appName("preflight-brief")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    df = spark.read.format("delta").load(os.path.join(lake_path, "silver", "metar_observations"))
    w = Window.partitionBy("station_id").orderBy(F.col("observed_at").desc())
    rows = (
        df.where(F.col("station_id").isin(ids))
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .collect()
    )
    out = {}
    for row in rows:
        rec = row.asDict()
        observed = rec.get("observed_at")
        if isinstance(observed, datetime):
            rec["observed_at"] = observed.replace(tzinfo=observed.tzinfo or timezone.utc).isoformat()
        rec["ceiling_ft"] = wx.ceiling_from_raw(rec.get("raw_text"))
        out[rec["station_id"]] = rec
    return out


def gather(route: list[str], lake_path: str | None = None) -> dict[str, dict]:
    """METAR + TAF + NOTAMs for every station on the route."""
    ids = list(dict.fromkeys(route))
    metars = read_lake_metars(lake_path, ids) if lake_path else fetch_metars(ids)
    tafs = fetch_tafs(ids)
    data = {}
    for icao in ids:
        metar, taf = metars.get(icao), tafs.get(icao)
        data[icao] = {
            "metar": metar,
            "taf": taf,
            "notams": fetch_notams(icao),
            "lat": (metar or {}).get("lat") or (taf or {}).get("lat"),
            "lon": (metar or {}).get("lon") or (taf or {}).get("lon"),
        }
    return data
