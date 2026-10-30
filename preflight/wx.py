"""Pure weather parsing helpers. No network, no Spark, fully unit-testable.

Thresholds deliberately match dbt/macros/metar_flight_category.sql and the
ceiling parser in dbt/models/intermediate/int_metar__ceiling.sql, so a brief
and the dbt marts never disagree about what "IFR" means.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone

CEILING_COVERS = {"BKN", "OVC", "OVX", "VV"}
_CLOUD_RE = re.compile(r"\b(FEW|SCT|BKN|OVC|VV)(\d{3})\b")
_WX_TOKEN_RE = re.compile(
    r"(\+|-|VC)?(MI|PR|BC|DR|BL|SH|TS|FZ)?"
    r"(DZ|RA|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|FC|SS|DS)*"
)

# Weather tokens that end the conversation for a light GA aircraft.
NO_GO_WX = {
    "TS": "thunderstorm",
    "FC": "funnel cloud / tornado",
    "GR": "hail",
    "FZRA": "freezing rain",
    "FZDZ": "freezing drizzle",
    "SS": "sandstorm",
    "DS": "duststorm",
}
CAUTION_WX = {
    "VCTS": "thunderstorm in the vicinity",
    "SN": "snow",
    "FG": "fog",
    "+RA": "heavy rain",
    "BR": "mist",
    "SH": "showers",
}


def parse_visibility(value) -> float | None:
    """Statute miles from AWC JSON ('10+', 6, '1 1/2', 'P6SM', '1/4')."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    text = str(value).strip().upper().replace("SM", "")
    if not text:
        return None
    # "6+" / "P6SM" / "10+" mean "greater than": TAFs cap at P6SM, so reading
    # it as exactly 6 would put every clear-sky TAF "near" a 5 sm minimum.
    if text.endswith("+") or text.startswith("P"):
        try:
            return max(10.0, float(text.strip("P+")))
        except ValueError:
            return None
    try:
        total = 0.0
        for part in text.split():
            if "/" in part:
                num, den = part.split("/", 1)
                total += float(num) / float(den)
            else:
                total += float(part)
        return total
    except (ValueError, ZeroDivisionError):
        return None


def ceiling_from_layers(layers) -> int | None:
    """Lowest BKN/OVC/VV base in feet AGL from AWC [{'cover','base'}] layers."""
    bases = [
        int(layer["base"])
        for layer in layers or []
        if isinstance(layer, dict)
        and str(layer.get("cover", "")).upper() in CEILING_COVERS
        and isinstance(layer.get("base"), (int, float))
    ]
    return min(bases) if bases else None


def ceiling_from_raw(raw: str | None) -> int | None:
    """Same rules as int_metar__ceiling.sql: ignore RMK, ignore '///' heights."""
    if not raw:
        return None
    body = raw.split(" RMK ", 1)[0]
    bases = [
        int(height) * 100
        for cover, height in _CLOUD_RE.findall(body)
        if cover in CEILING_COVERS
    ]
    return min(bases) if bases else None


def flight_category(ceiling_ft: int | None, visibility_sm: float | None) -> str:
    """FAA category; a null ceiling is unlimited (matches the dbt macro)."""
    if ceiling_ft is None and visibility_sm is None:
        return "UNKNOWN"
    cig = 99999 if ceiling_ft is None else ceiling_ft
    vis = 99.0 if visibility_sm is None else visibility_sm
    if cig < 500 or vis < 1:
        return "LIFR"
    if cig < 1000 or vis < 3:
        return "IFR"
    if cig <= 3000 or vis <= 5:
        return "MVFR"
    return "VFR"


def hazards(wx_string: str | None) -> tuple[list[str], list[str]]:
    """Split a present-weather string into (no_go, caution) descriptions."""
    if not wx_string:
        return [], []
    # Accepts either an AWC wxString ("-TSRA BR") or a whole raw METAR; only
    # tokens shaped like present-weather groups count, so station ids such as
    # KDSM or KFCM never read as "duststorm" or "funnel cloud".
    body = str(wx_string).upper().split(" RMK ", 1)[0]
    tokens = [t for t in body.split() if _WX_TOKEN_RE.fullmatch(t)]
    no_go, caution = [], []
    for token in tokens:
        if token.startswith("VC"):
            if "TS" in token:
                caution.append(CAUTION_WX["VCTS"])
            continue
        hit = next((label for code, label in NO_GO_WX.items() if code in token), None)
        if hit:
            no_go.append(f"{hit} ({token})")
            continue
        soft = next((label for code, label in CAUTION_WX.items() if code in token), None)
        if soft:
            caution.append(f"{soft} ({token})")
    return sorted(set(no_go)), sorted(set(caution))


def to_utc(value) -> datetime | None:
    """Epoch seconds or ISO-8601 → aware UTC datetime."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        text = str(value).strip()
        if text.isdigit():
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def distance_nm(lat1, lon1, lat2, lon2) -> float | None:
    """Great-circle distance in nautical miles."""
    if None in (lat1, lon1, lat2, lon2):
        return None
    r_nm = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r_nm * math.asin(math.sqrt(a))


def taf_periods_in_window(taf: dict | None, start: datetime, end: datetime) -> list[dict]:
    """Forecast groups from an AWC TAF that overlap [start, end]."""
    if not taf:
        return []
    out = []
    for period in taf.get("fcsts") or []:
        t_from, t_to = to_utc(period.get("timeFrom")), to_utc(period.get("timeTo"))
        if t_from is None or t_to is None:
            continue
        if t_from < end and t_to > start:
            out.append(period)
    return out
