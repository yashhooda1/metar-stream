"""Deterministic go / caution / no-go evaluation.

This is the part of the agent that makes decisions, and it is plain Python on
purpose: every flag cites the observation or forecast group that triggered it,
the thresholds are visible in one dataclass, and the same input always gives
the same verdict. The LLM writes prose around these flags; it never sets them.

Not an official briefing. A pilot still needs 1800wxbrief.com / Leidos or an
equivalent approved source before flight.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from . import wx

GO, CAUTION, NO_GO = "GO", "CAUTION", "NO_GO"
_RANK = {GO: 0, CAUTION: 1, NO_GO: 2}

STALE_METAR_MIN = 90
CURRENT_WINDOW = timedelta(hours=2)   # METAR is authoritative this close to now
TAF_WINDOW = timedelta(hours=1)       # +/- around the station's ETD/ETA
SUBSTITUTE_TAF_MAX_NM = 50


@dataclass(frozen=True)
class Minimums:
    name: str
    ceiling_ft: int
    visibility_sm: float
    wind_kt: int
    gust_kt: int
    gust_spread_kt: int
    ifr: bool = False
    # Caution band: within this margin of a limit is "caution", not "go".
    ceiling_margin_ft: int = 1000
    visibility_margin_sm: float = 2.0
    wind_margin_kt: int = 5


PROFILES = {
    # A conservative low-time VFR pilot's personal minimums.
    "vfr_conservative": Minimums("vfr_conservative", 3000, 5, 15, 20, 10),
    # Legal-ish basic VFR (14 CFR 91.155 class E) plus sane wind limits.
    "vfr_standard": Minimums("vfr_standard", 1000, 3, 20, 25, 15, ceiling_margin_ft=1500),
    # Instrument-rated: approach-type minimums plus the 1-2-3 alternate rule.
    "ifr": Minimums("ifr", 400, 1, 25, 30, 15, ifr=True, ceiling_margin_ft=300,
                    visibility_margin_sm=1.0),
}


@dataclass
class Flag:
    severity: str
    station: str
    source: str          # METAR | TAF | NOTAM | DATA
    code: str            # stable machine code, e.g. ceiling_below_min
    message: str


@dataclass
class StationBrief:
    station: str
    role: str                        # departure | enroute | destination
    eval_time: str
    category_now: str = "UNKNOWN"
    worst_forecast_category: str | None = None
    raw_metar: str | None = None
    raw_taf: str | None = None
    taf_source: str | None = None    # differs from station when a nearby TAF is used
    notams_checked: bool = False
    verdict: str = GO
    flags: list[Flag] = field(default_factory=list)


def worst(*severities: str) -> str:
    return max(severities, key=_RANK.__getitem__, default=GO)


def _cat_rank(cat: str | None) -> int:
    return {"VFR": 0, "MVFR": 1, "IFR": 2, "LIFR": 3}.get(cat or "", -1)


def _check_conditions(*, station, source, mins, ceiling, vis, wind, gust, wx_string,
                      soften=False, label=""):
    """Flags for one set of conditions (a METAR or one TAF group)."""
    flags: list[Flag] = []
    tag = f" {label}" if label else ""

    def add(sev, code, msg):
        if soften and sev == NO_GO:
            sev = CAUTION
        flags.append(Flag(sev, station, source, code, msg + tag))

    if ceiling is not None:
        if ceiling < mins.ceiling_ft:
            add(NO_GO, "ceiling_below_min", f"ceiling {ceiling} ft < {mins.ceiling_ft} ft minimum")
        elif ceiling < mins.ceiling_ft + mins.ceiling_margin_ft:
            add(CAUTION, "ceiling_near_min", f"ceiling {ceiling} ft is close to the {mins.ceiling_ft} ft minimum")
    if vis is not None:
        if vis < mins.visibility_sm:
            add(NO_GO, "visibility_below_min", f"visibility {vis:g} sm < {mins.visibility_sm:g} sm minimum")
        elif vis < mins.visibility_sm + mins.visibility_margin_sm:
            add(CAUTION, "visibility_near_min", f"visibility {vis:g} sm is close to the {mins.visibility_sm:g} sm minimum")
    if wind is not None:
        if wind > mins.wind_kt:
            add(NO_GO, "wind_above_max", f"wind {wind} kt > {mins.wind_kt} kt limit")
        elif wind > mins.wind_kt - mins.wind_margin_kt:
            add(CAUTION, "wind_near_max", f"wind {wind} kt is close to the {mins.wind_kt} kt limit")
    if gust is not None:
        if gust > mins.gust_kt:
            add(NO_GO, "gust_above_max", f"gusts {gust} kt > {mins.gust_kt} kt limit")
        if wind is not None and gust - wind > mins.gust_spread_kt:
            add(CAUTION, "gust_spread", f"gust spread {gust - wind} kt (> {mins.gust_spread_kt} kt) — expect turbulence on final")
    no_go_wx, caution_wx = wx.hazards(wx_string)
    for item in no_go_wx:
        add(NO_GO, "hazardous_weather", item)
    for item in caution_wx:
        add(CAUTION, "weather_present", item)
    return flags


def evaluate_metar(station, metar, mins, *, now, eval_time) -> tuple[list[Flag], str]:
    if not metar:
        return [Flag(CAUTION, station, "DATA", "no_metar", "no current METAR — station may not report")], "UNKNOWN"

    ceiling = metar.get("ceiling_ft")
    if ceiling is None:
        ceiling = wx.ceiling_from_layers(metar.get("clouds")) or wx.ceiling_from_raw(metar.get("raw_text"))
    vis = wx.parse_visibility(metar.get("visibility_sm"))
    category = metar.get("flight_category") or wx.flight_category(ceiling, vis)

    # Far from the ETD/ETA, current weather is a trend, not the answer.
    soften = abs(eval_time - now) > CURRENT_WINDOW
    flags = _check_conditions(
        station=station, source="METAR", mins=mins, ceiling=ceiling, vis=vis,
        wind=metar.get("wind_speed_kt"), gust=metar.get("wind_gust_kt"),
        wx_string=metar.get("wx_string") or metar.get("raw_text"),
        soften=soften, label="(current)" if not soften else "(current, trend only)",
    )
    observed = wx.to_utc(metar.get("observed_at"))
    if observed is not None:
        age_min = (now - observed).total_seconds() / 60
        if age_min > STALE_METAR_MIN:
            flags.append(Flag(CAUTION, station, "DATA", "stale_metar", f"latest METAR is {age_min:.0f} min old"))
    return flags, category


def evaluate_taf(station, taf, mins, *, eval_time, role, taf_source=None):
    """Flags for TAF groups overlapping the station's time window."""
    window_start, window_end = eval_time - TAF_WINDOW, eval_time + TAF_WINDOW
    periods = wx.taf_periods_in_window(taf, window_start, window_end)
    if not periods:
        sev = CAUTION if role in ("departure", "destination") else GO
        flags = [Flag(sev, station, "DATA", "no_taf", "no TAF covers this time — forecast unknown")] if sev != GO else []
        return flags, None

    flags: list[Flag] = []
    worst_cat = None
    alternate_needed = False
    for period in periods:
        change = str(period.get("fcstChange") or "BASE").upper()
        probabilistic = change in ("TEMPO", "PROB") or period.get("probability")
        ceiling = wx.ceiling_from_layers(period.get("clouds"))
        vis = wx.parse_visibility(period.get("visib"))
        cat = wx.flight_category(ceiling, vis)
        if _cat_rank(cat) > _cat_rank(worst_cat):
            worst_cat = cat
        t_from = wx.to_utc(period.get("timeFrom"))
        label = f"(TAF {change} {t_from:%d/%H%MZ})" if t_from else f"(TAF {change})"
        if taf_source and taf_source != station:
            label = label[:-1] + f" via {taf_source})"
        flags += _check_conditions(
            station=station, source="TAF", mins=mins, ceiling=ceiling, vis=vis,
            wind=period.get("wspd"), gust=period.get("wgst"),
            wx_string=period.get("wxString"), soften=bool(probabilistic), label=label,
        )
        # 14 CFR 91.169 "1-2-3": alternate needed if ceiling < 2000 or vis < 3
        # from 1 hr before to 1 hr after ETA.
        if mins.ifr and role == "destination" and not probabilistic:
            if (ceiling is not None and ceiling < 2000) or (vis is not None and vis < 3):
                alternate_needed = True
    if alternate_needed:
        flags.append(Flag(CAUTION, station, "TAF", "alternate_required",
                          "1-2-3 rule: file an alternate (ceiling < 2000 ft or vis < 3 sm within 1 hr of ETA)"))
    return flags, worst_cat


NOTAM_NO_GO = ("AD AP CLSD", "AERODROME CLOSED", "AIRPORT CLOSED", "AD CLSD")
NOTAM_CAUTION = ("RWY", "CLSD", "U/S", "OTS", "TFR", "LGT", "ILS", "GPS")


def evaluate_notams(station, notams, mins):
    if notams is None:
        return [Flag(CAUTION, station, "NOTAM", "notams_not_checked",
                     "NOTAMs not checked — pull them from an official source")]
    flags = []
    for text in notams:
        upper = text.upper()
        if any(key in upper for key in NOTAM_NO_GO):
            flags.append(Flag(NO_GO, station, "NOTAM", "airport_closed", text[:200]))
        elif "RWY" in upper and "CLSD" in upper:
            flags.append(Flag(CAUTION, station, "NOTAM", "runway_closed", text[:200]))
        elif "TFR" in upper:
            flags.append(Flag(CAUTION, station, "NOTAM", "tfr", text[:200]))
        elif mins.ifr and ("ILS" in upper or "GPS" in upper) and ("U/S" in upper or "OTS" in upper):
            flags.append(Flag(CAUTION, station, "NOTAM", "approach_unavailable", text[:200]))
    return flags


def station_times(route, etd, ete_hours):
    """ETD at the first station, ETA at the last, linear in between."""
    if len(route) == 1:
        return [etd]
    step = ete_hours / (len(route) - 1)
    return [etd + timedelta(hours=step * i) for i in range(len(route))]


def brief_route(route, data, *, profile="vfr_conservative", etd=None, ete_hours=1.0,
                now=None, taf_substitutes=None):
    """Evaluate every station on the route.

    route            ['KAUS', 'KCLL', 'KHOU']  departure → enroute → destination
    data[icao]       {'metar': {...}|None, 'taf': {...}|None, 'notams': [str]|None,
                      'lat': float, 'lon': float}
    taf_substitutes  {'KXXX': 'KYYY'} — use KYYY's TAF where KXXX has none.
                     Rejected unless KYYY is fetched and within 50 nm.
    """
    now = now or datetime.now(timezone.utc)
    etd = etd or now
    mins = PROFILES[profile] if isinstance(profile, str) else profile
    taf_substitutes = dict(taf_substitutes or {})
    times = station_times(route, etd, ete_hours)

    stations: list[StationBrief] = []
    for idx, (icao, when) in enumerate(zip(route, times)):
        role = "departure" if idx == 0 else "destination" if idx == len(route) - 1 else "enroute"
        if len(route) == 1:
            role = "departure"
        entry = data.get(icao) or {}
        metar = entry.get("metar")
        brief = StationBrief(station=icao, role=role, eval_time=when.isoformat(timespec="minutes"))
        brief.raw_metar = (metar or {}).get("raw_text")

        metar_flags, brief.category_now = evaluate_metar(icao, metar, mins, now=now, eval_time=when)

        taf, taf_source = entry.get("taf"), icao
        sub = taf_substitutes.get(icao)
        sub_flags: list[Flag] = []
        if not taf and sub:
            sub_entry = data.get(sub) or {}
            dist = wx.distance_nm(entry.get("lat"), entry.get("lon"), sub_entry.get("lat"), sub_entry.get("lon"))
            if sub_entry.get("taf") and dist is not None and dist <= SUBSTITUTE_TAF_MAX_NM:
                taf, taf_source = sub_entry["taf"], sub
                sub_flags.append(Flag(CAUTION, icao, "DATA", "nearby_taf",
                                      f"no TAF here — using {sub} TAF ({dist:.0f} nm away)"))
            else:
                sub_flags.append(Flag(CAUTION, icao, "DATA", "substitute_rejected",
                                      f"proposed TAF substitute {sub} rejected (missing or > {SUBSTITUTE_TAF_MAX_NM} nm)"))
        brief.raw_taf = (taf or {}).get("rawTAF")
        brief.taf_source = taf_source if taf else None

        taf_flags, brief.worst_forecast_category = evaluate_taf(
            icao, taf, mins, eval_time=when, role=role, taf_source=taf_source)
        notams = entry.get("notams")
        brief.notams_checked = notams is not None
        notam_flags = evaluate_notams(icao, notams, mins) if role != "enroute" else []

        brief.flags = metar_flags + sub_flags + taf_flags + notam_flags
        brief.verdict = worst(*(f.severity for f in brief.flags))
        stations.append(brief)

    overall = worst(*(s.verdict for s in stations))
    return {
        "route": list(route),
        "profile": asdict(mins),
        "etd": etd.isoformat(timespec="minutes"),
        "ete_hours": ete_hours,
        "generated_at": now.isoformat(timespec="seconds"),
        "verdict": overall,
        "stations": [asdict(s) for s in stations],
        "disclaimer": "Not an official weather briefing. Obtain a standard briefing "
                      "(1800wxbrief.com) and use your own judgement before flight.",
    }
