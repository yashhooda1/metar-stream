"""The agent loop — written against the raw Anthropic Messages API, no framework.

    gather()  ─▶  rules.brief_route()  ─▶  model + tools  ─▶  final brief
                         ▲                       │
                         └── use_nearby_taf ─────┘   (re-scored by rules)

Tools the model can call:
    find_nearby_taf      list TAF stations near a route airport that has none
    use_nearby_taf       adopt one of them; rules re-score the route and the
                         tool result returns the updated flags
    get_station_weather  METAR + TAF for extra stations (alternates, diversions)
    submit_brief         final narrative — ends the loop

The model is told the verdict and cannot change it: brief() always reports
rules.brief_route()'s verdict, and the narrative is shown beneath it.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Callable

import requests

from . import rules, sources, wx

log = logging.getLogger("preflight.agent")

API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = os.getenv("PREFLIGHT_MODEL", "claude-sonnet-5-5")
MAX_STEPS = 6
MAX_EXTRA_STATIONS = 5

SYSTEM = """You are a careful flight-planning assistant producing a preflight weather \
brief for a general-aviation pilot. You are given the route, the pilot's minimums, \
raw METARs/TAFs, and flags computed by a deterministic rules engine.

Rules you must follow:
- The VERDICT comes from the rules engine. Never contradict, soften, or upgrade it. \
If the verdict is NO_GO, say plainly that the flight should not go as planned.
- Only state facts present in the data or tool results. Never invent weather, NOTAMs, \
or station names. If something was not checked (e.g. NOTAMs), say so.
- If the departure or destination has no TAF, call find_nearby_taf and, if a \
representative station within 50 nm exists, call use_nearby_taf before writing.
- You may call get_station_weather for up to 5 extra stations (e.g. a sensible \
alternate when the destination is marginal).
- Finish by calling submit_brief exactly once.

Brief style: pilot-to-pilot, concise, organised as Departure / Enroute / Destination / \
Alternates / Bottom line. Use standard aviation abbreviations. Times in Zulu. \
End with: "Not an official briefing — get a standard brief from 1800wxbrief.com."
"""

TOOLS = [
    {
        "name": "find_nearby_taf",
        "description": "List TAF-issuing stations within radius_nm of a route airport, nearest first.",
        "input_schema": {
            "type": "object",
            "properties": {
                "station": {"type": "string", "description": "ICAO id on the route"},
                "radius_nm": {"type": "number", "default": 40},
            },
            "required": ["station"],
        },
    },
    {
        "name": "use_nearby_taf",
        "description": "Use substitute's TAF for station (must be within 50 nm and already found). "
                       "Returns the re-scored route.",
        "input_schema": {
            "type": "object",
            "properties": {"station": {"type": "string"}, "substitute": {"type": "string"}},
            "required": ["station", "substitute"],
        },
    },
    {
        "name": "get_station_weather",
        "description": "Current METAR and TAF for extra stations not on the route (max 5 total).",
        "input_schema": {
            "type": "object",
            "properties": {"stations": {"type": "array", "items": {"type": "string"}}},
            "required": ["stations"],
        },
    },
    {
        "name": "submit_brief",
        "description": "Submit the final narrative brief (markdown). Ends the session.",
        "input_schema": {
            "type": "object",
            "properties": {
                "narrative": {"type": "string"},
                "alternates_considered": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["narrative"],
        },
    },
]


def call_anthropic(payload: dict) -> dict:
    key = os.environ["ANTHROPIC_API_KEY"]
    resp = requests.post(
        API_URL,
        headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json=payload,
        timeout=120,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"anthropic {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _compact(result: dict) -> dict:
    """What the model sees of a rules result — flags and raw reports, no noise."""
    return {
        "verdict": result["verdict"],
        "etd": result["etd"],
        "ete_hours": result["ete_hours"],
        "minimums": {k: result["profile"][k] for k in ("name", "ceiling_ft", "visibility_sm", "wind_kt", "gust_kt", "ifr")},
        "stations": [
            {
                "station": s["station"], "role": s["role"], "eval_time": s["eval_time"],
                "verdict": s["verdict"], "category_now": s["category_now"],
                "worst_forecast_category": s["worst_forecast_category"],
                "metar": s["raw_metar"], "taf": s["raw_taf"], "taf_source": s["taf_source"],
                "notams_checked": s["notams_checked"],
                "flags": [f"{f['severity']} {f['source']} {f['code']}: {f['message']}" for f in s["flags"]],
            }
            for s in result["stations"]
        ],
    }


def fallback_narrative(result: dict) -> str:
    """Deterministic brief for --no-llm or when the model is unavailable."""
    lines = [f"**Verdict: {result['verdict'].replace('_', '-')}** "
             f"({result['profile']['name']}, ETD {result['etd']}, ETE {result['ete_hours']:g} h)", ""]
    for s in result["stations"]:
        lines.append(f"**{s['role'].title()} {s['station']}** — {s['verdict'].replace('_', '-')}, "
                     f"now {s['category_now']}" + (f", forecast worst {s['worst_forecast_category']}" if s["worst_forecast_category"] else ""))
        if s["raw_metar"]:
            lines.append(f"  METAR `{s['raw_metar']}`")
        if s["raw_taf"]:
            lines.append(f"  TAF{' (' + s['taf_source'] + ')' if s['taf_source'] != s['station'] else ''} `{s['raw_taf']}`")
        for f in s["flags"]:
            lines.append(f"  - {f['severity'].replace('_', '-')}: {f['message']}")
        lines.append("")
    lines.append(result["disclaimer"])
    return "\n".join(lines)


class BriefingAgent:
    def __init__(self, route, *, profile="vfr_conservative", etd=None, ete_hours=1.0,
                 data=None, lake_path=None, model=DEFAULT_MODEL,
                 call_model: Callable[[dict], dict] | None = None,
                 fetch_nearby=sources.nearby_taf_stations,
                 fetch_extra=None, now=None):
        self.route = [s.strip().upper() for s in route]
        self.profile, self.etd, self.ete_hours, self.now = profile, etd, ete_hours, now
        self.data = data if data is not None else sources.gather(self.route, lake_path)
        self.model = model
        self.call_model = call_model or call_anthropic
        self.fetch_nearby = fetch_nearby
        self.fetch_extra = fetch_extra or (lambda ids: sources.gather(ids))
        self.substitutes: dict[str, str] = {}
        self.extra: dict[str, dict] = {}
        self.trace: list[dict] = []

    # ── rules ────────────────────────────────────────────────────────────
    def score(self) -> dict:
        return rules.brief_route(self.route, self.data, profile=self.profile, etd=self.etd,
                                 ete_hours=self.ete_hours, now=self.now,
                                 taf_substitutes=self.substitutes)

    # ── tools ────────────────────────────────────────────────────────────
    def _tool(self, name: str, args: dict):
        if name == "find_nearby_taf":
            icao = str(args.get("station", "")).upper()
            entry = self.data.get(icao) or {}
            if entry.get("lat") is None:
                return {"error": f"no coordinates for {icao}"}
            radius = min(float(args.get("radius_nm") or 40), rules.SUBSTITUTE_TAF_MAX_NM)
            found = [s for s in self.fetch_nearby(entry["lat"], entry["lon"], radius) if s["station"] != icao]
            for s in found:  # remember so use_nearby_taf can validate against real data
                taf = s["taf"]
                self.data.setdefault(s["station"], {"metar": None, "taf": taf, "notams": None,
                                                    "lat": taf.get("lat"), "lon": taf.get("lon")})
            return [{"station": s["station"], "distance_nm": s["distance_nm"], "name": s.get("name"),
                     "taf": s["taf"].get("rawTAF")} for s in found[:6]]
        if name == "use_nearby_taf":
            icao, sub = str(args.get("station", "")).upper(), str(args.get("substitute", "")).upper()
            if icao not in self.route:
                return {"error": f"{icao} is not on the route"}
            self.substitutes[icao] = sub
            return _compact(self.score())
        if name == "get_station_weather":
            ids = [str(s).upper() for s in args.get("stations", [])][: MAX_EXTRA_STATIONS - len(self.extra)]
            if not ids:
                return {"error": "extra-station budget used up"}
            fetched = self.fetch_extra(ids)
            self.extra.update(fetched)
            return {icao: {"metar": (d.get("metar") or {}).get("raw_text"),
                           "taf": (d.get("taf") or {}).get("rawTAF")} for icao, d in fetched.items()}
        return {"error": f"unknown tool {name}"}

    # ── loop ─────────────────────────────────────────────────────────────
    def run(self, use_llm: bool = True) -> dict:
        result = self.score()
        no_key = self.call_model is call_anthropic and not os.getenv("ANTHROPIC_API_KEY")
        if not use_llm or no_key:
            return {**result, "narrative": fallback_narrative(result), "model": None, "trace": []}

        messages = [{"role": "user", "content": "Brief this flight.\n\n" + json.dumps(_compact(result), indent=1)}]
        narrative, alternates = None, []
        for step in range(MAX_STEPS):
            try:
                resp = self.call_model({"model": self.model, "max_tokens": 6000, "system": SYSTEM,
                                        "tools": TOOLS, "messages": messages})
            except Exception as exc:  # network / quota — degrade, never crash the brief
                log.warning("model call failed: %s", exc)
                break
            content = resp.get("content") or []
            messages.append({"role": "assistant", "content": content})  # keep thinking blocks intact
            calls = [b for b in content if b.get("type") == "tool_use"]
            if not calls:
                text = "\n".join(b.get("text", "") for b in content if b.get("type") == "text").strip()
                narrative = narrative or text or None
                break
            results = []
            for call in calls:
                if call["name"] == "submit_brief":
                    narrative = str(call["input"].get("narrative", "")).strip()
                    alternates = call["input"].get("alternates_considered") or []
                    out = {"ok": True}
                else:
                    out = self._tool(call["name"], call.get("input") or {})
                self.trace.append({"step": step, "tool": call["name"], "input": call.get("input")})
                results.append({"type": "tool_result", "tool_use_id": call["id"],
                                "content": json.dumps(out, default=str)[:12000]})
            messages.append({"role": "user", "content": results})
            if narrative:
                break

        final = self.score()  # authoritative — includes any adopted substitutes
        return {
            **final,
            "narrative": narrative or fallback_narrative(final),
            "alternates_considered": alternates,
            "extra_stations": {k: {"metar": (v.get("metar") or {}).get("raw_text"),
                                   "taf": (v.get("taf") or {}).get("rawTAF")} for k, v in self.extra.items()},
            "model": self.model if narrative else None,
            "trace": self.trace,
        }


def to_markdown(brief: dict) -> str:
    badge = {"GO": "🟢 GO", "CAUTION": "🟡 CAUTION", "NO_GO": "🔴 NO-GO"}[brief["verdict"]]
    head = f"# Preflight brief: {' → '.join(brief['route'])}\n\n**{badge}** — rules engine verdict " \
           f"({brief['profile']['name']})\n\n"
    if brief.get("model"):
        head += f"_Narrative by {brief['model']}; flags by rules.py._\n\n"
    return head + brief["narrative"] + "\n"


__all__ = ["BriefingAgent", "to_markdown", "fallback_narrative", "wx"]
