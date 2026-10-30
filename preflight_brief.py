"""Preflight briefing agent CLI.

    python preflight_brief.py KAUS KHOU
    python preflight_brief.py KAUS KCLL KHOU --etd 2026-11-01T15:00Z --ete 1.2 --profile vfr_standard
    python preflight_brief.py KAUS KDAL --profile ifr --json > brief.json
    python preflight_brief.py KAUS KHOU --lake-path ./lake      # METARs from Silver
    python preflight_brief.py KAUS KHOU --no-llm                # rules only, no API key needed

Env: ANTHROPIC_API_KEY (narrative), PREFLIGHT_MODEL, FAA_CLIENT_ID / FAA_CLIENT_SECRET (NOTAMs).
Exit code: 0 GO, 1 CAUTION, 2 NO-GO — so it can gate a script.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys

from preflight import rules, wx
from preflight.agent import BriefingAgent, to_markdown

ICAO = re.compile(r"^[A-Z0-9]{4}$")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Pilot-style preflight weather brief with go/no-go flags.")
    p.add_argument("route", nargs="+", help="ICAO ids: departure [enroute ...] destination")
    p.add_argument("--etd", help="ISO time, e.g. 2026-11-01T15:00Z (default: now)")
    p.add_argument("--ete", type=float, default=1.0, help="estimated time enroute, hours")
    p.add_argument("--profile", default="vfr_conservative", choices=sorted(rules.PROFILES))
    p.add_argument("--lake-path", help="read METARs from the Silver Delta table instead of the API")
    p.add_argument("--no-llm", action="store_true", help="skip the model; deterministic brief only")
    p.add_argument("--json", action="store_true", help="print the full JSON result")
    args = p.parse_args(argv)

    route = [s.strip().upper() for s in args.route]
    bad = [s for s in route if not ICAO.match(s)]
    if bad:
        p.error(f"not ICAO ids: {', '.join(bad)}")
    etd = wx.to_utc(args.etd) if args.etd else None
    if args.etd and etd is None:
        p.error(f"could not parse --etd {args.etd!r}")
    if not 0 < args.ete <= 12:
        p.error("--ete must be between 0 and 12 hours")

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    agent = BriefingAgent(route, profile=args.profile, etd=etd, ete_hours=args.ete, lake_path=args.lake_path)
    brief = agent.run(use_llm=not args.no_llm)

    print(json.dumps(brief, indent=2, default=str) if args.json else to_markdown(brief))
    return {"GO": 0, "CAUTION": 1, "NO_GO": 2}[brief["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
