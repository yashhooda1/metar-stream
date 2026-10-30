"""Preflight briefing agent built on top of METAR Stream.

Layers, in the order a brief is produced:

    sources.py  fetch METAR / TAF / NOTAM for each route station
                (NOAA AWC API, the FAA NOTAM API, or the Silver Delta table)
    wx.py       pure parsing: visibility, ceiling, flight category, TAF periods
    rules.py    deterministic go / caution / no-go flags against pilot minimums
    agent.py    an LLM tool loop that fills data gaps (e.g. no TAF at the
                destination) and writes the narrative brief

The verdict always comes from rules.py. The model can gather more data and
explain the flags, but it cannot change them.
"""
