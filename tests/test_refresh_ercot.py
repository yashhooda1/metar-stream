import copy
import json
import os
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "scripts"))

import refresh_ercot as rr  # noqa: E402
from ercot_feeds import parse_fuel_mix, parse_prices, parse_supply_demand  # noqa: E402
from fixtures.ercot_samples import FUEL_MIX, PRICES, SUPPLY_DEMAND  # noqa: E402

NOW = datetime(2026, 10, 9, 5, 30, tzinfo=timezone.utc)
ING = NOW.isoformat()


def records(prices=PRICES, demand=SUPPLY_DEMAND):
    return (parse_supply_demand(demand, ING)[0] + parse_fuel_mix(FUEL_MIX, ING)[0]
            + parse_prices(prices, ING)[0])


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, routes):
        self.routes = routes

    def get(self, url, **kw):
        for frag, payload in self.routes.items():
            if frag in url:
                return payload if isinstance(payload, FakeResponse) else FakeResponse(payload)
        return FakeResponse({}, 404)


METAR_OBS = [
    {"icaoId": "KIAH", "temp": 30.0, "obsTime": int(datetime(2026, 10, 8, 4, 53, tzinfo=timezone.utc).timestamp())},
    {"icaoId": "KDFW", "temp": 20.0, "obsTime": int(datetime(2026, 10, 8, 4, 53, tzinfo=timezone.utc).timestamp())},
    {"icaoId": "KJFK", "temp": 5.0, "obsTime": int(datetime(2026, 10, 8, 4, 51, tzinfo=timezone.utc).timestamp())},
    {"icaoId": "KAUS", "temp": None, "obsTime": 0},
]


class RefreshTests(unittest.TestCase):
    def test_hour_bucketing(self):
        t = datetime(2026, 10, 9, 5, 15, tzinfo=timezone.utc)
        self.assertEqual(rr.hour_of(t), "2026-10-09T05:00:00+00:00")
        end = datetime(2026, 10, 9, 6, 0, tzinfo=timezone.utc)
        self.assertEqual(rr.hour_of(end, interval_end=True), "2026-10-09T05:00:00+00:00")
        metar = datetime(2026, 10, 9, 4, 53, tzinfo=timezone.utc)
        self.assertEqual(rr.hour_of(metar, nearest=True), "2026-10-09T05:00:00+00:00")

    def test_temps_only_ercot_stations(self):
        temps = rr.bucket_temps(METAR_OBS)
        self.assertEqual(list(temps), ["2026-10-08T05:00:00+00:00"])
        self.assertEqual(set(temps["2026-10-08T05:00:00+00:00"]), {"KIAH", "KDFW"})

    def test_merge_counts_revisions_and_joins_weather(self):
        rows = rr.hourly_rows(records())
        temps = rr.bucket_temps(METAR_OBS)
        history, revised = rr.merge_history({}, rows, temps, NOW)
        self.assertEqual(revised, 0)
        row = history["2026-10-08T05:00:00+00:00"]
        self.assertEqual(row["demand_mw"], round((53588 + 53210) / 2, 2))
        self.assertEqual(row["stations"], 2)
        self.assertTrue(68 < row["temp_f"] < 86)

        changed = copy.deepcopy(PRICES)
        changed["rtSppData"][0]["hbHouston"] = 99.0
        history2, revised2 = rr.merge_history(copy.deepcopy(history), rr.hourly_rows(records(prices=changed)), {}, NOW)
        self.assertEqual(revised2, 1)
        self.assertEqual(history2["2026-10-09T05:00:00+00:00"]["rt:HB_HOUSTON"], 99.0)
        # weather survives a run that fetched no temperatures
        self.assertEqual(history2["2026-10-08T05:00:00+00:00"]["stations"], 2)

    def test_retention_drops_old_hours(self):
        old = {"2026-01-01T00:00:00+00:00": {"demand_mw": 1.0}}
        history, _ = rr.merge_history(old, {}, {}, NOW)
        self.assertEqual(history, {})

    def test_dashboard_shape(self):
        recs = records()
        history, revised = rr.merge_history({}, rr.hourly_rows(recs), {}, NOW)
        d = rr.build_dashboard(recs, [], history, {"runs": 1, "revised": 0}, NOW, revised)
        self.assertEqual(d["latest"]["demand_mw"], 53210.0)
        self.assertIn("HB_HOUSTON", d["latest"]["rt_prices"])
        self.assertEqual(d["latest"]["fuel_mw"]["Wind"], 10700.0)
        self.assertIsNone(d["weather_load"]["model"])  # needs 48 hours
        self.assertEqual(d["stats"]["records_this_run"]["price"], 18)
        json.dumps(d)  # serialisable

    def test_future_forecast_hours_do_not_crowd_out_the_past(self):
        recs = records()
        history, revised = rr.merge_history({}, rr.hourly_rows(recs), {}, NOW)
        # A week of forecast-only hours ahead of now, as the real feed provides.
        for k in range(1, 170):
            history[(NOW.replace(minute=0) + rr.timedelta(hours=k)).isoformat()] = {"forecast_mw": 50000.0}
        d = rr.build_dashboard(recs, [], history, {"runs": 1, "revised": 0}, NOW, revised)
        self.assertTrue(d["fuel_hourly"])
        self.assertTrue(all(r["t"] <= NOW.isoformat() for r in d["fuel_hourly"]))
        self.assertLess(d["stats"]["history_hours"], 10)

    def test_main_end_to_end_with_partial_feed_failure(self):
        session = FakeSession({
            "supply-demand": SUPPLY_DEMAND,
            "fuel-mix": FakeResponse({}, 503),
            "systemWidePrices": PRICES,
            "aviationweather": METAR_OBS,
        })
        with tempfile.TemporaryDirectory() as tmp, mock.patch("requests.Session", return_value=session):
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                self.assertEqual(rr.main(), 0)
                self.assertEqual(rr.main(), 0)
                dash = json.loads(pathlib.Path("docs/ercot_dashboard.json").read_text())
                totals = json.loads(pathlib.Path("docs/ercot_totals.json").read_text())
            finally:
                os.chdir(cwd)
        self.assertEqual(totals["runs"], 2)
        self.assertTrue(any(w.startswith("fuel:") for w in dash["warnings"]))
        self.assertEqual(dash["latest"]["fuel_mw"], {})

    def test_main_keeps_previous_snapshot_when_everything_fails(self):
        session = FakeSession({})
        with tempfile.TemporaryDirectory() as tmp, mock.patch("requests.Session", return_value=session):
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                self.assertEqual(rr.main(), 1)
                self.assertFalse(pathlib.Path("docs/ercot_dashboard.json").exists())
            finally:
                os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
