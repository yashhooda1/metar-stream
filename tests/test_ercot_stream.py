import json
import os
import time
import unittest
from datetime import datetime

# PySpark converts timestamps to and from naive Python datetimes using the
# process's local zone. Pin it, so the expected values below mean UTC on a
# laptop in Houston as well as on a CI runner.
os.environ["TZ"] = "UTC"
time.tzset()

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from ercot_feeds import parse_fuel_mix, parse_prices, parse_supply_demand, quality_errors
from ercot_gold import build_alerts, build_grid_15min, build_prices_hourly, build_weather_load
from ercot_stream import build_rejected, build_valid, evaluate_quality, latest_per_key
from fixtures.ercot_samples import FUEL_MIX, PRICES, SUPPLY_DEMAND

RAW_SCHEMA = StructType([StructField("value", StringType(), False)])
NOW = "2026-10-09T05:30:00+00:00"


def rec(**kw):
    base = {
        "feed": "price", "series": "HB_HOUSTON", "market": "RT",
        "observed_at": "2026-10-09T05:15:00+00:00", "value": 40.0, "unit": "USD/MWh",
        "capacity_mw": None, "available_mw": None,
        "source_updated_at": "2026-10-09T05:20:00+00:00", "ingested_at": NOW,
    }
    base.update(kw)
    return base


class ErcotSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("ercot-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw(self, records):
        rows = [(r if isinstance(r, str) else json.dumps(r),) for r in records]
        return self.spark.createDataFrame(rows, RAW_SCHEMA)

    def silver(self, records):
        return build_valid(evaluate_quality(self.raw(records)))

    def test_spark_contract_matches_python_contract(self):
        cases = [
            rec(), rec(value=-50.0), rec(value=99999.0), rec(feed="demand", value=0.0),
            rec(feed="fuel", value=-800.0), rec(feed="weather"), rec(value=None), rec(series=""),
        ]
        rows = evaluate_quality(self.raw(cases)).select("quality_errors").collect()
        for case, row in zip(cases, rows):
            self.assertEqual(sorted(row.quality_errors), sorted(quality_errors(case)), case)

    def test_malformed_payload_is_quarantined_with_reason(self):
        rejected = build_rejected(evaluate_quality(self.raw(["{not json", rec(value=None)]))).collect()
        reasons = sorted(r.quality_errors[0] for r in rejected)
        self.assertEqual(reasons, ["malformed_payload", "missing_value"])
        self.assertEqual(rejected[0].payload is not None, True)

    def test_latest_revision_wins(self):
        records = [
            rec(value=40.0, source_updated_at="2026-10-09T05:20:00+00:00"),
            rec(value=44.0, source_updated_at="2026-10-09T05:25:00+00:00"),
            rec(value=41.0, source_updated_at="2026-10-09T05:22:00+00:00"),
            # same instant, other market: a different key, must survive
            rec(market="DAM", value=50.0),
        ]
        out = {(r.market_key, r.value) for r in latest_per_key(self.silver(records)).collect()}
        self.assertEqual(out, {("RT", 44.0), ("DAM", 50.0)})

    def test_null_market_still_forms_one_key(self):
        records = [rec(feed="fuel", series="Wind", market=None, value=v,
                       source_updated_at=f"2026-10-09T05:2{i}:00+00:00") for i, v in enumerate([1.0, 2.0])]
        out = latest_per_key(self.silver(records)).collect()
        self.assertEqual([r.value for r in out], [2.0])

    def test_gold_grid_and_prices_from_parsed_documents(self):
        records = (
            parse_supply_demand(SUPPLY_DEMAND, NOW)[0]
            + parse_fuel_mix(FUEL_MIX, NOW)[0]
            + parse_prices(PRICES, NOW)[0]
        )
        silver = self.silver(records)
        grid = {r.window_start: r for r in build_grid_15min(silver).collect()}

        demand_row = grid[datetime(2026, 10, 8, 5, 0)]
        self.assertAlmostEqual(demand_row.demand_mw, (53588 + 53210) / 2)
        self.assertAlmostEqual(demand_row.margin_mw, (73115 + 73001) / 2 - (53588 + 53210) / 2)

        fuel_row = grid[datetime(2026, 10, 7, 5, 0)]
        self.assertAlmostEqual(fuel_row.wind_mw, (10676.66 + 10700.0) / 2)
        self.assertGreater(fuel_row.total_generation_mw, 50000)
        self.assertTrue(0 < fuel_row.wind_solar_share_pct < 30)

        hourly = {(r.settlement_point, r.hour_start): r for r in build_prices_hourly(silver).collect()}
        # RT 00:15 and DA hour-ending 01:00 (CDT) both belong to the 05:00 UTC hour
        h = hourly[("HB_HOUSTON", datetime(2026, 10, 9, 5, 0))]
        self.assertEqual(h.rt_usd_mwh, 38.54)
        self.assertEqual(h.da_usd_mwh, 47.0)
        self.assertEqual(h.rt_minus_da, -8.46)

    def test_alerts(self):
        silver = self.silver([
            rec(value=1200.0),
            rec(series="HB_WEST", value=30.0),
            rec(feed="demand", series="system", market=None, value=70000.0, capacity_mw=72000.0,
                unit="MW", observed_at="2026-10-09T22:00:00+00:00"),
        ])
        alerts = build_alerts(silver, build_grid_15min(silver)).collect()
        kinds = sorted(a.kind for a in alerts)
        self.assertEqual(kinds, ["price_spike", "thin_margin"])

    def test_weather_load_join_weights_and_rounds_to_nearest_hour(self):
        silver = self.silver([
            rec(feed="demand", series="system", market=None, value=60000.0, unit="MW",
                observed_at="2026-10-09T15:00:00+00:00"),
            rec(feed="demand", series="system", market=None, value=62000.0, unit="MW",
                observed_at="2026-10-09T15:30:00+00:00"),
        ])
        metar = self.spark.createDataFrame(
            [("KIAH", datetime(2026, 10, 9, 14, 53), 30.0),
             ("KDFW", datetime(2026, 10, 9, 14, 53), 20.0),
             ("KJFK", datetime(2026, 10, 9, 14, 51), -5.0)],
            "station_id string, observed_at timestamp, temp_c double",
        )
        out = build_weather_load(silver, metar).collect()
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertEqual(row.hour_start, datetime(2026, 10, 9, 15, 0))
        self.assertEqual(row.stations_reporting, 2)
        self.assertEqual(row.demand_mw, 61000.0)
        expected = (7.1 * 86.0 + 7.6 * 68.0) / (7.1 + 7.6)
        self.assertAlmostEqual(row.temp_f, round(expected, 2), places=2)


if __name__ == "__main__":
    unittest.main()
