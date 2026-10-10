import unittest

from ercot_feeds import (
    dedupe_key,
    fit_degree_day_model,
    parse_ercot_time,
    parse_fuel_mix,
    parse_prices,
    parse_supply_demand,
    quality_errors,
    weighted_temp_f,
)
from fixtures.ercot_samples import FUEL_MIX, PRICES, SUPPLY_DEMAND

NOW = "2026-10-09T05:30:00+00:00"


class ParseTests(unittest.TestCase):
    def test_time_offsets_are_honoured(self):
        cdt = parse_ercot_time("2026-10-08 16:05:00-0500")
        cst = parse_ercot_time("2026-12-08 16:05:00-0600")
        self.assertEqual(cdt.utcoffset().total_seconds(), -5 * 3600)
        self.assertEqual(cst.utcoffset().total_seconds(), -6 * 3600)
        self.assertIsNone(parse_ercot_time("yesterday"))
        self.assertIsNone(parse_ercot_time(None))

    def test_supply_demand_keeps_actuals_and_forecast_separately(self):
        recs, skipped = parse_supply_demand(SUPPLY_DEMAND, NOW)
        system = [r for r in recs if r["series"] == "system"]
        forecast = [r for r in recs if r["series"] == "forecast"]
        self.assertEqual(len(system), 2)
        self.assertEqual(len(forecast), 1)
        # null demand and bad timestamp; forecast-flagged rows are not failures
        self.assertEqual(skipped, 2)
        self.assertEqual(system[0]["observed_at"], "2026-10-08T05:00:00+00:00")
        self.assertEqual(system[0]["capacity_mw"], 73115.0)
        self.assertEqual(system[1]["available_mw"], 97637.0)
        self.assertEqual(system[0]["source_updated_at"], "2026-10-08T21:05:00+00:00")

    def test_fuel_mix_skips_storage_breakdown_and_bad_cells(self):
        recs, skipped = parse_fuel_mix(FUEL_MIX, NOW)
        series = {r["series"] for r in recs}
        self.assertNotIn("Power Storage Charging", series)
        self.assertNotIn("Power Storage Discharging", series)
        self.assertIn("Power Storage", series)
        self.assertEqual(len(recs), 9)  # 8 fuels + Wind at 00:05
        self.assertEqual(skipped, 1)    # "n/a" gas value

    def test_prices_emit_one_record_per_point_and_market(self):
        recs, skipped = parse_prices(PRICES, NOW)
        rt = [r for r in recs if r["market"] == "RT"]
        dam = [r for r in recs if r["market"] == "DAM"]
        self.assertEqual(len(rt), 15)
        self.assertEqual(len(dam), 3)  # only the points present in the row
        self.assertEqual(skipped, 0)
        houston = next(r for r in rt if r["series"] == "HB_HOUSTON")
        self.assertEqual(houston["value"], 38.54)
        self.assertEqual(houston["unit"], "USD/MWh")

    def test_wrong_shapes_do_not_raise(self):
        self.assertEqual(parse_fuel_mix({"data": []}, NOW), ([], 1))
        self.assertEqual(parse_supply_demand({"data": ["x"]}, NOW), ([], 1))
        self.assertEqual(parse_prices({}, NOW), ([], 0))

    def test_dedupe_key_distinguishes_markets(self):
        recs, _ = parse_prices(PRICES, NOW)
        keys = {dedupe_key(r) for r in recs}
        self.assertEqual(len(keys), len(recs))


class QualityTests(unittest.TestCase):
    def base(self, **kw):
        rec = {"feed": "price", "series": "HB_HOUSTON", "observed_at": NOW, "value": 40.0}
        rec.update(kw)
        return rec

    def test_contract(self):
        self.assertEqual(quality_errors(self.base()), [])
        self.assertEqual(quality_errors(self.base(value=-50.0)), [])  # negative prices happen
        self.assertEqual(quality_errors(self.base(value=99999.0)), ["price_out_of_range"])
        self.assertEqual(quality_errors(self.base(feed="demand", value=0.0)), ["demand_out_of_range"])
        self.assertEqual(quality_errors(self.base(feed="fuel", value=-800.0)), [])  # storage charging
        self.assertEqual(quality_errors(self.base(feed="weather")), ["unknown_feed"])
        self.assertEqual(quality_errors(self.base(value=None)), ["missing_value"])


class WeatherModelTests(unittest.TestCase):
    def test_weighted_temperature_renormalises_over_reporting_stations(self):
        self.assertEqual(weighted_temp_f({"KIAH": 30.0}), 86.0)
        both = weighted_temp_f({"KIAH": 30.0, "KDFW": 20.0})
        self.assertTrue(68.0 < both < 86.0)
        self.assertIsNone(weighted_temp_f({"KJFK": 20.0}))

    def test_recovers_known_coefficients(self):
        # demand = 40,000 + 900*CDD + 400*HDD, exactly
        pts = []
        for t in range(30, 105):
            cdd, hdd = max(t - 65, 0), max(65 - t, 0)
            pts.append((float(t), 40000 + 900 * cdd + 400 * hdd))
        m = fit_degree_day_model(pts)
        self.assertAlmostEqual(m["intercept_mw"], 40000, delta=1)
        self.assertAlmostEqual(m["mw_per_cooling_degree"], 900, delta=1)
        self.assertAlmostEqual(m["mw_per_heating_degree"], 400, delta=1)
        self.assertEqual(m["r2"], 1.0)

    def test_summer_only_data_drops_the_heating_term(self):
        pts = [(float(t), 40000 + 900 * (t - 65)) for t in range(70, 100)] * 2
        m = fit_degree_day_model(pts)
        self.assertIsNone(m["mw_per_heating_degree"])
        self.assertAlmostEqual(m["mw_per_cooling_degree"], 900, delta=1)

    def test_sparse_heating_hours_are_not_fitted(self):
        warm = [(float(t), 40000 + 900 * (t - 65)) for t in range(66, 100)] * 2
        cool = [(60.0, 39000.0)] * 5  # a few cool nights
        m = fit_degree_day_model(warm + cool)
        self.assertIsNone(m["mw_per_heating_degree"])

    def test_too_few_points(self):
        self.assertIsNone(fit_degree_day_model([(80.0, 50000.0)] * 10))


if __name__ == "__main__":
    unittest.main()
