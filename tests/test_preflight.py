import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from preflight import rules, sources, wx
from preflight.agent import BriefingAgent, fallback_narrative

NOW = datetime(2026, 11, 1, 15, 0, tzinfo=timezone.utc)
EPOCH = int(NOW.timestamp())


def metar(icao, *, wspd=8, wgst=None, visib="10+", clouds=None, wx_string=None, age_min=10,
          lat=30.19, lon=-97.67, raw=None):
    return sources.silver_shape({
        "icaoId": icao, "obsTime": EPOCH - age_min * 60, "wspd": wspd, "wgst": wgst,
        "visib": visib, "clouds": clouds or [{"cover": "FEW", "base": 5000}],
        "wxString": wx_string, "rawOb": raw or f"{icao} 011450Z 18008KT 10SM FEW050 22/14 A3002",
        "lat": lat, "lon": lon, "name": icao,
    })


def taf(icao, groups, lat=30.19, lon=-97.67):
    return {"icaoId": icao, "rawTAF": f"TAF {icao} ...", "lat": lat, "lon": lon, "fcsts": groups}


def group(start_h, end_h, *, change=None, visib="6+", clouds=None, wspd=10, wgst=None, wx_string=None, prob=None):
    return {"timeFrom": EPOCH + start_h * 3600, "timeTo": EPOCH + end_h * 3600, "fcstChange": change,
            "probability": prob, "visib": visib, "clouds": clouds or [{"cover": "SCT", "base": 6000}],
            "wspd": wspd, "wgst": wgst, "wxString": wx_string}


def station(icao, m=None, t=None, notams=(), lat=30.19, lon=-97.67):
    return {"metar": m, "taf": t, "notams": list(notams) if notams is not None else None, "lat": lat, "lon": lon}


class WxParsingTests(unittest.TestCase):
    def test_visibility_formats(self):
        cases = {"10+": 10, 6: 6, "6+": 10, "1 1/2": 1.5, "1/4": 0.25, "P6SM": 10, None: None, "bad": None}
        for raw, want in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(wx.parse_visibility(raw), want)

    def test_ceiling_matches_dbt_parser_rules(self):
        self.assertEqual(wx.ceiling_from_raw("KAUS 011450Z 10SM FEW008 SCT012 BKN025 OVC040"), 2500)
        self.assertIsNone(wx.ceiling_from_raw("KAUS 011450Z 10SM FEW008 SCT012"))
        self.assertIsNone(wx.ceiling_from_raw("KAUS 011450Z 10SM SCT050 RMK BKN010"))
        self.assertIsNone(wx.ceiling_from_raw("KAUS 011450Z 1/4SM VV///"))
        self.assertEqual(wx.ceiling_from_raw("KAUS 011450Z 1/4SM VV002"), 200)

    def test_flight_category_boundaries_match_macro(self):
        self.assertEqual(wx.flight_category(3000, 10), "MVFR")
        self.assertEqual(wx.flight_category(3100, 10), "VFR")
        self.assertEqual(wx.flight_category(None, 2.5), "IFR")
        self.assertEqual(wx.flight_category(400, 10), "LIFR")
        self.assertEqual(wx.flight_category(None, None), "UNKNOWN")

    def test_hazards_ignore_station_ids_and_remarks(self):
        no_go, caution = wx.hazards("KDSM 011450Z 10SM CLR RMK TSB12")
        self.assertEqual((no_go, caution), ([], []))
        no_go, caution = wx.hazards("-TSRA BR")
        self.assertTrue(any("thunderstorm" in h for h in no_go))
        self.assertTrue(any("mist" in h for h in caution))
        self.assertEqual(wx.hazards("VCTS")[0], [])
        self.assertTrue(wx.hazards("VCTS")[1])


class RulesTests(unittest.TestCase):
    def brief(self, data, route=("KAUS", "KHOU"), **kw):
        kw.setdefault("now", NOW)
        return rules.brief_route(list(route), data, **kw)

    def test_clear_day_is_go(self):
        good_taf = lambda i: taf(i, [group(-2, 6)])
        out = self.brief({"KAUS": station("KAUS", metar("KAUS"), good_taf("KAUS")),
                          "KHOU": station("KHOU", metar("KHOU"), good_taf("KHOU"))})
        self.assertEqual(out["verdict"], rules.GO, out)

    def test_low_ceiling_at_destination_is_no_go(self):
        low = [{"cover": "OVC", "base": 800}]
        out = self.brief({"KAUS": station("KAUS", metar("KAUS"), taf("KAUS", [group(-2, 6)])),
                          "KHOU": station("KHOU", metar("KHOU", clouds=low), taf("KHOU", [group(-2, 6, clouds=low)]))})
        self.assertEqual(out["verdict"], rules.NO_GO)
        dest = out["stations"][1]
        self.assertIn("ceiling_below_min", [f["code"] for f in dest["flags"]])
        self.assertEqual(dest["category_now"], "IFR")

    def test_tempo_thunderstorm_is_caution_not_no_go(self):
        groups = [group(-2, 6), group(0, 3, change="TEMPO", wx_string="TSRA", visib=3)]
        out = self.brief({"KAUS": station("KAUS", metar("KAUS"), taf("KAUS", groups)),
                          "KHOU": station("KHOU", metar("KHOU"), taf("KHOU", [group(-2, 6)]))})
        self.assertEqual(out["verdict"], rules.CAUTION)
        self.assertTrue(any(f["code"] == "hazardous_weather" and f["severity"] == "CAUTION"
                            for f in out["stations"][0]["flags"]))

    def test_current_metar_is_trend_only_when_flight_is_far_out(self):
        gusty = metar("KAUS", wspd=22, wgst=35)
        data = {"KAUS": station("KAUS", gusty, taf("KAUS", [group(-2, 12)])),
                "KHOU": station("KHOU", metar("KHOU"), taf("KHOU", [group(-2, 12)]))}
        now_brief = self.brief(data)
        later = self.brief(data, etd=NOW + timedelta(hours=5))
        self.assertEqual(now_brief["verdict"], rules.NO_GO)
        self.assertEqual(later["verdict"], rules.CAUTION)

    def test_stale_metar_and_missing_notams_are_cautions(self):
        out = self.brief({"KAUS": station("KAUS", metar("KAUS", age_min=180), taf("KAUS", [group(-2, 6)]), notams=None)},
                         route=("KAUS",))
        codes = {f["code"] for f in out["stations"][0]["flags"]}
        self.assertIn("stale_metar", codes)
        self.assertIn("notams_not_checked", codes)

    def test_airport_closed_notam_is_no_go(self):
        out = self.brief({"KAUS": station("KAUS", metar("KAUS"), taf("KAUS", [group(-2, 6)]),
                                          notams=["!AUS 11/001 AUS AD AP CLSD 1100-1800"])}, route=("KAUS",))
        self.assertEqual(out["verdict"], rules.NO_GO)

    def test_ifr_profile_applies_1_2_3_alternate_rule(self):
        marginal = [{"cover": "BKN", "base": 1500}]
        out = self.brief({"KAUS": station("KAUS", metar("KAUS"), taf("KAUS", [group(-2, 6)])),
                          "KHOU": station("KHOU", metar("KHOU"), taf("KHOU", [group(-2, 6, clouds=marginal)]))},
                         profile="ifr")
        self.assertIn("alternate_required", [f["code"] for f in out["stations"][1]["flags"]])

    def test_taf_substitute_must_be_close(self):
        data = {"KAUS": station("KAUS", metar("KAUS"), taf("KAUS", [group(-2, 6)])),
                "KEDC": station("KEDC", metar("KEDC", lat=30.40, lon=-97.57), None, lat=30.40, lon=-97.57),
                "KGTU": station("KGTU", None, taf("KGTU", [group(-2, 6)], lat=30.68, lon=-97.68), lat=30.68, lon=-97.68),
                "KDFW": station("KDFW", None, taf("KDFW", [group(-2, 6)], lat=32.90, lon=-97.04), lat=32.90, lon=-97.04)}
        ok = self.brief(data, route=("KAUS", "KEDC"), taf_substitutes={"KEDC": "KGTU"})
        far = self.brief(data, route=("KAUS", "KEDC"), taf_substitutes={"KEDC": "KDFW"})
        self.assertIn("nearby_taf", [f["code"] for f in ok["stations"][1]["flags"]])
        self.assertEqual(ok["stations"][1]["taf_source"], "KGTU")
        self.assertIn("substitute_rejected", [f["code"] for f in far["stations"][1]["flags"]])


class AgentLoopTests(unittest.TestCase):
    def data(self):
        return {"KAUS": station("KAUS", metar("KAUS"), taf("KAUS", [group(-2, 6)])),
                "KEDC": station("KEDC", metar("KEDC", lat=30.40, lon=-97.57), None, lat=30.40, lon=-97.57)}

    def test_agent_fills_taf_gap_then_submits(self):
        script = iter([
            {"content": [{"type": "thinking", "thinking": "...", "signature": "x"},
                         {"type": "tool_use", "id": "t1", "name": "find_nearby_taf", "input": {"station": "KEDC"}}]},
            {"content": [{"type": "tool_use", "id": "t2", "name": "use_nearby_taf",
                          "input": {"station": "KEDC", "substitute": "KAUS"}}]},
            {"content": [{"type": "tool_use", "id": "t3", "name": "submit_brief",
                          "input": {"narrative": "Departure KAUS VFR. Bottom line: GO with caution."}}]},
        ])
        sent = []

        def fake_model(payload):
            sent.append(payload)
            return next(script)

        nearby = lambda lat, lon, r: [{"station": "KAUS", "distance_nm": 13.9, "name": "Austin",
                                       "taf": self.data()["KAUS"]["taf"]}]
        agent = BriefingAgent(["KAUS", "KEDC"], data=self.data(), now=NOW, call_model=fake_model, fetch_nearby=nearby)
        out = agent.run()

        self.assertEqual([t["tool"] for t in out["trace"]], ["find_nearby_taf", "use_nearby_taf", "submit_brief"])
        self.assertEqual(out["stations"][1]["taf_source"], "KAUS")
        self.assertIn("Bottom line", out["narrative"])
        self.assertNotIn("temperature", sent[0])  # Claude 5 models reject sampling params
        # thinking block is echoed back verbatim on the next turn
        self.assertEqual(sent[1]["messages"][1]["content"][0]["type"], "thinking")

    def test_model_cannot_change_verdict(self):
        data = self.data()
        data["KAUS"]["metar"] = metar("KAUS", clouds=[{"cover": "OVC", "base": 300}], visib="1/2")
        liar = lambda payload: {"content": [{"type": "tool_use", "id": "t", "name": "submit_brief",
                                             "input": {"narrative": "Looks great, GO!"}}]}
        out = BriefingAgent(["KAUS", "KEDC"], data=data, now=NOW, call_model=liar).run()
        self.assertEqual(out["verdict"], rules.NO_GO)

    def test_model_failure_falls_back_to_rules_brief(self):
        def boom(payload):
            raise RuntimeError("529 overloaded")
        out = BriefingAgent(["KAUS", "KEDC"], data=self.data(), now=NOW, call_model=boom).run()
        self.assertIsNone(out["model"])
        self.assertIn("Verdict:", out["narrative"])

    @patch.dict(os.environ, {}, clear=True)
    def test_no_api_key_means_rules_only(self):
        out = BriefingAgent(["KAUS", "KEDC"], data=self.data(), now=NOW).run()
        self.assertEqual(out["narrative"], fallback_narrative({k: v for k, v in out.items()
                                                                if k not in ("narrative", "model", "trace")}))


class SourcesTests(unittest.TestCase):
    @patch("preflight.sources.requests.get")
    def test_fetch_metars_keeps_latest_per_station(self, get):
        get.return_value.status_code = 200
        get.return_value.content = b"[]"
        get.return_value.json.return_value = [
            {"icaoId": "KAUS", "obsTime": EPOCH - 3600, "rawOb": "old"},
            {"icaoId": "KAUS", "obsTime": EPOCH, "rawOb": "new", "clouds": [{"cover": "BKN", "base": 2500}]},
        ]
        out = sources.fetch_metars(["KAUS"])
        self.assertEqual(out["KAUS"]["raw_text"], "new")
        self.assertEqual(out["KAUS"]["ceiling_ft"], 2500)

    @patch.dict(os.environ, {}, clear=True)
    def test_notams_without_credentials_are_unchecked_not_empty(self):
        self.assertIsNone(sources.fetch_notams("KAUS"))


if __name__ == "__main__":
    unittest.main()
