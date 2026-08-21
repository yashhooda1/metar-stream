import json
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from metar_stream import build_quality_metrics, build_rejected, build_silver


RAW_SCHEMA = StructType([StructField("value", StringType(), False)])


def payload(**overrides):
    record = {
        "station_id": "KSGR",
        "observed_at": "2026-08-21T12:00:00Z",
        "ingested_at": "2026-08-21T12:01:00Z",
        "name": "Sugar Land Regional Airport",
        "lat": 29.62,
        "lon": -95.66,
        "temp_c": 34.0,
        "wind_speed_kt": 12,
        "wind_gust_kt": None,
        "visibility_sm": "10+",
        "flight_category": "VFR",
    }
    record.update(overrides)
    return (json.dumps(record),)


class SilverTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("metar-stream-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def transform(self, rows):
        raw = self.spark.createDataFrame(rows, RAW_SCHEMA)
        return build_silver(raw)

    def test_parses_visibility_applies_gust_fallback_and_lag(self):
        row = self.transform([payload()]).collect()[0]

        self.assertEqual(row.station_id, "KSGR")
        self.assertEqual(row.visibility_mi, 10.0)
        self.assertEqual(row.wind_gust_kt, 12)
        self.assertEqual(row.lag_seconds, 60)

    def test_deduplicates_station_and_observation_time(self):
        silver = self.transform([payload(), payload()])

        self.assertEqual(silver.count(), 1)

    def test_rejects_records_outside_the_silver_contract(self):
        rows = [
            payload(station_id="BAD"),
            payload(station_id="ksgr"),
            payload(observed_at=None),
            payload(lat=91.0),
            payload(lon=-181.0),
            payload(temp_c=-101.0),
            payload(wind_speed_kt=-1),
            payload(wind_gust_kt=251),
        ]

        self.assertEqual(self.transform(rows).count(), 0)

    def test_accepts_nullable_optional_measurements(self):
        row = payload(
            lat=None,
            lon=None,
            temp_c=None,
            wind_speed_kt=None,
            wind_gust_kt=None,
            visibility_sm="unknown",
        )
        result = self.transform([row]).collect()[0]

        self.assertIsNone(result.visibility_mi)
        self.assertIsNone(result.wind_gust_kt)

    def test_rejected_records_include_payload_and_reason_codes(self):
        rows = [
            payload(station_id="BAD"),
            payload(lat=91.0),
            payload(temp_c=-101.0, wind_speed_kt=-1),
            ("{not-json}",),
        ]
        raw = self.spark.createDataFrame(rows, RAW_SCHEMA)
        rejected = build_rejected(raw)
        results = rejected.collect()
        by_payload = {row.payload: row for row in results}

        self.assertEqual(len(results), 4)
        self.assertEqual(
            by_payload[rows[0][0]].quality_errors,
            ["invalid_station_id"],
        )
        self.assertEqual(
            by_payload[rows[1][0]].quality_errors,
            ["latitude_out_of_range"],
        )
        self.assertEqual(
            by_payload[rows[2][0]].quality_errors,
            ["temperature_out_of_range", "wind_speed_out_of_range"],
        )
        self.assertEqual(
            by_payload[rows[3][0]].quality_errors,
            ["malformed_payload"],
        )
        self.assertIsNotNone(by_payload[rows[0][0]].rejected_at)

    def test_quality_metrics_count_each_rejection_reason(self):
        rows = [
            payload(lat=91.0),
            payload(lat=-91.0),
            payload(wind_gust_kt=251),
        ]
        raw = self.spark.createDataFrame(rows, RAW_SCHEMA)
        metrics = {
            row.reason: row.rejected_count
            for row in build_quality_metrics(build_rejected(raw)).collect()
        }

        self.assertEqual(metrics["latitude_out_of_range"], 2)
        self.assertEqual(metrics["wind_gust_out_of_range"], 1)


if __name__ == "__main__":
    unittest.main()
