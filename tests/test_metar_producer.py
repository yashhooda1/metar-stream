import unittest
from unittest.mock import Mock, patch

import requests

import metar_producer


class NormalizeTests(unittest.TestCase):
    def test_normalizes_station_and_timestamp(self):
        record = metar_producer.normalize(
            {"icaoId": " ksgr ", "obsTime": "1724198400", "visib": "10+"}
        )

        self.assertEqual(record["station_id"], "KSGR")
        self.assertEqual(record["observed_at"], "2024-08-21T00:00:00+00:00")
        self.assertEqual(record["visibility_sm"], "10+")

    def test_rejects_missing_or_invalid_identifiers(self):
        invalid = [
            None,
            {},
            {"icaoId": "", "obsTime": 1724198400},
            {"icaoId": 1234, "obsTime": 1724198400},
            {"icaoId": "KSGR"},
        ]

        for observation in invalid:
            with self.subTest(observation=observation):
                self.assertIsNone(metar_producer.normalize(observation))

    def test_rejects_invalid_timestamp_without_crashing_cycle(self):
        self.assertIsNone(
            metar_producer.normalize({"icaoId": "KSGR", "obsTime": "not-a-time"})
        )


class FetchObservationsTests(unittest.TestCase):
    @patch("metar_producer.time.sleep")
    @patch("metar_producer.requests.get")
    def test_partial_tile_failure_preserves_healthy_results(self, get, _sleep):
        healthy = Mock()
        healthy.raise_for_status.return_value = None
        healthy.json.return_value = [{"icaoId": "KSGR", "obsTime": 1724198400}]
        get.side_effect = [requests.Timeout("upstream timeout")] + [healthy] * 5

        observations = metar_producer.fetch_observations()

        self.assertEqual(len(observations), 5)
        self.assertEqual(get.call_count, len(metar_producer.TILES))

    @patch("metar_producer.time.sleep")
    @patch("metar_producer.requests.get")
    def test_non_list_payload_is_skipped(self, get, _sleep):
        unexpected = Mock()
        unexpected.raise_for_status.return_value = None
        unexpected.json.return_value = {"error": "unexpected response"}
        get.return_value = unexpected

        self.assertEqual(metar_producer.fetch_observations(), [])
        self.assertEqual(get.call_count, len(metar_producer.TILES))


if __name__ == "__main__":
    unittest.main()
