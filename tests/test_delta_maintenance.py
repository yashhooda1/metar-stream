import tempfile
import unittest
from pathlib import Path
from unittest import mock

import delta_maintenance as maintenance


class FakeOptimizer:
    def __init__(self, calls):
        self.calls = calls

    def executeCompaction(self):
        self.calls.append(("optimize",))


class FakeDeltaTable:
    def __init__(self, calls):
        self.calls = calls

    def optimize(self):
        return FakeOptimizer(self.calls)

    def vacuum(self, retention_hours):
        self.calls.append(("vacuum", retention_hours))


class DeltaMaintenanceTests(unittest.TestCase):
    def test_retention_rejects_nonpositive_values(self):
        for retention in (0, -1):
            with self.subTest(retention=retention):
                with self.assertRaisesRegex(ValueError, "greater than zero"):
                    maintenance.validate_retention(retention)

    def test_retention_rejects_less_than_seven_days_by_default(self):
        with self.assertRaisesRegex(ValueError, "below 168 hours"):
            maintenance.validate_retention(167)

    def test_explicit_override_allows_short_retention(self):
        maintenance.validate_retention(24, allow_unsafe_vacuum=True)

    def test_build_targets_uses_allow_list_beneath_lake_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            targets = maintenance.build_targets(tmp, ["silver", "alerts"])

            self.assertEqual(["silver", "alerts"], [target.name for target in targets])
            self.assertEqual(
                Path(tmp).resolve() / "silver/metar_observations",
                targets[0].path,
            )

    def test_build_targets_rejects_unknown_table(self):
        with self.assertRaisesRegex(ValueError, "unknown maintenance tables"):
            maintenance.build_targets("/tmp/lake", ["silver", "../outside"])

    def test_file_stats_counts_only_parquet_data_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "part").mkdir()
            (root / "one.parquet").write_bytes(b"123")
            (root / "part/two.parquet").write_bytes(b"12345")
            (root / "_delta_log").mkdir()
            (root / "_delta_log/000.json").write_text("{}")

            stats = maintenance.collect_file_stats(root)

            self.assertEqual(2, stats.parquet_files)
            self.assertEqual(8, stats.parquet_bytes)

    def test_dry_run_never_loads_delta_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "silver"
            path.mkdir()
            target = maintenance.TableTarget("silver", path)
            factory = mock.Mock(side_effect=AssertionError("must not be called"))

            result = maintenance.maintain_target(
                None,
                target,
                execute=False,
                vacuum_enabled=True,
                retention_hours=168,
                table_factory=factory,
            )

            self.assertEqual("dry_run", result.status)
            factory.assert_not_called()

    def test_missing_table_is_skipped_without_starting_delta(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = maintenance.TableTarget("alerts", Path(tmp) / "missing")
            factory = mock.Mock(side_effect=AssertionError("must not be called"))

            result = maintenance.maintain_target(
                None,
                target,
                execute=True,
                vacuum_enabled=True,
                retention_hours=168,
                table_factory=factory,
            )

            self.assertEqual("skipped_missing", result.status)
            factory.assert_not_called()

    def test_execute_compacts_then_vacuums(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "silver"
            path.mkdir()
            (path / "part.parquet").write_bytes(b"data")
            calls = []

            result = maintenance.maintain_target(
                object(),
                maintenance.TableTarget("silver", path),
                execute=True,
                vacuum_enabled=True,
                retention_hours=336,
                table_factory=lambda spark, table_path: FakeDeltaTable(calls),
            )

            self.assertEqual("completed", result.status)
            self.assertEqual([("optimize",), ("vacuum", 336)], calls)
            self.assertEqual(1, result.parquet_files_before)
            self.assertEqual(1, result.parquet_files_after)

    def test_skip_vacuum_only_compacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bronze"
            path.mkdir()
            calls = []

            result = maintenance.maintain_target(
                object(),
                maintenance.TableTarget("bronze", path),
                execute=True,
                vacuum_enabled=False,
                retention_hours=1,
                table_factory=lambda spark, table_path: FakeDeltaTable(calls),
            )

            self.assertEqual("completed", result.status)
            self.assertEqual([("optimize",)], calls)

    def test_table_failure_is_reported_without_raising(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "quality"
            path.mkdir()

            result = maintenance.maintain_target(
                object(),
                maintenance.TableTarget("quality", path),
                execute=True,
                vacuum_enabled=True,
                retention_hours=168,
                table_factory=mock.Mock(side_effect=RuntimeError("broken log")),
            )

            self.assertEqual("error", result.status)
            self.assertEqual("RuntimeError: broken log", result.error)


if __name__ == "__main__":
    unittest.main()
