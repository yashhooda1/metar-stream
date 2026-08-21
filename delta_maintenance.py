"""Safe, observable maintenance for the METAR Delta Lake.

The command is a dry run unless --execute is supplied. OPTIMIZE runs before
VACUUM, and retention below seven days requires an explicit unsafe override.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

MIN_VACUUM_RETENTION_HOURS = 168
DEFAULT_VACUUM_RETENTION_HOURS = 168

TABLE_PATHS = {
    "bronze": "bronze/metar_raw",
    "silver": "silver/metar_observations",
    "rejected": "quarantine/metar_rejected",
    "quality": "gold/metar_quality_15min",
    "windowed": "gold/metar_15min",
    "alerts": "gold/metar_alerts",
}


@dataclass(frozen=True)
class TableTarget:
    name: str
    path: Path


@dataclass(frozen=True)
class FileStats:
    parquet_files: int
    parquet_bytes: int


@dataclass(frozen=True)
class MaintenanceResult:
    table: str
    path: str
    status: str
    vacuum_enabled: bool
    retention_hours: int
    parquet_files_before: int
    parquet_files_after: int
    parquet_files_delta: int
    parquet_bytes_before: int
    parquet_bytes_after: int
    parquet_bytes_delta: int
    duration_seconds: float
    error: str | None = None


DeltaTableFactory = Callable[[Any, str], Any]


def validate_retention(
    retention_hours: int,
    allow_unsafe_vacuum: bool = False,
) -> None:
    """Reject invalid or unsafe VACUUM retention settings."""

    if retention_hours <= 0:
        raise ValueError("VACUUM retention must be greater than zero hours")

    if (
        retention_hours < MIN_VACUUM_RETENTION_HOURS
        and not allow_unsafe_vacuum
    ):
        raise ValueError(
            "VACUUM retention below 168 hours can delete files still needed by "
            "active readers. Pass --allow-unsafe-vacuum only after verifying "
            "that no stream, batch job, or time-travel query needs those files."
        )


def build_targets(lake_path: str | Path, table_names: Sequence[str]) -> list[TableTarget]:
    """Resolve allow-listed table names beneath one lake root."""

    root = Path(lake_path).expanduser().resolve(strict=False)
    unknown = sorted(set(table_names) - TABLE_PATHS.keys())
    if unknown:
        raise ValueError(f"unknown maintenance tables: {', '.join(unknown)}")

    targets: list[TableTarget] = []
    for name in table_names:
        path = (root / TABLE_PATHS[name]).resolve(strict=False)
        if path != root and root not in path.parents:
            raise ValueError(f"maintenance path escapes lake root: {path}")
        targets.append(TableTarget(name=name, path=path))
    return targets


def collect_file_stats(path: Path) -> FileStats:
    """Count Parquet data files and bytes without following unrelated files."""

    file_count = 0
    byte_count = 0

    if not path.exists():
        return FileStats(parquet_files=0, parquet_bytes=0)

    for parquet_file in path.rglob("*.parquet"):
        try:
            if parquet_file.is_file():
                file_count += 1
                byte_count += parquet_file.stat().st_size
        except FileNotFoundError:
            # A concurrent transaction may remove a file between discovery and stat.
            continue

    return FileStats(parquet_files=file_count, parquet_bytes=byte_count)


def build_spark() -> Any:
    """Create the Delta-enabled Spark session used only for executed plans."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("metar-delta-maintenance")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def delta_table_for_path(spark: Any, path: str) -> Any:
    """Load a DeltaTable lazily so dry runs need no Delta/JVM dependency."""

    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, path)


def _result(
    target: TableTarget,
    *,
    status: str,
    vacuum_enabled: bool,
    retention_hours: int,
    before: FileStats,
    after: FileStats,
    started: float,
    error: str | None = None,
) -> MaintenanceResult:
    return MaintenanceResult(
        table=target.name,
        path=str(target.path),
        status=status,
        vacuum_enabled=vacuum_enabled,
        retention_hours=retention_hours,
        parquet_files_before=before.parquet_files,
        parquet_files_after=after.parquet_files,
        parquet_files_delta=after.parquet_files - before.parquet_files,
        parquet_bytes_before=before.parquet_bytes,
        parquet_bytes_after=after.parquet_bytes,
        parquet_bytes_delta=after.parquet_bytes - before.parquet_bytes,
        duration_seconds=round(time.monotonic() - started, 3),
        error=error,
    )


def maintain_target(
    spark: Any,
    target: TableTarget,
    *,
    execute: bool,
    vacuum_enabled: bool,
    retention_hours: int,
    table_factory: DeltaTableFactory = delta_table_for_path,
) -> MaintenanceResult:
    """Compact and optionally vacuum one target, returning before/after metrics."""

    started = time.monotonic()
    before = collect_file_stats(target.path)

    if not target.path.exists():
        return _result(
            target,
            status="skipped_missing",
            vacuum_enabled=vacuum_enabled,
            retention_hours=retention_hours,
            before=before,
            after=before,
            started=started,
        )

    if not execute:
        return _result(
            target,
            status="dry_run",
            vacuum_enabled=vacuum_enabled,
            retention_hours=retention_hours,
            before=before,
            after=before,
            started=started,
        )

    try:
        delta_table = table_factory(spark, str(target.path))
        delta_table.optimize().executeCompaction()
        if vacuum_enabled:
            delta_table.vacuum(retention_hours)

        after = collect_file_stats(target.path)
        return _result(
            target,
            status="completed",
            vacuum_enabled=vacuum_enabled,
            retention_hours=retention_hours,
            before=before,
            after=after,
            started=started,
        )
    except Exception as exc:
        after = collect_file_stats(target.path)
        return _result(
            target,
            status="error",
            vacuum_enabled=vacuum_enabled,
            retention_hours=retention_hours,
            before=before,
            after=after,
            started=started,
            error=f"{type(exc).__name__}: {exc}",
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or execute compaction and retention-aware VACUUM for allow-listed "
            "METAR Delta tables. The default is a non-mutating dry run."
        )
    )
    parser.add_argument(
        "--lake-path",
        default=os.getenv("LAKE_PATH", "./lake"),
        help="Delta Lake root (default: LAKE_PATH or ./lake)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=sorted(TABLE_PATHS),
        default=list(TABLE_PATHS),
        help="logical tables to maintain (default: all)",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=DEFAULT_VACUUM_RETENTION_HOURS,
        help="VACUUM retention in hours (default: 168)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform OPTIMIZE and VACUUM; without this flag the command is a dry run",
    )
    parser.add_argument(
        "--skip-vacuum",
        action="store_true",
        help="run OPTIMIZE without deleting expired data files",
    )
    parser.add_argument(
        "--allow-unsafe-vacuum",
        action="store_true",
        help="allow VACUUM retention below 168 hours after external safety checks",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    vacuum_enabled = not args.skip_vacuum

    if vacuum_enabled:
        try:
            validate_retention(args.retention_hours, args.allow_unsafe_vacuum)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

    try:
        targets = build_targets(args.lake_path, args.tables)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    spark = None
    results: list[MaintenanceResult] = []
    started_at = datetime.now(timezone.utc).isoformat()

    try:
        if args.execute and any(target.path.exists() for target in targets):
            spark = build_spark()
            spark.sparkContext.setLogLevel("WARN")

        for target in targets:
            results.append(
                maintain_target(
                    spark,
                    target,
                    execute=args.execute,
                    vacuum_enabled=vacuum_enabled,
                    retention_hours=args.retention_hours,
                )
            )
    finally:
        if spark is not None:
            spark.stop()

    summary = {
        "started_at_utc": started_at,
        "mode": "execute" if args.execute else "dry_run",
        "vacuum_enabled": vacuum_enabled,
        "retention_hours": args.retention_hours,
        "results": [asdict(result) for result in results],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 1 if any(result.status == "error" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
