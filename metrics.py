"""
Compute the benchmark numbers for the README from the Delta tables.

Run this after the pipeline has been up for a while (24h gives the most
meaningful numbers, but it works on any window):

    python metrics.py
"""

import os
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = os.getenv("LAKE_PATH", "./lake")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("metar-metrics")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def file_stats(path: str) -> tuple[int, str]:
    count = subprocess.run(
        f"find {path} -name '*.parquet' | wc -l",
        shell=True, capture_output=True, text=True,
    ).stdout.strip()
    size = subprocess.run(
        f"du -sh {path}", shell=True, capture_output=True, text=True,
    ).stdout.split()[0]
    return int(count), size


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("delta").load(f"{LAKE}/silver/metar_observations")
    bronze = spark.read.format("delta").load(f"{LAKE}/bronze/metar_raw")

    total = silver.count()
    window = silver.agg(
        F.min("observed_at").alias("first"),
        F.max("observed_at").alias("last"),
    ).collect()[0]

    span_s = (window["last"] - window["first"]).total_seconds() or 1

    # End-to-end latency: how long from observation time to landing in Kafka.
    latency = silver.approxQuantile("lag_seconds", [0.5, 0.95, 0.99], 0.01)

    dupes = (
        silver.groupBy("station_id", "observed_at")
        .count()
        .filter("count > 1")
        .count()
    )

    stations = silver.select("station_id").distinct().count()

    print("\n" + "=" * 55)
    print("METAR STREAM — BENCHMARKS")
    print("=" * 55)
    print(f"window            {window['first']} .. {window['last']}")
    print(f"duration          {span_s / 3600:.2f} h")
    print(f"observations      {total:,}")
    print(f"distinct stations {stations:,}")
    print(f"throughput        {total / span_s:.1f} obs/sec sustained")
    print()
    print(f"latency p50       {latency[0]:.0f} s")
    print(f"latency p95       {latency[1]:.0f} s")
    print(f"latency p99       {latency[2]:.0f} s")
    print()
    print(f"duplicate keys    {dupes}   <- must be 0")
    print()

    for name, path in [
        ("bronze", f"{LAKE}/bronze/metar_raw"),
        ("silver", f"{LAKE}/silver/metar_observations"),
        ("gold/15min", f"{LAKE}/gold/metar_15min"),
        ("gold/alerts", f"{LAKE}/gold/metar_alerts"),
    ]:
        try:
            count, size = file_stats(path)
            print(f"{name:<12} {count:>6} files   {size:>8}")
        except Exception as exc:
            print(f"{name:<12} unavailable ({exc})")

    print()
    print(f"bronze rows       {bronze.count():,} (includes re-published dupes)")
    print("=" * 55 + "\n")

    print("Top wind gusts observed:")
    (
        silver.select("station_id", "name", "observed_at", "wind_gust_kt")
        .filter(F.col("wind_gust_kt").isNotNull())
        .orderBy(F.desc("wind_gust_kt"))
        .show(10, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
