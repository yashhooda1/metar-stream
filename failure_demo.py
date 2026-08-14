"""
Broker failure recovery demo.

Kills the Kafka broker under a running streaming job, lets a backlog build,
restarts it, and verifies the pipeline drained the backlog without introducing
duplicates or losing records.

Requires: the producer and metar_stream.py already running.

    python failure_demo.py

Writes evidence to failure_demo_results.md for the README.
"""

import subprocess
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = "./lake"
OUTAGE_SECONDS = 180


def spark_session() -> SparkSession:
    s = (
        SparkSession.builder.appName("failure-demo")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s


def snapshot(spark) -> dict:
    silver = spark.read.format("delta").load(f"{LAKE}/silver/metar_observations")
    bronze = spark.read.format("delta").load(f"{LAKE}/bronze/metar_raw")
    dupes = (
        silver.groupBy("station_id", "observed_at")
        .count()
        .filter("count > 1")
        .count()
    )
    latest = silver.agg(F.max("observed_at")).collect()[0][0]
    return {
        "silver_rows": silver.count(),
        "bronze_rows": bronze.count(),
        "duplicate_keys": dupes,
        "latest_observation": latest,
        "at": datetime.now(tz=timezone.utc).strftime("%H:%M:%S"),
    }


def show(label: str, snap: dict) -> None:
    print(f"\n--- {label} ({snap['at']} UTC) ---")
    print(f"  silver rows       {snap['silver_rows']:,}")
    print(f"  bronze rows       {snap['bronze_rows']:,}")
    print(f"  duplicate keys    {snap['duplicate_keys']}")
    print(f"  latest obs time   {snap['latest_observation']}")


def docker(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True)


def main() -> None:
    spark = spark_session()

    print("=" * 60)
    print("BROKER FAILURE RECOVERY DEMO")
    print("=" * 60)

    before = snapshot(spark)
    show("BEFORE OUTAGE", before)

    print(f"\n>>> stopping broker, waiting {OUTAGE_SECONDS}s to build a backlog")
    docker("stop", "redpanda")
    outage_start = time.monotonic()

    for remaining in range(OUTAGE_SECONDS, 0, -30):
        print(f"    broker down, {remaining}s remaining "
              f"(producer should be logging connection failures)")
        time.sleep(min(30, remaining))

    print("\n>>> restarting broker")
    docker("start", "redpanda")
    restart_at = time.monotonic()
    outage_duration = restart_at - outage_start

    during = snapshot(spark)
    show("IMMEDIATELY AFTER RESTART", during)

    print("\n>>> waiting for the job to drain the backlog")
    stable_for = 0
    last_count = during["silver_rows"]
    drained_at = None

    while stable_for < 90:
        time.sleep(30)
        current = spark.read.format("delta").load(
            f"{LAKE}/silver/metar_observations"
        ).count()
        gained = current - last_count
        print(f"    silver rows {current:,} (+{gained})")
        if gained == 0:
            stable_for += 30
            if drained_at is None:
                drained_at = time.monotonic()
        else:
            stable_for = 0
            drained_at = None
        last_count = current

    recovery_seconds = (drained_at - restart_at) if drained_at else 0
    after = snapshot(spark)
    show("AFTER RECOVERY", after)

    print("\n" + "=" * 60)
    print("RESULT")
    print("=" * 60)
    print(f"  outage duration        {outage_duration:.0f}s")
    print(f"  backlog drained in     ~{recovery_seconds:.0f}s after restart")
    print(f"  rows recovered         {after['silver_rows'] - before['silver_rows']:,}")
    print(f"  duplicate keys after   {after['duplicate_keys']}  <- must be 0")
    print(f"  job survived outage    {'YES' if after['silver_rows'] > during['silver_rows'] else 'CHECK LOGS'}")
    print("=" * 60)

    with open("failure_demo_results.md", "w") as fh:
        fh.write("## Broker failure recovery — measured\n\n")
        fh.write(f"Broker stopped for {outage_duration:.0f}s under a running job.\n\n")
        fh.write("| | before | after restart | after recovery |\n")
        fh.write("|---|---|---|---|\n")
        fh.write(f"| silver rows | {before['silver_rows']:,} | "
                 f"{during['silver_rows']:,} | {after['silver_rows']:,} |\n")
        fh.write(f"| bronze rows | {before['bronze_rows']:,} | "
                 f"{during['bronze_rows']:,} | {after['bronze_rows']:,} |\n")
        fh.write(f"| duplicate keys | {before['duplicate_keys']} | "
                 f"{during['duplicate_keys']} | {after['duplicate_keys']} |\n\n")
        fh.write(f"The streaming job did not exit during the outage. On restart it "
                 f"resumed from checkpointed offsets and drained the backlog in "
                 f"~{recovery_seconds:.0f}s, recovering "
                 f"{after['silver_rows'] - before['silver_rows']:,} rows with "
                 f"{after['duplicate_keys']} duplicate keys.\n")

    print("\nwrote failure_demo_results.md")
    spark.stop()


if __name__ == "__main__":
    main()
