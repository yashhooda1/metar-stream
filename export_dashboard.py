"""
Export the gold layer to a compact JSON for the yashhooda.ai dashboard.

The pipeline runs locally, so the website cannot query it directly. This writes
a snapshot the site can fetch statically — same pattern as ClimatePulse.

    python export_dashboard.py

Writes docs/metar_dashboard.json (small enough to commit).
"""

import json
import os
import pathlib
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LAKE = os.getenv("LAKE_PATH", "./lake")
OUT = pathlib.Path("docs/metar_pipeline.json")

# Canonical FAA flight categories, worst to best.
CATEGORY_ORDER = ["LIFR", "IFR", "MVFR", "VFR"]


def build_spark() -> SparkSession:
    s = (
        SparkSession.builder.appName("metar-export")
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


def main() -> None:
    spark = build_spark()

    silver = spark.read.format("delta").load(f"{LAKE}/silver/metar_observations")
    bronze = spark.read.format("delta").load(f"{LAKE}/bronze/metar_raw")

    # Latest observation per station — the current picture of US airspace.
    latest = (
        silver.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("station_id").orderBy(F.desc("observed_at"))
            ),
        )
        .filter("rn = 1")
        .drop("rn")
    )

    categories = {
        r["flight_category"]: r["n"]
        for r in latest.groupBy("flight_category")
        .agg(F.count("*").alias("n"))
        .collect()
        if r["flight_category"]
    }

    gusts = [
        {
            "station": r["station_id"],
            "name": r["name"],
            "gust_kt": r["wind_gust_kt"],
            "observed_at": r["observed_at"].isoformat(),
        }
        for r in silver.filter(F.col("wind_gust_kt").isNotNull())
        .orderBy(F.desc("wind_gust_kt"))
        .limit(12)
        .collect()
    ]

    alerts = [
        {
            "station": r["station_id"],
            "name": r["name"],
            "category": r["flight_category"],
            "gust_kt": r["wind_gust_kt"],
            "visibility_mi": r["visibility_mi"],
            "observed_at": r["observed_at"].isoformat(),
        }
        for r in latest.filter(
            (F.col("flight_category").isin("IFR", "LIFR"))
            | (F.col("wind_gust_kt") >= 35)
        )
        .orderBy(F.desc("observed_at"))
        .limit(20)
        .collect()
    ]

    silver_rows = silver.count()
    bronze_rows = bronze.count()
    window = silver.agg(
        F.min("observed_at").alias("a"), F.max("observed_at").alias("b")
    ).collect()[0]

    payload = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "window": {
            "from": window["a"].isoformat(),
            "to": window["b"].isoformat(),
        },
        "stats": {
            "stations": latest.count(),
            "observations_retained": silver_rows,
            "messages_ingested": bronze_rows,
            "dedupe_pct": round(100 * (1 - silver_rows / bronze_rows), 1),
        },
        "flight_categories": {c: categories.get(c, 0) for c in CATEGORY_ORDER},
        "top_gusts": gusts,
        "alerts": alerts,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes)")
    print(json.dumps(payload["stats"], indent=2))

    spark.stop()


if __name__ == "__main__":
    main()
