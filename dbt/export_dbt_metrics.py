#!/usr/bin/env python3
"""Publish dbt model + audit metrics as JSON for yashhooda.ai.

Mirrors the metar_pipeline.json convention: writes to docs/, which publish.sh
pushes to the `data` branch. Run from the dbt/ directory after `dbt build`,
so target/run_results.json reflects the current run.

    cd dbt && dbt build && python export_dbt_metrics.py
"""

import json
import pathlib
from datetime import datetime, timezone

from pyspark.sql import SparkSession

REPO = pathlib.Path(__file__).resolve().parent.parent
OUT = REPO / "docs" / "metar_dbt.json"
RUN_RESULTS = pathlib.Path(__file__).resolve().parent / "target" / "run_results.json"

spark = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)


def rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def one(sql, col):
    return rows(sql)[0][col]


# --- parity against the PySpark Gold path -----------------------------------
parity = {r["diff_type"]: r["n"] for r in rows(
    "select diff_type, count(*) n from metar_audit.audit_gold_parity group by diff_type"
)}
total_hours = one("select count(*) n from metar_gold_dbt.agg_station_hourly", "n")
mismatched = sum(parity.values())

# --- ceiling parser graded against NOAA's published category ----------------
# "Non-trivial" excludes rows where no ceiling was reported and conditions were
# VFR anyway -- those agree by construction and would inflate the headline.
grading = rows("""
    select
        count(*)                                                    as graded,
        sum(case when category_agrees then 1 else 0 end)            as agree,
        sum(case when ceiling_ft_agl is not null
                   or flight_category_noaa <> 'VFR' then 1 else 0 end) as non_trivial,
        sum(case when (ceiling_ft_agl is not null
                        or flight_category_noaa <> 'VFR')
                      and category_agrees then 1 else 0 end)        as non_trivial_agree
    from metar_gold_dbt.fct_observations
    where flight_category_noaa is not null
""")[0]

# Rows NOAA published no category for, where the derived value fills the gap.
derived_only = one("""
    select count(*) n from metar_gold_dbt.fct_observations
    where flight_category_noaa is null and flight_category_derived <> 'UNKNOWN'
""", "n")

# --- dbt test results -------------------------------------------------------
tests = {"total": 0, "passed": 0}
if RUN_RESULTS.exists():
    results = json.loads(RUN_RESULTS.read_text())["results"]
    t = [r for r in results if r["unique_id"].startswith("test.")]
    tests = {"total": len(t), "passed": sum(1 for r in t if r["status"] == "pass")}

payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "models": {
        "observations": one("select count(*) n from metar_gold_dbt.fct_observations", "n"),
        "station_hours": total_hours,
        "stations": one("select count(*) n from metar_gold_dbt.dim_stations", "n"),
        "transitions": one("select count(*) n from metar_gold_dbt.fct_category_transitions", "n"),
    },
    "tests": tests,
    "parity": {
        "hours_compared": total_hours,
        "hours_mismatched": mismatched,
        "pct": round(100.0 * (total_hours - mismatched) / total_hours, 2) if total_hours else None,
        "by_type": parity,
    },
    "ceiling_parser": {
        "graded": grading["graded"],
        "agree": grading["agree"],
        "non_trivial": grading["non_trivial"],
        "non_trivial_agree": grading["non_trivial_agree"],
        "derived_only": derived_only,
    },
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(payload, indent=2))
print(f"wrote {OUT}")
print(json.dumps(payload, indent=2))
