#!/usr/bin/env bash
# Daily dbt refresh: build models, export metrics, publish to the data branch.
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate || exit 1
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.2.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"
cd dbt && dbt build && python export_dbt_metrics.py || exit 1
cd .. && ./publish.sh
