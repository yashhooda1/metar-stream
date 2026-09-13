#!/usr/bin/env bash
#
# One-off full refresh of the dbt Gold incrementals.
#
# Rebuilds the incremental models from all of Silver instead of from the
# incremental watermark, which is what clears the MISSING_IN_DBT rows in the
# parity audit. Intended to be run by hand, not on a schedule — the normal
# path stays dbt/refresh.sh.
#
# Usage:
#   dbt/full_refresh_gold.sh                 # rebuild the three gold incrementals
#   MODELS="fct_observations" dbt/full_refresh_gold.sh
#   KILL_ORPHANS=1 dbt/full_refresh_gold.sh  # reap leftover Spark JVMs first
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_DIR="$REPO_ROOT/dbt"
LAKE_DIR="$REPO_ROOT/lake/gold_dbt"

# fct_category_transitions is included on purpose: it is incremental and derives
# from fct_observations with lag/window logic, so backfilling observations
# underneath a stale transitions table leaves transitions that point at rows
# whose neighbours have changed. Drop it from MODELS only if you know better.
MODELS="${MODELS:-fct_observations agg_station_hourly fct_category_transitions}"
KILL_ORPHANS="${KILL_ORPHANS:-0}"

if [[ -f "$REPO_ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.venv/bin/activate"
fi
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DBT_DIR}"

if [[ "$KILL_ORPHANS" == "1" ]]; then
  echo "== reaping orphaned Spark JVMs =="
  pkill -f 'pyspark.daemon' 2>/dev/null || true
  pkill -f 'org.apache.spark.deploy.SparkSubmit' 2>/dev/null || true
  sleep 2
fi

# Record the current Delta version of each table before we replace it, so the
# rebuild is reversible. --full-refresh issues CREATE OR REPLACE, which is a new
# commit on the existing Delta log rather than a delete, so time travel still works.
echo "== pre-refresh Delta versions (for rollback) =="
declare -A BEFORE
for model in $MODELS; do
  log_dir="$LAKE_DIR/$model/_delta_log"
  if [[ -d "$log_dir" ]]; then
    version=$(find "$log_dir" -maxdepth 1 -name '*.json' -printf '%f\n' \
      | sed 's/\.json$//' | sort -n | tail -1)
    BEFORE["$model"]=$((10#$version))
    echo "  $model: version ${BEFORE[$model]}"
  else
    echo "  $model: no Delta log at $log_dir (new table?)"
  fi
done

cd "$DBT_DIR"

echo
echo "== dbt run --full-refresh --select $MODELS =="
# shellcheck disable=SC2086
dbt run --full-refresh --select $MODELS

echo
echo "== dbt test --select $MODELS =="
# shellcheck disable=SC2086
dbt test --select $MODELS

echo
echo "== done =="
echo "Re-run dbt/refresh.sh to regenerate docs/metar_dbt.json and get fresh"
echo "parity numbers. Expect hours_mismatched to drop from 745 toward 39 —"
echo "the remaining COUNT_MISMATCH rows are a real logic difference between"
echo "the dbt models and the PySpark Gold job, not a backfill gap."
echo
echo "To roll back, in a Spark session with Delta:"
for model in $MODELS; do
  if [[ -n "${BEFORE[$model]:-}" ]]; then
    echo "  RESTORE TABLE metar_gold_dbt.$model TO VERSION AS OF ${BEFORE[$model]};"
  fi
done
