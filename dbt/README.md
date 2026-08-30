# metar_dbt

dbt layer over the METAR streaming pipeline. `metar_stream.py` owns ingestion
through Silver; dbt owns Silver -> Gold modeling, testing, and lineage.

Runs **alongside** the existing PySpark Gold job rather than replacing it.
`audit_gold_parity` diffs the two so the equivalence can be demonstrated before
anything is cut over.

## What it adds beyond the Spark Gold tables

Silver publishes NOAA's `flight_category` but carries no cloud structure. This
project parses the ceiling out of `raw_text`, derives the category
independently, and then grades itself against NOAA in
`audit_category_agreement`. That turns an opaque passthrough field into a
checkable derivation — the disagreement rate is a measurable claim rather than
an assumed one.

## Lineage

```
lake/silver/metar_observations ─ stg_metar__observations ─┬─ int_metar__ceiling ─┐
                                                          └──────────────────────┴─ fct_observations
                                                                                    ├─ dim_stations
                                                                                    ├─ agg_station_hourly ─┐
                                                                                    └─ fct_category_transitions
                                                                                                           │
lake/gold/metar_15min (PySpark) ───────────────────────────────────────────────────── audit_gold_parity ───┘
fct_observations ─────────────────────────────────────────────────── audit_category_agreement
```

## Setup

1. Register the path-addressed Delta tables in the metastore (once):

   ```bash
   $SPARK_HOME/sbin/start-thriftserver.sh \
     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
     --packages io.delta:delta-spark_2.12:3.2.0

   beeline -u jdbc:hive2://localhost:10000 -f dbt/bootstrap_catalog.sql
   ```

   The streaming job must not be running under a second JVM at the same time —
   the same one-writer constraint that corrupted the checkpoints applies here.
   Check `ps aux | grep "[j]ava" | wc -l` first.

2. Install dbt in its own venv, not the pipeline's. dbt-spark and PySpark pin
   conflicting versions of several transitive dependencies:

   ```bash
   python -m venv .venv-dbt && source .venv-dbt/bin/activate
   pip install "dbt-spark[PyHive]"
   cp dbt/profiles.example.yml ~/.dbt/profiles.yml
   cd dbt && dbt deps && dbt debug
   ```

3. Build:

   ```bash
   dbt build                      # run + test
   dbt source freshness           # confirms the stream is still landing data
   dbt docs generate && dbt docs serve
   ```

## Reading the audits

```sql
-- How often does the parsed ceiling reproduce NOAA's category?
SELECT * FROM metar_audit.audit_category_agreement ORDER BY is_agreement, observation_count DESC;

-- Where do the Spark and dbt aggregates disagree? Empty means parity.
SELECT diff_type, COUNT(*) FROM metar_audit.audit_gold_parity GROUP BY diff_type;
```

`audit_gold_parity` excludes the current hour, which is still filling on both
sides. Expect a residue of `COUNT_MISMATCH` on the oldest hour if Spark's Gold
was started later than Silver.

## Schema notes

Built against the actual Silver schema, not assumptions:

| Silver column | Handling |
|---|---|
| `visibility_sm` (string) | Raw token, e.g. `10+`. Passed through as `visibility_raw`; never used numerically |
| `visibility_mi` (double) | The parsed value everything computes on |
| `altimeter_hpa` | Converted to inHg (`/ 33.8639`) alongside the original |
| `flight_category` | NOAA's, kept as `flight_category_noaa` and never conflated with the derived value |
| `raw_text` | Only source of cloud layers; regex-parsed in `int_metar__ceiling` |
| `lat`/`lon`/`name`/`elevation_m` | Per-observation, so `dim_stations` is built from Silver rather than a station list |
| `obs_date` | Reused as the partition key, matching the streaming job's choice |

No dedup window: Spark already applies `dropDuplicates(["station_id", "observed_at"])`.

## Ceiling parsing

Cloud groups are three letters plus hundreds of feet — `BKN008` is 800 ft AGL.
Only BKN, OVC, and VV are ceilings; FEW and SCT are not, however low.

Two deliberate exclusions: everything after `RMK`, and groups with unknown
heights (`BKN///`), which yield null rather than a fabricated zero. Nulls are
treated as unlimited, which is the bug most homegrown decoders ship with — a
null ceiling read as zero turns clear skies into LIFR.

| Category | Ceiling (ft AGL) | Visibility (sm) |
|---|---|---|
| LIFR | < 500 | < 1 |
| IFR | 500–999 | 1–2.9 |
| MVFR | 1000–3000 | 3–5 |
| VFR | > 3000 | > 5 |

Worse of the two wins.

## Incremental behavior

Marts merge on a surrogate key over a trailing `metar_lookback_hours` window
(default 6). `fct_category_transitions` uses 4x that, because computing a
transition requires the observation before it.

```bash
dbt run -s fct_observations+ --vars '{metar_lookback_hours: 168}'   # backfill a week
```
