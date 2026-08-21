# METAR Stream

Real-time ingestion of US aviation weather observations into a Delta Lake
medallion, via Kafka and Spark Structured Streaming.

METAR reports are surface weather observations published by airport stations —
wind, visibility, temperature, ceiling, flight category. Roughly 1,100 North
American stations report at any given time, most on a 20–60 minute cadence, arriving
continuously and out of order. That makes them a genuine unbounded stream
rather than a batch job wearing a streaming costume.

```
NOAA Aviation Weather API
        │  6 tiled requests every 60s
        ▼
   metar_producer.py ──▶ Redpanda topic: metar.raw
                              │  keyed by ICAO station id
                              ▼
                    Spark Structured Streaming
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   bronze/metar_raw     silver/metar_observations   gold/
   append-only          parsed, watermarked,        ├─ metar_15min
   replayable           deduplicated                ├─ metar_alerts
                              │                     └─ metar_quality_15min
                              ▼
                    quarantine/metar_rejected
                    payload + rejection reasons
```

## Running it

```bash
docker compose up -d          # Redpanda broker + console on :8080
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python metar_producer.py      # terminal 1
python metar_stream.py        # terminal 2
```

Requires Python 3.12 or lower (PySpark 3.5.1 does not support 3.13+) and a JRE.


## Quality gates

Every pull request runs a Python 3.12 CI job that compiles the codebase and
executes both producer unit tests and local Spark transformation tests. The
suite covers record normalization, malformed timestamps, partial regional API
failures, the Silver data-quality contract, deduplication, visibility parsing,
and gust fallback without requiring Kafka, Delta Lake, or a live NOAA request.

```bash
python -m unittest discover -s tests -v
```

Silver rejects impossible values while Bronze retains the original Kafka payload
for replay. Current contract limits are four-character ICAO identifiers, valid
coordinates, temperatures from -100°C to 70°C, and wind values from 0–250 kt.


## Data-quality observability

Rejected records are not discarded. Spark writes the original JSON payload,
parsed identifiers, rejection timestamp, and one or more stable reason codes to
`quarantine/metar_rejected`. A separate Gold stream explodes those codes into
15-minute counts at `gold/metar_quality_15min`, ready for dashboards and
threshold alerts.

Current reason codes:

- `malformed_payload`
- `invalid_station_id`
- `missing_observed_at`
- `latitude_out_of_range` / `longitude_out_of_range`
- `temperature_out_of_range`
- `wind_speed_out_of_range` / `wind_gust_out_of_range`

```sql
SELECT reason, SUM(rejected_count) AS rejected
FROM delta.`./lake/gold/metar_quality_15min`
GROUP BY reason
ORDER BY rejected DESC;
```

## Design decisions

**Redpanda over Kafka.** Kafka API compatible, single binary, no ZooKeeper or
KRaft configuration. Moving to MSK or Confluent Cloud is a bootstrap-server
change and nothing else.

**Watermark of 30 minutes.** METAR arrives late routinely — a station's uplink
drops and it flushes an hour of backlog at once. The watermark bounds how long
streaming state is retained so it cannot grow without limit. The tradeoff is
explicit: observations later than 30 minutes are dropped. Ingest lag is tracked
per window in the gold table so the cost of that choice stays visible.

**Dedupe in Spark, not the producer.** The upstream API returns the current
observation set on every poll, so identical observations are republished
repeatedly. `dropDuplicates(["station_id", "observed_at"])` under a watermark
handles this with bounded state. Producers stay simple; the stream carries the
logic.

**Idempotent producer, checkpointed sinks.** `enable.idempotence=true` with
`acks=all` on the write path, a dedicated `checkpointLocation` per sink on the
read path. Combined with the dedupe key, this yields effectively-exactly-once
end to end.

**Backpressure via `maxOffsetsPerTrigger`.** Capped at 50,000. Without it the
first micro-batch after an outage attempts the entire backlog in one pass and
the executors die.

**Coordinated shutdown.** Every sink has a stable query name. SIGINT and SIGTERM
set a shutdown event, the supervisor stops all active queries, waits for their
checkpoint commits, and then stops Spark. Cleanup continues if any individual
query throws, preventing one failed sink from stranding the remaining JVM work.

## Problems found and fixed

Five issues surfaced during development. Each is worth reading as a category of
failure, not just a bug.

### 1. Malformed request returned success, not an error

The API expects `bbox` as `(lat0, lon0, lat1, lon1)`. Passing longitude first
returned HTTP 200 with an empty body, so the failure surfaced only as a JSON
parse error several layers away from the cause. Fixed by correcting the
parameter order and logging the response body on parse failure — a parse error
that hides its own payload is nearly undebuggable.

### 2. The API silently truncates at 400 results

A single CONUS-wide query returned exactly 400 observations on every cycle. The
number being suspiciously round was the only clue; nothing in the response
indicated truncation. Verified by querying a smaller region and getting 244 —
proving 400 was a ceiling, not a station count.

Fixed by partitioning the geographic query space into six tiles, each verified
to return under the cap. Coverage went from 400 to ~1,090 observations per
cycle. The producer now warns if any tile reaches the limit, so future station
growth surfaces immediately instead of silently losing data.

Silent truncation is among the most common ways pipelines lose data without
anyone noticing.

**Coverage note.** The tiles extend to latitude 50, which crosses into southern
Canada, and the southern edge reaches northern Mexico. Grouping station IDs by
ICAO prefix showed 996 K (US), 90 C (Canada), 18 M (Mexico) — so nearly 10% of
coverage is non-US. The bounding box was chosen against API result limits, not
political borders. The extra stations were kept and the description corrected;
the original "US stations" claim was an assumption the data did not support.

### 3. Per-tile isolation contains upstream failures

Tiling produced an unplanned benefit. When one region returned HTTP 502:

```
19:00:42 WARNING tile 38,-125,50,-105 failed: 502 Server Error
19:00:49 INFO published=903 skipped=0
19:02:02 INFO published=1091 skipped=0
```

One region was lost for one cycle and recovered on its own. Before tiling, the
same 502 would have cost the entire batch.

### 4. High-cardinality partitioning produced 2,364 files for 38 MB

Silver was originally partitioned by `station_id`. With ~1,090 stations, every
micro-batch wrote one small file per station.

| | files | size |
|---|---|---|
| `partitionBy("station_id")` | 2,364 | 38 MB |
| `partitionBy("obs_date")` | 6 | same data |
| bronze (unpartitioned, same window) | 34 | 6.5 MB |

At ~16 KB per file, Parquet footer and column metadata dominated the actual
payload. Extrapolated to a full day that is roughly 95,000 files. Partition
keys need low cardinality relative to write frequency; station identity is a
query filter, not a partition key.

### 5. Checkpoint directories assume exactly one writer

Ctrl-C on a PySpark job signals the Python process, but the JVM does not always
receive it. An orphaned JVM kept running and a second job was started against
the same checkpoint directories. Both advanced the offset log independently:

```
ERROR MicroBatchExecution: The offset log for batch 43 doesn't exist,
which is required to restart the query from the latest batch 44
```

Recovery was to drop the corrupted checkpoints and rebuild downstream layers
from Kafka. Nothing was lost, because bronze is append-only and the source
topic retains the data — which is the entire argument for the medallion
pattern, demonstrated rather than asserted.

Standard practice now before starting the job:

```bash
ps aux | grep "[j]ava" | wc -l    # must be 0
```

### 6. Measured before tuning, and did not tune

High p95 ingest lag (3,174 s) suggested the 30-minute watermark was dropping
late-arriving data. Rather than widening it, the drop rate was measured
directly — distinct `(station_id, observed_at)` keys in bronze against row
count in silver:

```
bronze distinct keys: 4,179
silver rows:          4,177
dropped as late:      2 (0.0%)
```

Ingest lag and event-time lateness are different quantities. `lag_seconds`
measures how stale an observation is when it reaches Kafka; a station reporting
hourly hands over a report that is already ~50 minutes old, but on first arrival
it is not late relative to the stream's watermark. The republished copies
inflating p95 were being removed by dedupe, not dropped by the watermark.

Widening to 2 hours would have quadrupled state-store retention to solve a
problem that did not exist. Configuration left unchanged.

## Failure recovery demo

The broker was stopped for 181 seconds under a running streaming job, then
restarted. `failure_demo.py` automates this and captures the evidence.

| claim | evidence |
|---|---|
| job survives broker loss | did not exit during the outage; logged connection failures and continued |
| resumes from checkpointed offsets | 1,379 rows landed in silver after restart |
| no duplicates introduced | 0 duplicate `(station_id, observed_at)` keys, before and after |
| no gap in coverage | latest observation advanced 20:22 → 21:02 across the outage |
| bronze remained replayable | 110,563 → 156,000 raw messages, append-only throughout |

**Measurement limitation.** Recovery time was not isolated. The producer kept
polling the upstream API throughout, so row growth after restart mixes backlog
drain with ordinary ingestion and the two cannot be separated from row counts
alone. Cleanly measuring drain time would require pausing the producer for the
duration of the outage, or reading `StreamingQueryProgress.numInputRows` per
micro-batch rather than table counts.

Duplicate check, run against silver at any time:

```sql
SELECT station_id, observed_at, COUNT(*) AS c
FROM delta.`./lake/silver/metar_observations`
GROUP BY station_id, observed_at
HAVING c > 1;
-- expected: zero rows
```

## Benchmarks

Measured over a 2.78 hour window (2026-08-13 17:25 – 20:12 UTC):

| metric | value |
|---|---|
| distinct stations | 1,096 |
| observations retained (silver) | 4,157 |
| raw messages ingested (bronze) | 99,664 |
| deduplication ratio | 96% discarded as republished duplicates |
| duplicate keys surviving | 0 |
| late-arrival drop rate | 0.0% (2 of 4,179) |
| ingest lag p50 / p95 / p99 | 291 s / 3,174 s / 4,914 s |

Throughput is source-limited, not pipeline-limited: stations report on a
20–60 minute cadence, so retained volume reflects the upstream cadence rather
than any capacity ceiling. The trigger is capped at 50,000 offsets per
micro-batch.

Ingest lag percentiles are high by design — the API serves each observation
repeatedly for as long as it remains current, so the tail is dominated by
re-delivery of already-seen records rather than by pipeline delay.

Still to measure: state store size, and isolated backlog-drain time\n(see the measurement limitation noted above).

## Next

- Schema Registry with Avro instead of JSON (Redpanda exposes it on :8081)
- Scheduled Delta `OPTIMIZE` and `VACUUM`
- Surface `gold/metar_alerts` on the ClimatePulse dashboard
