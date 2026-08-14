# METAR Stream

Real-time ingestion of US aviation weather observations into a Delta Lake
medallion, via Kafka and Spark Structured Streaming.

METAR reports are surface weather observations published by airport stations —
wind, visibility, temperature, ceiling, flight category. Roughly 1,100 US
stations report at any given time, most on a 20–60 minute cadence, arriving
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
   replayable           deduplicated                └─ metar_alerts
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

## Failure recovery demo

1. Run until gold has several closed windows.
2. `docker compose stop redpanda` — the streaming job logs connection failures
   and does not exit.
3. Wait ~3 minutes to accumulate a backlog.
4. `docker compose start redpanda`.
5. The job resumes from checkpointed offsets and drains the backlog across
   throttled micro-batches.

Verify no duplicates were introduced:

```sql
SELECT station_id, observed_at, COUNT(*) AS c
FROM delta.`./lake/silver/metar_observations`
GROUP BY station_id, observed_at
HAVING c > 1;
-- expected: zero rows
```

## Benchmarks

_To be filled from a 24-hour run:_

- sustained observations/second
- p50 / p95 end-to-end latency (`ingested_at` − `observed_at`)
- state store size under the 30-minute watermark
- recovery time from broker failure
- late-arriving records dropped, as a percentage

## Next

- Schema Registry with Avro instead of JSON (Redpanda exposes it on :8081)
- Scheduled Delta `OPTIMIZE` and `VACUUM`
- Graceful shutdown: stop each query before exiting rather than dying on Ctrl-C
- Surface `gold/metar_alerts` on the ClimatePulse dashboard
