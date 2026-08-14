# METAR Stream — real-time aviation weather pipeline

Continuous ingestion of METAR surface observations from ~5,000 US airport
weather stations, through Kafka into a Spark Structured Streaming job that
writes a Delta Lake medallion and emits a low-visibility alert stream.

```
NOAA Aviation Weather API
        │  (poll 60s, ~5k obs/cycle)
        ▼
   metar_producer.py ──▶ Redpanda topic: metar.raw
                              │  keyed by ICAO station id
                              ▼
                    Spark Structured Streaming
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   bronze/metar_raw     silver/metar_observations   gold/
   (append-only,        (parsed, typed,             ├─ metar_15min
    replayable)          watermarked, deduped)      └─ metar_alerts
```

## Why these choices

**Redpanda over Kafka** — Kafka API compatible, single binary, no ZooKeeper or
KRaft config. Runs on a laptop. Swapping to MSK or Confluent Cloud is a
bootstrap-server change.

**Watermark of 30 minutes** — METAR arrives late as a matter of course; a
station's uplink drops and it flushes an hour of backlog at once. The watermark
bounds streaming state so it doesn't grow without limit. The tradeoff is
explicit: anything later than 30 minutes is dropped, and the drop count is
tracked in the gold table via `avg_ingest_lag_s`.

**Dedupe in Spark, not the producer** — the upstream API returns the current
observation set on every poll, so the same observation is published repeatedly.
`dropDuplicates(["station_id", "observed_at"])` under a watermark handles it
with bounded state. Producers stay simple; the stream carries the logic.

**Idempotent producer + checkpointed sinks** — `enable.idempotence=true` with
`acks=all` on the write side, per-sink `checkpointLocation` on the read side.
Together with the dedupe key this gives effectively-exactly-once end to end.

**maxOffsetsPerTrigger=50000** — backpressure. Without it the first micro-batch
after an outage attempts the entire backlog and the executors die.

## Running it

```bash
docker compose up -d
pip install -r requirements.txt

# terminal 1
python metar_producer.py

# terminal 2
python metar_stream.py
```

Redpanda Console at http://localhost:8080 to watch the topic fill.

## Failure recovery demo

This is the part worth recording as a GIF for the repo:

1. Let the pipeline run until the gold table has several closed windows.
2. `docker compose stop redpanda` — the streaming job logs connection failures
   but does not exit.
3. Wait ~3 minutes so a real backlog accumulates upstream.
4. `docker compose start redpanda`.
5. The job resumes from its checkpointed offsets, drains the backlog across
   several throttled micro-batches, and the row count in silver matches the
   pre-failure count plus the backlog — no duplicates, no gap.

Query to prove it:

```sql
SELECT station_id, observed_at, COUNT(*) AS c
FROM delta.`./lake/silver/metar_observations`
GROUP BY station_id, observed_at
HAVING c > 1;
-- expected: zero rows
```

## Benchmarks to fill in

Run it for 24 hours and record actual numbers. These are what turn a portfolio
project into a conversation:

- observations/second sustained
- p50 / p95 end-to-end latency (`ingested_at` minus `observed_at`)
- state store size under the 30-minute watermark
- recovery time from the failure demo above
- records dropped as late-arriving, as a percentage

## Next

- Schema Registry + Avro instead of JSON (Redpanda exposes it on :8081)
- Delta `OPTIMIZE` / `VACUUM` maintenance job
- Wire `gold/metar_alerts` into the ClimatePulse dashboard on yashhooda.ai
