"""
Spark Structured Streaming: ERCOT grid records -> Delta medallion.

Bronze     : raw Kafka payload, append-only, replayable. Same as METAR.
Quarantine : records that fail the silver contract, with reason codes.
Silver     : typed records, one row per (feed, series, market, observed_at),
             kept at the LATEST revision ERCOT published.

The silver write is where this job deliberately differs from metar_stream.py.

  METAR observations never change once issued, so dropDuplicates under a
  watermark is correct there: the first copy is as good as any other.

  ERCOT revises recent intervals. A fuel-mix value for 14:05 can be republished
  later with a different number. dropDuplicates would freeze the first,
  possibly provisional, value forever. So silver is written with foreachBatch:
  each micro-batch is reduced to the newest revision per key (by the source
  document's lastUpdated), then MERGEd into the Delta table, replacing a stored
  row only when the incoming revision is newer. Replaying old Kafka offsets
  therefore cannot overwrite newer data.

Gold is built by ercot_gold.py as a batch job over silver. Streaming reads of a
table that receives MERGE updates need Delta change data feed; batch keeps the
first version simple and correct (see README "ERCOT grid").
"""

import logging
import os
import signal
import threading

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from metar_stream import await_shutdown, stop_queries

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("ERCOT_TOPIC", "ercot.raw")
LAKE = os.getenv("LAKE_PATH", "./lake")
SILVER_PATH = f"{LAKE}/silver/ercot_observations"

log = logging.getLogger("ercot-stream")

ERCOT_SCHEMA = StructType(
    [
        StructField("feed", StringType(), False),
        StructField("series", StringType(), False),
        StructField("market", StringType(), True),
        StructField("observed_at", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("capacity_mw", DoubleType(), True),
        StructField("available_mw", DoubleType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

# Natural key of a silver row. market is null for demand and fuel, and SQL
# null never equals null, so merges compare a coalesced version.
KEY = ["feed", "series", "market_key", "observed_at"]

PRICE_MIN, PRICE_MAX = -1000.0, 10000.0
MW_MAX = 200_000.0


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ercot-stream")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.2.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Timestamps are UTC end to end; do not let the host zone leak into obs_date or hour buckets.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("failOnDataLoss", "false")
        .load()
    )


def _error_if(condition, code: str):
    empty = F.array().cast("array<string>")
    return F.when(condition, F.array(F.lit(code))).otherwise(empty)


def evaluate_quality(raw):
    """Parse the JSON payload and attach reason codes. Mirrors
    ercot_feeds.quality_errors; tests/test_ercot_stream.py keeps them aligned."""
    payload = F.col("value").cast("string")
    parsed = F.from_json(payload, ERCOT_SCHEMA)
    evaluated = raw.select(payload.alias("_payload"), parsed.alias("_parsed")).select(
        "_payload",
        (F.col("_parsed").isNull() | F.get_json_object("_payload", "$").isNull()).alias("_malformed"),
        "_parsed.*",
    )
    ok = ~F.col("_malformed")
    v = F.col("value")
    errors = F.concat(
        _error_if(F.col("_malformed"), "malformed_payload"),
        _error_if(ok & ~F.coalesce(F.col("feed").isin("demand", "fuel", "price"), F.lit(False)), "unknown_feed"),
        _error_if(ok & (F.col("series").isNull() | (F.col("series") == "")), "missing_series"),
        _error_if(ok & F.col("observed_at").isNull(), "missing_observed_at"),
        _error_if(ok & v.isNull(), "missing_value"),
        _error_if(ok & (F.col("feed") == "price") & v.isNotNull() & ~v.between(PRICE_MIN, PRICE_MAX),
                  "price_out_of_range"),
        _error_if(ok & (F.col("feed") == "demand") & v.isNotNull() & ((v <= 0) | (v > MW_MAX)),
                  "demand_out_of_range"),
        _error_if(ok & (F.col("feed") == "fuel") & v.isNotNull() & ~v.between(-MW_MAX, MW_MAX),
                  "generation_out_of_range"),
    )
    return evaluated.withColumn("quality_errors", errors)


def build_valid(evaluated):
    return (
        evaluated.filter(F.size("quality_errors") == 0)
        .drop("_payload", "_malformed", "quality_errors")
        .withColumn("market_key", F.coalesce(F.col("market"), F.lit("-")))
        .withColumn("obs_date", F.to_date("observed_at"))
    )


def build_rejected(evaluated):
    return (
        evaluated.filter(F.size("quality_errors") > 0)
        .withColumn("rejected_at", F.current_timestamp())
        .select(F.col("_payload").alias("payload"), "feed", "series", "observed_at",
                "quality_errors", "rejected_at")
    )


def latest_per_key(df):
    """Reduce a batch to one row per natural key: the newest source revision,
    ties broken by the most recent ingest. Every poll republishes the whole
    day, so a single micro-batch routinely holds many copies of one interval."""
    order = Window.partitionBy(*KEY).orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("ingested_at").desc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(order)).filter("_rn = 1").drop("_rn")


def upsert_batch(batch_df, batch_id: int, path: str = SILVER_PATH) -> None:
    """foreachBatch sink: newest revision wins, older replays are ignored."""
    from delta.tables import DeltaTable

    spark = batch_df.sparkSession
    latest = latest_per_key(batch_df)
    if not DeltaTable.isDeltaTable(spark, path):
        latest.write.format("delta").partitionBy("obs_date").save(path)
        return
    target = DeltaTable.forPath(spark, path)
    on = " AND ".join(f"t.{k} = s.{k}" for k in KEY)
    (
        target.alias("t")
        .merge(latest.alias("s"), on)
        .whenMatchedUpdateAll(
            condition="s.source_updated_at > t.source_updated_at OR t.source_updated_at IS NULL"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_bronze(raw):
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .writeStream.format("delta")
        .queryName("ercot-bronze")
        .outputMode("append")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/ercot_bronze")
        .trigger(processingTime="60 seconds")
        .start(f"{LAKE}/bronze/ercot_raw")
    )


def write_silver(valid):
    return (
        valid.writeStream.queryName("ercot-silver-upsert")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", f"{LAKE}/_checkpoints/ercot_silver")
        .trigger(processingTime="60 seconds")
        .start()
    )


def write_rejected(rejected):
    return (
        rejected.writeStream.format("delta")
        .queryName("ercot-quarantine")
        .outputMode("append")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/ercot_rejected")
        .trigger(processingTime="60 seconds")
        .start(f"{LAKE}/quarantine/ercot_rejected")
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop_event = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda s, f: stop_event.set())

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    queries = []
    try:
        raw = read_kafka(spark)
        evaluated = evaluate_quality(raw)
        queries = [
            write_bronze(raw),
            write_silver(build_valid(evaluated)),
            write_rejected(build_rejected(evaluated)),
        ]
        for q in queries:
            log.info("started query %s", q.name or q.id)
        await_shutdown(spark, stop_event)
    finally:
        stop_queries(queries)
        spark.stop()
        log.info("Spark stopped cleanly")


if __name__ == "__main__":
    main()
