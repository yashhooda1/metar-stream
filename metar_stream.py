"""
Spark Structured Streaming: METAR observations -> Delta medallion.

Bronze : raw Kafka payload, append-only, nothing thrown away.
Silver : parsed, typed, deduplicated within a watermark window.
Gold   : 15-minute windowed aggregates per station, plus a low-visibility alert
         stream.

The parts that matter for interviews, and why:

  maxOffsetsPerTrigger
      Backpressure. Without it, the first micro-batch after a long outage tries
      to eat the entire backlog in one go and the executors OOM.

  withWatermark("observed_at", "30 minutes")
      METAR arrives late constantly -- a station's uplink drops and it flushes
      an hour of observations at once. The watermark bounds how long Spark
      keeps state so it doesn't grow forever, at the cost of dropping anything
      later than the threshold. 30 min is a deliberate tradeoff, not a default.

  dropDuplicates(["station_id", "observed_at"])
      The API re-sends the same observation every poll. Combined with the
      watermark, state is bounded. Watermark + dedupe + checkpoint is what
      actually delivers effectively-exactly-once here.

  checkpointLocation
      Offsets and state live here. Delete it and you replay from scratch;
      keep it and a killed job resumes mid-stream. Every sink gets its own.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("METAR_TOPIC", "metar.raw")
LAKE = os.getenv("LAKE_PATH", "./lake")

METAR_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), False),
        StructField("observed_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), True),
        StructField("name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("elevation_m", DoubleType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("dewpoint_c", DoubleType(), True),
        StructField("wind_dir_deg", IntegerType(), True),
        StructField("wind_speed_kt", IntegerType(), True),
        StructField("wind_gust_kt", IntegerType(), True),
        StructField("visibility_sm", StringType(), True),  # "10+" shows up, so string
        StructField("altimeter_hpa", DoubleType(), True),
        StructField("flight_category", StringType(), True),
        StructField("raw_text", StringType(), True),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("metar-stream")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.2.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("failOnDataLoss", "false")
        .load()
    )


def write_bronze(raw):
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/bronze")
        .trigger(processingTime="30 seconds")
        .start(f"{LAKE}/bronze/metar_raw")
    )


def build_silver(raw):
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), METAR_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("station_id").isNotNull())
    )

    # "10+" is the API's way of saying "at least 10 statute miles".
    visibility = (
        F.when(F.col("visibility_sm").rlike(r"^\d+(\.\d+)?\+?$"),
               F.regexp_replace("visibility_sm", r"\+", "").cast("double"))
        .otherwise(F.lit(None).cast("double"))
    )

    return (
        parsed.withColumn("visibility_mi", visibility)
        .withColumn(
            "wind_gust_kt",
            F.coalesce(F.col("wind_gust_kt"), F.col("wind_speed_kt")),
        )
        .withColumn(
            "lag_seconds",
            F.col("ingested_at").cast("long") - F.col("observed_at").cast("long"),
        )
        .withColumn("obs_date", F.to_date("observed_at"))
        .withWatermark("observed_at", "30 minutes")
        .dropDuplicates(["station_id", "observed_at"])
    )


def write_silver(silver):
    return (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/silver")
        .partitionBy("obs_date")
        .trigger(processingTime="30 seconds")
        .start(f"{LAKE}/silver/metar_observations")
    )


def write_gold_windowed(silver):
    """15-minute tumbling aggregates per station. Stateful, watermark-bounded."""
    agg = (
        silver.groupBy(
            F.window("observed_at", "15 minutes"),
            F.col("station_id"),
        )
        .agg(
            F.avg("temp_c").alias("avg_temp_c"),
            F.max("wind_gust_kt").alias("peak_gust_kt"),
            F.min("visibility_mi").alias("min_visibility_mi"),
            F.count("*").alias("observation_count"),
            F.avg("lag_seconds").alias("avg_ingest_lag_s"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "station_id",
            "avg_temp_c",
            "peak_gust_kt",
            "min_visibility_mi",
            "observation_count",
            "avg_ingest_lag_s",
        )
    )

    return (
        agg.writeStream.format("delta")
        .outputMode("append")  # append works because the watermark closes windows
        .option("checkpointLocation", f"{LAKE}/_checkpoints/gold_windowed")
        .trigger(processingTime="60 seconds")
        .start(f"{LAKE}/gold/metar_15min")
    )


def write_gold_alerts(silver):
    """IFR/LIFR conditions -- the stream a dashboard or pager would subscribe to."""
    alerts = silver.filter(
        (F.col("flight_category").isin("IFR", "LIFR"))
        | (F.col("wind_gust_kt") >= 35)
    ).select(
        "station_id",
        "name",
        "observed_at",
        "flight_category",
        "wind_gust_kt",
        "visibility_mi",
        "raw_text",
    )

    return (
        alerts.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/gold_alerts")
        .trigger(processingTime="30 seconds")
        .start(f"{LAKE}/gold/metar_alerts")
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka(spark)
    silver = build_silver(raw)

    queries = [
        write_bronze(raw),
        write_silver(silver),
        write_gold_windowed(silver),
        write_gold_alerts(silver),
    ]

    for q in queries:
        print(f"started query: {q.name or q.id}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
