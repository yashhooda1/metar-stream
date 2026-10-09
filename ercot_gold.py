"""
ERCOT gold layer: batch job over silver, plus the weather-vs-load join with
METAR silver. Run it on a timer next to the stream (every 15 minutes is plenty):

    python ercot_gold.py

Writes, overwriting each run (gold is fully derived, so overwrite is safe):

    gold/ercot_grid_15min      demand, capacity, margin, generation by fuel,
                               net load, wind+solar share
    gold/ercot_prices_hourly   real-time vs day-ahead per settlement point,
                               with the DA/RT spread
    gold/ercot_alerts          price spikes and thin capacity margins
    gold/ercot_weather_load    hourly demand joined to the population-weighted
                               temperature of six ERCOT airports from METAR

and docs/ercot_pipeline.json, a snapshot for the dashboard.
"""

import json
import os
import pathlib
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ercot_feeds import ERCOT_STATIONS, fit_degree_day_model

LAKE = os.getenv("LAKE_PATH", "./lake")
OUT = pathlib.Path("docs/ercot_pipeline.json")

PRICE_ALERT_USD = float(os.getenv("ERCOT_PRICE_ALERT", "500"))
THIN_MARGIN_PCT = float(os.getenv("ERCOT_THIN_MARGIN_PCT", "8"))

FUEL_COLUMNS = {
    "Natural Gas": "gas_mw",
    "Wind": "wind_mw",
    "Solar": "solar_mw",
    "Nuclear": "nuclear_mw",
    "Coal and Lignite": "coal_mw",
    "Power Storage": "storage_mw",
    "Hydro": "hydro_mw",
    "Other": "other_mw",
}


def build_grid_15min(silver):
    """One row per 15-minute window. Pivoting is a batch operation, which is
    one reason gold is batch rather than streaming."""
    w = F.window("observed_at", "15 minutes")
    demand = (
        silver.filter((F.col("feed") == "demand") & (F.col("series") == "system"))
        .groupBy(w.alias("w"))
        .agg(F.avg("value").alias("demand_mw"), F.avg("capacity_mw").alias("capacity_mw"))
    )
    fuel = (
        silver.filter(F.col("feed") == "fuel")
        .groupBy(w.alias("w"))
        .pivot("series", list(FUEL_COLUMNS))
        .agg(F.avg("value"))
    )
    for src, dst in FUEL_COLUMNS.items():
        fuel = fuel.withColumnRenamed(src, dst)

    gen_cols = [F.coalesce(F.col(c), F.lit(0.0)) for c in FUEL_COLUMNS.values()]
    total_gen = gen_cols[0]
    for c in gen_cols[1:]:
        total_gen = total_gen + c
    renewables = F.coalesce(F.col("wind_mw"), F.lit(0.0)) + F.coalesce(F.col("solar_mw"), F.lit(0.0))

    return (
        demand.join(fuel, "w", "full_outer")
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "demand_mw", "capacity_mw", *FUEL_COLUMNS.values(),
        )
        .withColumn("total_generation_mw", total_gen)
        .withColumn("margin_mw", F.col("capacity_mw") - F.col("demand_mw"))
        .withColumn("margin_pct", F.round(100 * F.col("margin_mw") / F.col("demand_mw"), 2))
        # Net load is what dispatchable plants must cover once wind and solar
        # are taken out. It, not raw demand, drives evening price spikes.
        .withColumn("net_load_mw", F.col("demand_mw") - renewables)
        .withColumn(
            "wind_solar_share_pct",
            F.when(total_gen > 0, F.round(100 * renewables / total_gen, 2)),
        )
        .orderBy("window_start")
    )


def build_prices_hourly(silver):
    """Real-time 15-minute prices averaged to the hour beside the day-ahead
    price for the same hour. Positive spread = real time cleared above DA."""
    price = silver.filter(F.col("feed") == "price")
    hour_start = F.date_trunc("hour", F.col("observed_at") - F.expr("INTERVAL 1 SECOND"))
    # RT intervals and DA hours are stamped at interval END, so 14:15, 14:30,
    # 14:45 and 15:00 all belong to the 14:00-15:00 hour.
    rt = (
        price.filter(F.col("market") == "RT")
        .groupBy("series", hour_start.alias("hour_start"))
        .agg(F.avg("value").alias("rt_usd_mwh"), F.max("value").alias("rt_max_usd_mwh"),
             F.count("*").alias("rt_intervals"))
    )
    da = (
        price.filter(F.col("market") == "DAM")
        .select("series", hour_start.alias("hour_start"), F.col("value").alias("da_usd_mwh"))
    )
    return (
        rt.join(da, ["series", "hour_start"], "full_outer")
        .withColumnRenamed("series", "settlement_point")
        .withColumn("rt_minus_da", F.round(F.col("rt_usd_mwh") - F.col("da_usd_mwh"), 2))
        .orderBy("hour_start", "settlement_point")
    )


def build_alerts(silver, grid):
    spikes = silver.filter(
        (F.col("feed") == "price") & (F.col("market") == "RT") & (F.col("value") >= PRICE_ALERT_USD)
    ).select(
        F.lit("price_spike").alias("kind"),
        F.col("observed_at").alias("at"),
        F.col("series").alias("subject"),
        F.col("value").alias("value"),
        F.lit("USD/MWh").alias("unit"),
    )
    thin = grid.filter(F.col("margin_pct") < THIN_MARGIN_PCT).select(
        F.lit("thin_margin").alias("kind"),
        F.col("window_start").alias("at"),
        F.lit("system").alias("subject"),
        F.col("margin_pct").alias("value"),
        F.lit("% of demand").alias("unit"),
    )
    return spikes.unionByName(thin).orderBy(F.desc("at"))


def build_weather_load(silver, metar_silver):
    """Hourly ERCOT demand beside the population-weighted temperature of the
    ERCOT airports in METAR silver. Weights are renormalised per hour over the
    stations that actually reported."""
    spark = silver.sparkSession
    weights = spark.createDataFrame(
        [(s, float(w)) for s, (_, w) in ERCOT_STATIONS.items()], ["station_id", "weight"]
    )
    temps = (
        metar_silver.filter(F.col("temp_c").isNotNull())
        .join(weights, "station_id")
        # Routine METARs are issued around :51-:55, so round to the NEAREST
        # hour: a 14:53 report describes 15:00 better than 14:00.
        .withColumn("hour_start", F.date_trunc("hour", F.col("observed_at") + F.expr("INTERVAL 30 MINUTES")))
        .groupBy("hour_start", "station_id", "weight")
        .agg(F.avg(F.col("temp_c") * 9 / 5 + 32).alias("temp_f"))
        .groupBy("hour_start")
        .agg(
            (F.sum(F.col("temp_f") * F.col("weight")) / F.sum("weight")).alias("temp_f"),
            F.count("*").alias("stations_reporting"),
        )
    )
    demand = (
        silver.filter((F.col("feed") == "demand") & (F.col("series") == "system"))
        .withColumn("hour_start", F.date_trunc("hour", "observed_at"))
        .groupBy("hour_start")
        .agg(F.avg("value").alias("demand_mw"), F.count("*").alias("demand_intervals"))
    )
    return (
        demand.join(temps, "hour_start", "inner")
        .withColumn("temp_f", F.round("temp_f", 2))
        .withColumn("demand_mw", F.round("demand_mw", 1))
        .orderBy("hour_start")
    )


def build_spark() -> SparkSession:
    s = (
        SparkSession.builder.appName("ercot-gold")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Timestamps are UTC end to end; do not let the host zone leak into obs_date or hour buckets.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def main() -> None:
    spark = build_spark()
    silver = spark.read.format("delta").load(f"{LAKE}/silver/ercot_observations")
    grid = build_grid_15min(silver).cache()
    prices = build_prices_hourly(silver)
    alerts = build_alerts(silver, grid)

    grid.write.format("delta").mode("overwrite").save(f"{LAKE}/gold/ercot_grid_15min")
    prices.write.format("delta").mode("overwrite").save(f"{LAKE}/gold/ercot_prices_hourly")
    alerts.write.format("delta").mode("overwrite").save(f"{LAKE}/gold/ercot_alerts")

    model = None
    metar_path = f"{LAKE}/silver/metar_observations"
    try:
        metar = spark.read.format("delta").load(metar_path)
        wl = build_weather_load(silver, metar)
        wl.write.format("delta").mode("overwrite").save(f"{LAKE}/gold/ercot_weather_load")
        model = fit_degree_day_model((r["temp_f"], r["demand_mw"]) for r in wl.collect())
    except Exception as exc:  # METAR job not running on this host: skip, do not fail gold
        print(f"weather join skipped: {exc}")

    latest = grid.filter(F.col("demand_mw").isNotNull()).orderBy(F.desc("window_start")).limit(1).collect()
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest": {k: _iso(v) for k, v in latest[0].asDict().items()} if latest else None,
        "rows": {
            "silver": silver.count(),
            "grid_15min": grid.count(),
            "alerts": alerts.count(),
        },
        "weather_model": model,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
