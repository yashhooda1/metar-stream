from pyspark.sql import SparkSession
LAKE = "/home/yashh/metar-stream/lake"
s = (SparkSession.builder
     .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .enableHiveSupport().getOrCreate())
s.sql("CREATE DATABASE IF NOT EXISTS metar_silver")
s.sql(f"CREATE TABLE IF NOT EXISTS metar_silver.observations "
      f"USING DELTA LOCATION '{LAKE}/silver/metar_observations'")
s.sql("CREATE DATABASE IF NOT EXISTS metar_gold_spark")
s.sql(f"CREATE TABLE IF NOT EXISTS metar_gold_spark.metar_15min "
      f"USING DELTA LOCATION '{LAKE}/gold/metar_15min'")
s.sql("SELECT COUNT(*) AS n FROM metar_silver.observations").show()
