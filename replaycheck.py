from pyspark.sql import SparkSession
from pyspark.sql import functions as F

s = (SparkSession.builder.appName('replay')
     .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.2.0')
     .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
     .config('spark.sql.catalog.spark_catalog',
             'org.apache.spark.sql.delta.catalog.DeltaCatalog')
     .getOrCreate())
s.sparkContext.setLogLevel('ERROR')

sv = s.read.format('delta').load('lake/silver/metar_observations')
gd = s.read.format('delta').load('lake/gold/metar_15min')

print('\nSilver rows in the 23:00-23:30 window on 08-13:')
print(sv.filter("observed_at >= '2026-08-13 23:00' and observed_at < '2026-08-13 23:30'").count())

print('\nGold 15-min windows covering that period:')
gd.filter("window_start >= '2026-08-13 23:00' and window_start < '2026-08-13 23:30'") \
  .select('window_start', 'station_id', 'observation_count').show(10, truncate=False)
print('gold rows in that period:',
      gd.filter("window_start >= '2026-08-13 23:00' and window_start < '2026-08-13 23:30'").count())

s.stop()
