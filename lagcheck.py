from pyspark.sql import SparkSession
from pyspark.sql import functions as F

s = (SparkSession.builder.appName('lag')
     .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.2.0')
     .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
     .config('spark.sql.catalog.spark_catalog',
             'org.apache.spark.sql.delta.catalog.DeltaCatalog')
     .getOrCreate())
s.sparkContext.setLogLevel('ERROR')

d = s.read.format('delta').load('lake/silver/metar_observations')
late = d.filter('lag_seconds > 21600')

print('\nRecords with ingest lag over 6 hours:')
late.select('station_id', 'name', 'observed_at', 'ingested_at', 'lag_seconds') \
    .orderBy(F.desc('lag_seconds')).show(15, truncate=False)

print('count:', late.count(), 'of', d.count())

print('\nStations responsible:')
late.groupBy('station_id', 'name').count().orderBy(F.desc('count')).show(20, truncate=False)

s.stop()
