from pyspark.sql import SparkSession
from pyspark.sql import functions as F

s = (SparkSession.builder.appName('geo')
     .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.2.0')
     .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
     .config('spark.sql.catalog.spark_catalog',
             'org.apache.spark.sql.delta.catalog.DeltaCatalog')
     .getOrCreate())
s.sparkContext.setLogLevel('ERROR')

d = s.read.format('delta').load('lake/silver/metar_observations')
st = d.select('station_id', 'name').distinct()

print('\nStations by ICAO prefix:')
st.withColumn('prefix', F.substring('station_id', 1, 1)) \
  .groupBy('prefix').count().orderBy(F.desc('count')).show(truncate=False)

print('\nSample of non-K stations:')
st.filter("station_id not like 'K%'").show(15, truncate=False)

s.stop()
