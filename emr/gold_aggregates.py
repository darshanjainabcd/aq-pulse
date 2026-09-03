"""EMR PySpark job: Silver -> Gold daily city aggregates."""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
parser = argparse.ArgumentParser()
parser.add_argument("--source", required=True)
parser.add_argument("--target", required=True)
args = parser.parse_args()
spark = SparkSession.builder.appName("aq-pulse-gold").getOrCreate()
df = spark.read.parquet(args.source)
agg = (
  df.groupBy("city_id", "event_date")
  .agg(
    F.avg("pm2_5").alias("avg_pm2_5"),
    F.max("pm2_5").alias("max_pm2_5"),
    F.avg("pm10").alias("avg_pm10"),
    F.max("pm10").alias("max_pm10"),
    F.avg("nitrogen_dioxide").alias("avg_no2"),
    F.avg("ozone").alias("avg_o3"),
    F.avg("european_aqi").alias("avg_aqi"),
    F.max("european_aqi").alias("max_aqi"),
    F.sum(F.when(F.col("european_aqi") >= 60, 1).otherwise(0)).alias("poor_air_hours"),
    F.count(F.lit(1)).alias("record_count"),
  )
)
agg.write.mode("overwrite").partitionBy("event_date").parquet(args.target)
spark.stop()
