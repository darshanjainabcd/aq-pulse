"""AWS Glue PySpark job: Bronze NDJSON -> validated/deduplicated Silver Parquet."""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE", "TARGET", "REJECTED"])
spark = SparkSession.builder.getOrCreate()
schema = T.StructType([
	T.StructField("city_id", T.StringType(), False),
	T.StructField("city", T.StringType(), True),
	T.StructField("country", T.StringType(), True),
	T.StructField("latitude", T.DoubleType(), True),
	T.StructField("longitude", T.DoubleType(), True),
	T.StructField("timezone", T.StringType(), True),
	T.StructField("event_ts", T.StringType(), False),
	T.StructField("pm10", T.DoubleType(), True),
	T.StructField("pm2_5", T.DoubleType(), True),
	T.StructField("carbon_monoxide", T.DoubleType(), True),
	T.StructField("nitrogen_dioxide", T.DoubleType(), True),
	T.StructField("sulphur_dioxide", T.DoubleType(), True),
	T.StructField("ozone", T.DoubleType(), True),
	T.StructField("european_aqi", T.DoubleType(), True),
	T.StructField("record_key", T.StringType(), True),
	T.StructField("source_ingest_ts", T.StringType(), True),
])
df = spark.read.schema(schema).json(args["SOURCE"])
df = (
	df.withColumn("event_timestamp", F.to_timestamp("event_ts"))
	.withColumn("ingest_timestamp", F.to_timestamp("source_ingest_ts"))
	.withColumn("event_date", F.to_date("event_timestamp"))
	.withColumn("record_key", F.coalesce("record_key", F.concat_ws("|", "city_id", "event_ts")))
)

invalid = (
	F.col("city_id").isNull()
	| F.col("event_timestamp").isNull()
	| ~F.col("latitude").between(-90, 90)
	| ~F.col("longitude").between(-180, 180)
	| (F.coalesce(F.col("pm10"), F.lit(0.0)) &lt; 0)
	| (F.coalesce(F.col("pm2_5"), F.lit(0.0)) &lt; 0)
)

rejected = df.filter(invalid).withColumn("reject_reason", F.lit("schema_or_range_validation"))
valid = df.filter(~invalid)
window = Window.partitionBy("record_key").orderBy(F.col("ingest_timestamp").desc_nulls_last())
valid = valid.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")
columns = [
	"city_id", "city", "country", "latitude", "longitude", "timezone",
	"event_timestamp", "event_date", "pm10", "pm2_5", "carbon_monoxide",
	"nitrogen_dioxide", "sulphur_dioxide", "ozone", "european_aqi",
	"record_key", "ingest_timestamp",
]
(
	valid.select(*columns)
	.repartition("event_date")
	.write.mode("overwrite")
	.partitionBy("event_date")
	.parquet(args["TARGET"])
)
if not rejected.rdd.isEmpty():
	rejected.write.mode("append").parquet(args["REJECTED"])



