from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Start the session
spark = SparkSession.builder.appName("GCS Parquet Transform").getOrCreate()

input_path = "gs://kestra-demo-latypov/yellow_tripdata_2024-01.parquet"
print(f"--- Processing: {input_path} ---")

# Read data
df = spark.read.parquet(input_path)

# Transformations
df_transformed = df.filter(col("fare_amount") > 10) \
    .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

df_transformed.show(5)

spark.stop()