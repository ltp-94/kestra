# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, year, month

# # Start the session
# spark = SparkSession.builder.appName("GCS Parquet Transform").getOrCreate()

# input_path = "gs://kestra-demo-latypov/yellow_tripdata_2024-01.parquet"
# print(f"--- Processing: {input_path} ---")

# # Read data
# df = spark.read.parquet(input_path)

# # Transformations
# df_transformed = df.filter(col("fare_amount") > 10) \
#     .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
#     .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# df_transformed.show(5)

# df_transformed.write

# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Start the session
spark = SparkSession.builder.appName("GCS Parquet Transform").getOrCreate()

input_path = "gs://kestra-demo-latypov/yellow_tripdata_2024-01.parquet"
output_path = "gs://kestra-demo-latypov/yellow_taxi_top_10/"

print(f"--- Processing: {input_path} ---")

# 1. Read data
df = spark.read.parquet(input_path)

# 2. Transformations
df_transformed = df.filter(col("fare_amount") > 10) \
    .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# 3. GET THE FIRST 10 ROWS ONLY
df_top_10 = df_transformed.limit(10)

# 4. Write to GCS
# repartition(1) makes sure we get only 1 file in the folder
print(f"--- Writing 10 rows to: {output_path} ---")

df_top_10.repartition(1).write \
    .mode("overwrite") \
    .parquet(output_path)

print("--- Write Successful ---")

spark.stop()