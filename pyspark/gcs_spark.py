import os  # <--- THIS WAS MISSING
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# 1. Determine which path to use for the credentials
# If running in your permanent Docker container, it uses /opt/spark/conf/
# If running in Kestra, it uses the current local directory
key_path = "/opt/spark/conf/gcp-key.json" if os.path.exists("/opt/spark/conf/gcp-key.json") else "gcp-key.json"

# 2. Start the session
spark = SparkSession.builder \
    .appName("GCS Parquet Transform") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path) \
    .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE") \
    .getOrCreate()

input_path = "gs://kestra-demo-latypov/yellow_tripdata_2024-01.parquet"
output_path = "gs://kestra-demo-latypov/yellow_taxi_top_10/"

print(f"--- Processing: {input_path} ---")

# 3. Read data
df = spark.read.parquet(input_path)

# 4. Transformations
df_transformed = df.filter(col("fare_amount") > 10) \
    .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

df_top_10 = df_transformed.limit(10)
print(df_top_10.show())

print(f"--- Writing 10 rows to: {output_path} ---")

#output_name = "yellow_taxi_top_10_output"

# Save to the current working directory
# Kestra will pick this up if you use outputFiles in the YAML
# Use a simple relative path
output_dir = "output_data"

df_top_10.repartition(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_dir)

print(f"Checking if directory exists: {os.path.exists(output_dir)}")


spark.stop()






# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, year, month

# # Start the session
# spark = SparkSession.builder.appName("GCS Parquet Transform").getOrCreate()

# input_path = "gs://kestra-demo-latypov/yellow_tripdata_2024-01.parquet"
# output_path = "gs://kestra-demo-latypov/yellow_taxi_top_10/"

# print(f"--- Processing: {input_path} ---")

# # 1. Read data
# df = spark.read.parquet(input_path)

# # 2. Transformations
# df_transformed = df.filter(col("fare_amount") > 10) \
#     .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
#     .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# # 3. GET THE FIRST 10 ROWS ONLY
# df_top_10 = df_transformed.limit(10)
# print(df_top_10.show())
# # 4. Write to GCS
# # repartition(1) makes sure we get only 1 file in the folder
# print(f"--- Writing 10 rows to: {output_path} ---")

# df_top_10.repartition(1).write \
#     .mode("overwrite") \
#     .parquet(output_path)

# print("--- Write Successful ---")

spark.stop()