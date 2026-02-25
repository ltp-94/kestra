import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# # --- 1. PATH LOGIC (Environment Detection) ---
# current_dir = os.getcwd()
# current_script_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.dirname(current_script_dir)

# # Define the three possible locations for the GCP Key
# kestra_key_path = os.path.join(current_dir, "gcp-key.json")
# docker_key_path = "/opt/spark/conf/gcp-key.json"
# local_key_path = os.path.join(project_root, "service-account.json")

# if os.path.exists(kestra_key_path):
#     key_path = kestra_key_path
#     print(f"Environment: Kestra - Using {key_path}")
# elif os.path.exists(docker_key_path):
#     key_path = docker_key_path
#     print(f"Environment: Docker - Using {key_path}")
# elif os.path.exists(local_key_path):
#     key_path = local_key_path
#     print(f"Environment: Local/Codespace - Using {key_path}")
# else:
#     raise FileNotFoundError(f"Could not find JSON key at: \n1. {kestra_key_path}\n2. {docker_key_path}\n3. {local_key_path}")

key_path = os.getenv("GCP_KEY_PATH", "service-account.json")
print(f"--- Using GCP Key from: {key_path} ---")


# --- 2. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS Parquet Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    .getOrCreate())

# --- 3. PROCESSING ---
input_path = "gs://kestra-bucket-latypov/raw/Books.csv"
output_path = "yellow_taxi_output"

print(f"--- Processing: {input_path} ---")

# Read data from GCS
df = spark.read.parquet(input_path)

# Transformations
df_transformed = df.filter(col("fare_amount") > 10) \
    .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# Get sample for validation
df_top_10 = df_transformed.limit(10)
df_top_10.show()

print(f"--- Writing 10 rows to local folder: {output_path} ---")

# Write output (Kestra will pick this up via outputFiles)
df_top_10.repartition(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("--- Job Successful ---")

# Stop the session
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