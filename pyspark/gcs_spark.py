import os
import time  # <--- Added this
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- 1. PATH LOGIC ---
start = time.time()
# Kestra passes the path to the key via this env var
key_path = os.getenv("GCP_KEY_PATH", "gcp-key.json")
print(f"--- Using GCP Key from: {key_path} ---")

# --- 2. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS CSV Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .getOrCreate())

# --- 3. PROCESSING ---
input_path = "gs://kestra-bucket-latypov/raw/Books.csv"
# Save to 'spark_output' so Kestra captures it
output_path = "spark_output/processed_books" 

print(f"--- Reading CSV: {input_path} ---")

# --- 4. Read Data (Crucial fix: .csv instead of .parquet) ---
df = spark.read.csv(input_path, header=True, inferSchema=True)

# --- 5. Transform ---
df_renamed = (df
    .withColumnRenamed("Book-Title", "title")
    .withColumnRenamed("Book-Author", "author")
    .withColumnRenamed("Year-Of-Publication", "year")
    .withColumnRenamed("Publisher", "publisher")
)

# Optional: Write the result back to GCS or locally
print(f"--- Saving result to: {output_path} ---")
df_renamed.write.mode("overwrite").csv(output_path)

df_renamed.show(5)

end = time.time()
print(f"Elapsed time: {end - start:.2f} seconds")
print("--- Job Successful ---")

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