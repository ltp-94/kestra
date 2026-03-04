import os
import time  # <--- Added this
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
input_path = "gs://kestra-bucket-latypov/raw/Users.csv"
# Save to 'spark_output' so Kestra captures it
output_path = "gs://kestra-bucket-latypov/transformed_users" 


df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .option("multiLine", True)
         .option("quote", '"')             # Standard double quote
         .option("escape", '"')            # In many CSVs, "" is the escape for "
         .csv(input_path)
)


# --- 4. Explore ---
print("Schema:")
df.printSchema()

print("Sample rows:")
df.show(5)

df = df.withColumn("age", F.col("age").cast("int"))

df = df.withColumnRenamed('User-ID', 'user_id') \
        .withColumnRenamed('Location', 'location') \
        .withColumnRenamed('Age', 'age')

df = df.withColumn('split_parts', F.split(F.col("location"), ", "))


missing_percantage = {}
columns = df.columns
for colName in columns:
    missing_percantage[colName] = 100 * df.filter(F.col(colName).isNull()).count() / df.count()
    print(f'{colName}: {int(missing_percantage[colName])}')




exceptions = [
    "ny",
    "nyc",
    "la",
    "dc",
    "sf",
    "usa",
    "uk",
    "uae",
    "eu",
    "u.a.e"
]

def split_location(df, colName, idx):
    return df.withColumn(
        colName,
        F.when(
            (F.get(F.col("split_parts"), idx).isNull()) |
            (F.get(F.col("split_parts"), idx) == "") |
            (F.get(F.col("split_parts"), idx) == "n/a") |
            (F.get(F.col("split_parts"), idx) == ","),
            F.lit("Unknown")
        ).otherwise(
            F.when(
                F.get(F.col("split_parts"), idx).isin(exceptions),
                F.upper(F.get(F.col("split_parts"), idx))
            ).otherwise(
                # Handle hyphens and slashes properly
                    F.initcap(
                        F.regexp_replace(F.get(F.col("split_parts"), idx), "[-/]", " ")
                    )
                )
            )
        )
    


df = split_location(df, "city", 0)
df = split_location(df, "region", 1)
df = split_location(df, "country", 2)




df = df.drop(F.col("split_parts"))

df.show()


df.write.mode("overwrite") \
    .option("header", "true") \
    .option("quote", '"') \
    .option("quoteAll", "true") \
    .option("escape", '"') \
    .csv(output_path)


spark.stop()