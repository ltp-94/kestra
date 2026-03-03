import os
import time
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, year, month
from pyspark.sql import functions as F


start = time.time()
spark = (
   SparkSession.builder \
   .appName("Users Transformation")\
   .getOrCreate()
)

# --- 2. Paths ---
input_path = "/workspaces/kestra/data/Users.csv"
output_path = "/workspaces/kestra/data/users_output"

print(f"--- Processing: {input_path} ---")

# --- 3. Read Data ---
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

spark.stop()