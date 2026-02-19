import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# --- 1. Spark Session ---
spark = (SparkSession.builder
    .appName("Transform")
    .getOrCreate())

# --- 2. Paths ---
input_path = "/workspaces/kestra/data/Books.csv"
output_path = "/workspaces/kestra/data/books_output"

print(f"--- Processing: {input_path} ---")

# --- 3. Read Data ---
# Use header=True so Spark picks up column names
df = spark.read.csv(input_path, header=True, inferSchema=True)

# --- 4. Explore ---
print("Schema:")
df.printSchema()

print("Sample rows:")
df.show(5)

# Rename individual columns
df_renamed = (df
    .withColumnRenamed("Book-Title", "title")
    .withColumnRenamed("Book-Author", "author")
    .withColumnRenamed("Year-Of-Publication", "year")
    .withColumnRenamed("Publisher", "publisher")
)

df_renamed.show(5)


# --- 5. Manipulations ---
# Example: select only a few columns
# books = df.select("Book-Title", "Book-Author", "Year-Of-Publication")

# # Filter: books published after 2000
# recent_books = books.filter(col("Year-Of-Publication") > 2000)

# # Group: count books per author
# author_counts = books.groupBy("Book-Author").count().orderBy(col("count").desc())

# # --- 6. Save Results ---
# recent_books.write.mode("overwrite").csv(output_path + "/recent_books", header=True)
# author_counts.write.mode("overwrite").csv(output_path + "/author_counts", header=True)

