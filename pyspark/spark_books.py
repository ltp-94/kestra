import os
import time
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, year, month
from pyspark.sql import functions as F


start = time.time()
spark = (
   SparkSession.builder \
   .appName("GCS Test")\
   .getOrCreate()
)

# --- 2. Paths ---
input_path = "/workspaces/kestra/data/Books.csv"
output_path = "/workspaces/kestra/data/books_output"

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

# Rename individual columns
df = (df
    .withColumnRenamed("Book-Title", "title")
    .withColumnRenamed("Book-Author", "author")
    .withColumnRenamed("Year-Of-Publication", "year")
    .withColumnRenamed("Publisher", "publisher")
    .withColumnRenamed("Image-URL-S", "image_url_small")
    .withColumnRenamed("Image-URL-M", "image_url_medium")
    .withColumnRenamed("Image-URL-L", "image_url_large")
)

df.show(5)




print(df.count())
print(df.select('ISBN').distinct().count())

for c in df.columns:
    null_counts = df.filter(df[c].isNull()).count()
    print(f'{c}: {null_counts}')


df = df.withColumn( "split_parts", F.split(F.col("title"), r'\";') )
print(df.count())
df.filter(F.size(F.col("split_parts")) > 1).show()



author_condition = (F.col("author").rlike(r'^\d{4}$'))

df = df.withColumn("publisher", F.when(author_condition, F.col("year")).otherwise(F.col("publisher"))) \
                    .withColumn("title", F.when(author_condition, F.col("split_parts").getItem(0)).otherwise(F.col("title"))) \
                    .withColumn("image_url_large", F.when(author_condition, F.col("image_url_medium")).otherwise(F.col("image_url_large"))) \
                    .withColumn("year", F.when(author_condition, F.col("author")).otherwise(F.col("year"))) \
                    .withColumn("author", F.when(author_condition, F.col("split_parts").getItem(1)).otherwise(F.col("author"))) \
                    
                     
                            
print(df.count())
df.filter(F.size(F.col("split_parts")) > 1).show()



# 1. Start with the dirty dataframe
# Create the 'title_clean' column once and we will keep updating IT.
# 2. Setup the fixes (Order is very important!)
# We put the 2-character sequences FIRST so they don't get broken by single-char fixes.
encoding_fixes = {
    # Catalan / Italian fixes
    "í²": "ò",
    "í¨": "è",
    "í¡": "à",
    
    # French / Double-encoded fixes
    "Ã\\?Â©": "é",
    "Ã\\?Â": "à", 
    "Ã\\?Â¨": "è",
    "Ã\\?Âª": "ê",
    "Ã\\?Â«": "ë",
    "Ã\\?Â´": "ô",
    "Ã\\?Â®": "î",
    "Ã\\?Â¯": "ï",
    "Ã\\?Â¹": "ù",
    "Ã\\?Â§": "ç",
    
    # Spanish fixes
    "Ã³": "ó",
    "Ã±": "ñ",
    "Ã¡": "á",
    "Ã©": "é",
    "Ã": "í"  # Keep this single character fix at the very bottom
}

# 3. Apply encoding fixes to the column 'title_clean'
for bad_str, good_str in encoding_fixes.items():
    df = df.withColumn(
        "title", 
        F.regexp_replace(F.col("title"), bad_str, good_str)
    )

# 4. Apply all other cleanups (Notice we continue using df_clean, NOT df_dirty)
df = df.withColumn(
    "title",
    # Step A: Remove backslashes and quotes
    F.translate(F.col("title"), '\\"', '')
).withColumn(
    "title",
    # Step B: Fix HTML entities
    F.regexp_replace(F.col("title"), "&amp;", "&")
).withColumn(
    "title",
    # Step C: Turn / into ,
    F.regexp_replace(F.col("title"), "/", ", ")
).withColumn(
    "title",
    # Step D: Collapse multiple spaces into one and trim
    F.trim(F.regexp_replace(F.col("title"), "\\s+", " "))
)


df = df.drop("split_parts")

df = df.withColumn("year", F.col("year").cast("int"))

df.printSchema()

df.write.mode("overwrite").csv(output_path, header=True)



# --- 5. Keep Spark UI alive ---
#print("Sleeping for 120 seconds so you can view the Spark UI at http://localhost:4040")

#time.sleep(120)
end = time.time()
print(f"Elapsed time: {end - start:.2f} seconds")
print(f'Spark version: {spark.version}')
# --- 6. Stop Spark ---
spark.stop()



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
