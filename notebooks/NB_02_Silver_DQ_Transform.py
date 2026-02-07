# ============================================================
# Project 08 | BRONZE -> SILVER
# Part 1: Data Quality Checks (for the month batch)
# Part 2: Cleaning + Transformations (analytics-ready)
# Part 3: Final validations + write rerun-safe
# ============================================================

from pyspark.sql import functions as F
import re

# -----------------------------
# Parameters (pipeline)
# -----------------------------
if not file_name:
    raise ValueError("Missing parameter: file_name. Check pipeline base parameters key is 'file_name'.")
if not date_processed:
    raise ValueError("Missing parameter: date_processed. Check pipeline base parameters key is 'date_processed'.")

m = re.search(r"(\d{4})_(\d{2})", file_name)
if not m:
    raise ValueError(f"Cannot derive month_key from file_name: {file_name}")
month_key = f"{m.group(1)}-{m.group(2)}"

print("file_name:", file_name)
print("date_processed:", date_processed)
print("Processing month_key:", month_key)

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

source_table = "bronze.coffee_sales"
target_table = "silver.coffee_sales"

df0 = spark.table(source_table).filter(F.col("month_key") == F.lit(month_key))
if df0.rdd.isEmpty():
    raise ValueError(f"No Bronze data found for month_key={month_key}")

# ============================================================
# PART 1: INITIAL DQ CHECKS (BRONZE for this month)
# ============================================================
print("=== INITIAL DATA QUALITY CHECKS (BRONZE for this month) ===")

total_rows = df0.count()
print("Total rows (this month):", total_rows)

dup_txn_cnt = (
    df0.groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print("Duplicate transaction_id (number of IDs repeating):", dup_txn_cnt)

full_dup_rows = total_rows - df0.dropDuplicates().count()
print("Full-row duplicate rows:", full_dup_rows)

critical_cols = ["transaction_id", "store_id", "product_id", "transaction_datetime"]
for c in critical_cols:
    null_count = df0.filter(F.col(c).isNull()).count()
    print(f"Null count in {c}:", null_count)

invalid_qty = df0.filter((F.col("transaction_qty").isNull()) | (F.col("transaction_qty") <= 0)).count()
invalid_price = df0.filter((F.col("unit_price").isNull()) | (F.col("unit_price") <= 0)).count()
print("Invalid transaction_qty rows (null or <= 0):", invalid_qty)
print("Invalid unit_price rows (null or <= 0):", invalid_price)

# store_id -> multiple locations check
store_loc_cnt = (
    df0.groupBy("store_id")
    .agg(F.countDistinct("store_location").alias("location_count"))
    .filter(F.col("location_count") > 1)
    .count()
)
print("store_id mapped to >1 store_location:", store_loc_cnt)

# product_id -> inconsistent attributes check
prod_attr_cnt = (
    df0.groupBy("product_id")
    .agg(
        F.countDistinct("product_category").alias("category_cnt"),
        F.countDistinct("product_type").alias("type_cnt"),
        F.countDistinct("product_detail").alias("detail_cnt")
    )
    .filter((F.col("category_cnt") > 1) | (F.col("type_cnt") > 1) | (F.col("detail_cnt") > 1))
    .count()
)
print("product_id mapped to inconsistent attributes:", prod_attr_cnt)

# ============================================================
# PART 2: CLEANING + TRANSFORMATIONS
# ============================================================
df_clean = (
    df0
    .dropna(subset=["transaction_id", "store_id", "product_id", "transaction_datetime"])
    .filter((F.col("transaction_qty") > 0) & (F.col("unit_price") > 0))
    .withColumn("store_location", F.upper(F.trim(F.col("store_location"))))
    .withColumn("product_category", F.upper(F.trim(F.col("product_category"))))
    .withColumn("product_type", F.upper(F.trim(F.col("product_type"))))
    .withColumn("product_detail", F.trim(F.col("product_detail")))
    .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type"))))
    .dropDuplicates(["transaction_id", "product_id", "transaction_datetime", "store_id"])
)

df_silver = (
    df_clean
    .withColumn("total_amount", F.round(F.col("transaction_qty") * F.col("unit_price"), 2))
    .withColumn("transaction_date", F.to_date(F.col("transaction_datetime")))
    .withColumn("transaction_time", F.date_format(F.col("transaction_datetime"), "HH:mm:ss"))
    .withColumn("transaction_hour", F.hour(F.col("transaction_datetime")))
    .withColumn("day_name", F.date_format(F.col("transaction_datetime"), "EEEE"))
    .withColumn("month_name", F.date_format(F.col("transaction_datetime"), "MMMM"))
    .withColumn("month_number", F.month(F.col("transaction_datetime")))
    .withColumn(
        "time_bucket",
        F.when(F.col("transaction_hour").between(5, 11), "Morning")
         .when(F.col("transaction_hour").between(12, 16), "Afternoon")
         .when(F.col("transaction_hour").between(17, 21), "Evening")
         .otherwise("Night")
    )
    .withColumn("date_processed", F.lit(date_processed))
)

print("Final rows (this month):", df_silver.count())

# ============================================================
# PART 3: FINAL DQ CHECKS (SILVER)
# ============================================================
print("=== FINAL DATA QUALITY CHECKS (SILVER) ===")

# critical nulls
for c in ["transaction_id", "store_id", "product_id", "transaction_datetime", "month_key"]:
    print(f"Null count in {c}:", df_silver.filter(F.col(c).isNull()).count())

# invalid measures
print("Invalid transaction_qty (<=0):", df_silver.filter(F.col("transaction_qty") <= 0).count())
print("Invalid unit_price (<=0):", df_silver.filter(F.col("unit_price") <= 0).count())
print("Invalid total_amount (<=0):", df_silver.filter(F.col("total_amount") <= 0).count())

# duplicates on business grain
dup_cnt = (
    df_silver.groupBy("transaction_id", "product_id", "transaction_datetime", "store_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print("Duplicate rows on grain:", dup_cnt)

if dup_cnt > 0:
    raise ValueError("SILVER DQ FAILED: duplicates found on business grain.")

# ============================================================
# WRITE SILVER (rerun-safe overwrite ONLY this month_key)
# ============================================================
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if not spark.catalog.tableExists(target_table):
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("month_key")
        .saveAsTable(target_table)
    )
    print("Created silver table:", target_table, "| partitionBy month_key")
else:
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"month_key = '{month_key}'")
        .saveAsTable(target_table)
    )
    print("Overwrote month partition in silver:", month_key)

spark.sql(f"""
SELECT month_key, COUNT(*) AS cnt
FROM {target_table}
GROUP BY month_key
ORDER BY month_key
""").show()
