# ============================================================
# Project 08 | SILVER -> GOLD
# Purpose: Build star schema for Power BI
#          - Dimension tables (overwrite)
#          - Fact table month-based incremental overwrite
# ============================================================

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

df = spark.table("silver.coffee_sales")
if df.rdd.isEmpty():
    raise ValueError("silver.coffee_sales is empty. Cannot build Gold.")

# If needed, rename transaction_type -> payment_method
if "payment_method" not in df.columns and "transaction_type" in df.columns:
    df = df.withColumnRenamed("transaction_type", "payment_method")

df = df.withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))

# -----------------------------
# Process latest month_key (incremental)
# -----------------------------
if "month_key" not in df.columns:
    raise ValueError("month_key not found in silver.coffee_sales. Gold expects month_key.")

month_key = spark.sql("SELECT MAX(month_key) AS mk FROM silver.coffee_sales").collect()[0]["mk"]
if month_key is None:
    raise ValueError("No month_key found in Silver.")

print("Building GOLD for month_key:", month_key)

df_batch = df.filter(F.col("month_key") == F.lit(month_key))
if df_batch.rdd.isEmpty():
    raise ValueError(f"No Silver data found for month_key={month_key}")

# ============================================================
# DIMENSIONS (overwrite is fine - small tables)
# ============================================================

dim_store = (
    df.select("store_id", "store_location")
    .dropDuplicates(["store_id"])
)

dim_product = (
    df.select("product_id", "product_category", "product_type", "product_detail")
    .dropDuplicates(["product_id"])
)

dim_date = (
    df.select(
        "transaction_date",
        "day_name",
        "month_name",
        "month_number",
        F.weekofyear("transaction_date").alias("week_of_year")
    )
    .dropDuplicates(["transaction_date"])
)

dim_time = (
    df.select("transaction_hour", "time_bucket")
    .dropDuplicates(["transaction_hour"])
    .withColumn("hour_label", F.format_string("%02d:00", F.col("transaction_hour")))
    .select("transaction_hour", "hour_label", "time_bucket")
)

dim_payment_method = (
    df.select("payment_method")
    .dropna(subset=["payment_method"])
    .dropDuplicates(["payment_method"])
)

# Write dimensions
dim_store.write.format("delta").mode("overwrite").saveAsTable("gold.dim_store")
dim_product.write.format("delta").mode("overwrite").saveAsTable("gold.dim_product")
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")
dim_time.write.format("delta").mode("overwrite").saveAsTable("gold.dim_time")
dim_payment_method.write.format("delta").mode("overwrite").saveAsTable("gold.dim_payment_method")

print("Gold dimensions written.")

# ============================================================
# FACT TABLE (overwrite ONLY this month_key)
# ============================================================

fact_cols = [
    "month_key",
    "transaction_id",
    "transaction_datetime",
    "transaction_date",
    "transaction_hour",
    "store_id",
    "product_id",
    "payment_method",
    "transaction_qty",
    F.round(F.col("unit_price"), 2).alias("unit_price"),
    F.round(F.col("total_amount"), 2).alias("total_amount"),
]

# add lineage cols if present
if "date_processed" in df_batch.columns:
    fact_cols.append("date_processed")
if "source_file_name" in df_batch.columns:
    fact_cols.append("source_file_name")

fact_sales = df_batch.select(*fact_cols).dropDuplicates(
    ["transaction_id", "product_id", "store_id", "transaction_datetime"]
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if not spark.catalog.tableExists("gold.fact_sales"):
    (
        fact_sales.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("month_key")
        .saveAsTable("gold.fact_sales")
    )
    print("Created gold.fact_sales")
else:
    (
        fact_sales.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"month_key = '{month_key}'")
        .saveAsTable("gold.fact_sales")
    )
    print("Overwrote gold.fact_sales for month_key:", month_key)

# Quick validation
spark.sql("""
SELECT month_key, COUNT(*) AS cnt
FROM gold.fact_sales
GROUP BY month_key
ORDER BY month_key
""").show(truncate=False)
