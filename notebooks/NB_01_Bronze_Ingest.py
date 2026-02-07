# ============================================================
# LANDING -> BRONZE
# Purpose: Ingest raw CSV from Landing into Bronze as Delta
#          - No business transformations
#          - Add lineage columns
#          - Partition by month_key
# ============================================================

from pyspark.sql import functions as F
import re

# -----------------------------
# Parameters (injected by Fabric Pipeline)
# -----------------------------
# Expected: file_name like "coffee_shop_sales_2024_01.csv"
# Expected: date_processed like "2026-01-11"
if not file_name:
    raise ValueError("file_name is missing from pipeline parameters")
if not date_processed:
    raise ValueError("date_processed is missing from pipeline parameters")

print("file_name:", file_name)
print("date_processed:", date_processed)

# -----------------------------
# Paths
# -----------------------------
landing_base_path = (
    "abfss://Coffee_Shop@onelake.dfs.fabric.microsoft.com/"
    "Coffee_shop_LH.Lakehouse/Files/landing"
)
landing_file_path = f"{landing_base_path}/{file_name}"
print("landing_file_path:", landing_file_path)

# -----------------------------
# month_key from filename (YYYY-MM)
# -----------------------------
m = re.search(r"(\d{4})_(\d{2})", file_name)
if not m:
    raise ValueError(f"Cannot derive month_key from file_name: {file_name}")
month_key = f"{m.group(1)}-{m.group(2)}"
print("month_key:", month_key)

# -----------------------------
# Create Bronze schema
# -----------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# -----------------------------
# Read Landing (CSV)
# -----------------------------
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .load(landing_file_path)
)

# Remove accidental extra columns like _c0
df_raw = df_raw.drop(*[c for c in df_raw.columns if c.startswith("_c")])

# -----------------------------
# Cast + Select + Add Lineage
# -----------------------------
df_bronze = df_raw.select(
    F.col("transaction_id").cast("string").alias("transaction_
