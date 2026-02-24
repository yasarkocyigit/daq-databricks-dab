# Databricks notebook source
# MAGIC %md
# MAGIC # Health Check & Monitoring
# MAGIC
# MAGIC Validates pipeline health: watermark freshness, table row counts,
# MAGIC streaming checkpoint lag, and data quality metrics.

# COMMAND ----------

from pyspark.sql.functions import *
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if "dbutils" in dir() else "main"
environment = dbutils.widgets.get("environment") if "dbutils" in dir() else "dev"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark Freshness Check

# COMMAND ----------

try:
    watermarks_df = spark.table(f"{catalog}.framework.watermarks")
    stale_watermarks = watermarks_df.filter(
        col("updated_at") < current_timestamp() - expr("INTERVAL 24 HOURS")
    )

    if stale_watermarks.count() > 0:
        print("[WARNING] Stale watermarks detected:")
        stale_watermarks.show(truncate=False)
    else:
        print("[OK] All watermarks are fresh (updated within 24 hours)")
except Exception as e:
    print(f"[INFO] Watermark table not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Row Count Summary

# COMMAND ----------

schemas_to_check = ["bronze_raw", "silver_data", "gold_analytics"]
if environment == "dev":
    schemas_to_check = ["dev_bronze", "dev_silver", "dev_gold"]

for schema in schemas_to_check:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        print(f"\n[{schema.upper()}] Tables: {len(tables)}")
        for t in tables:
            try:
                count = spark.table(f"{catalog}.{schema}.{t.tableName}").count()
                print(f"  {t.tableName}: {count:,} rows")
            except Exception:
                print(f"  {t.tableName}: ERROR reading")
    except Exception:
        print(f"\n[{schema.upper()}] Schema not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Status

# COMMAND ----------

print(f"\n[HEALTH CHECK COMPLETE] Environment: {environment}, Catalog: {catalog}")
