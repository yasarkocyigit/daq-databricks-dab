# Databricks notebook source
# MAGIC %md
# MAGIC # Health Check & Monitoring
# MAGIC
# MAGIC Validates pipeline health: control-table freshness and row count summaries.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, expr

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if "dbutils" in dir() else "main"
environment = dbutils.widgets.get("environment") if "dbutils" in dir() else "dev"
bronze_raw_schema = (
    dbutils.widgets.get("bronze_raw_schema") if "dbutils" in dir() else "bronze_raw"
)
silver_schema = (
    dbutils.widgets.get("silver_schema") if "dbutils" in dir() else "silver_data"
)
gold_schema = (
    dbutils.widgets.get("gold_schema") if "dbutils" in dir() else "gold_analytics"
)
control_schema = dbutils.widgets.get("control_schema") if "dbutils" in dir() else "control"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark Freshness Check

# COMMAND ----------

watermark_table = f"{catalog}.{control_schema}.watermarks"

try:
    watermarks_df = spark.table(watermark_table)
    stale_watermarks = watermarks_df.filter(
        col("updated_at") < current_timestamp() - expr("INTERVAL 24 HOURS")
    )

    if stale_watermarks.count() > 0:
        print(f"[WARNING] Stale watermarks detected in {watermark_table}:")
        stale_watermarks.show(truncate=False)
    else:
        print(f"[OK] Watermarks are fresh in {watermark_table}")
except Exception as e:
    print(f"[INFO] Watermark table not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Count Summary

# COMMAND ----------

schemas_to_check = [bronze_raw_schema, silver_schema, gold_schema]

for schema_name in schemas_to_check:
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_name}").collect()
        print(f"\n[{schema_name.upper()}] Tables: {len(tables)}")
        for t in tables:
            try:
                count = spark.table(f"{catalog}.{schema_name}.{t.tableName}").count()
                print(f"  {t.tableName}: {count:,} rows")
            except Exception:
                print(f"  {t.tableName}: ERROR reading")
    except Exception:
        print(f"\n[{schema_name.upper()}] Schema not found")

# COMMAND ----------

print(
    f"\n[HEALTH CHECK COMPLETE] env={environment} catalog={catalog} "
    f"bronze={bronze_raw_schema} silver={silver_schema} gold={gold_schema}"
)
