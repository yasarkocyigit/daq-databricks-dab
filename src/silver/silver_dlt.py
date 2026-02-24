# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Metadata-Driven Transformations
# MAGIC
# MAGIC Reads from Bronze DLT tables, applies transformations defined in YAML,
# MAGIC handles SCD Type 1/2, and applies data quality expectations.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit, col, trim, expr, when
from pyspark.sql import DataFrame
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from framework.config_reader import ConfigReader
from framework.quality_engine import apply_expectations

# COMMAND ----------

environment = spark.conf.get("environment", "dev")

config = ConfigReader(environment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Engine

# COMMAND ----------

def apply_silver_transformations(df: DataFrame, table_cfg) -> DataFrame:
    """Apply transformations defined in YAML config to a DataFrame."""
    for transform in table_cfg.silver_transformations:
        t_type = transform["type"]

        if t_type == "add_column":
            df = df.withColumn(transform["name"], expr(transform["expression"]))

        elif t_type == "trim_strings":
            cols = transform.get("columns", "all")
            for col_name, col_type in df.dtypes:
                if col_type == "string":
                    if cols == "all" or col_name in cols:
                        df = df.withColumn(col_name, trim(col(col_name)))

        elif t_type == "null_standardize":
            null_values = transform.get("values", ["NULL", "null", "N/A", ""])
            for col_name, col_type in df.dtypes:
                if col_type == "string":
                    condition = col(col_name)
                    for nv in null_values:
                        condition = when(col(col_name) == nv, None).otherwise(
                            condition
                        )
                    df = df.withColumn(col_name, condition)

        elif t_type == "rename_column":
            df = df.withColumnRenamed(transform["from"], transform["to"])

        elif t_type == "cast":
            df = df.withColumn(
                transform["column"],
                col(transform["column"]).cast(transform["target_type"]),
            )

        elif t_type == "drop_columns":
            df = df.drop(*transform["columns"])

    # Standard Silver metadata
    df = df.withColumns(
        {
            "_silver_timestamp": current_timestamp(),
            "_is_current": lit(True),
        }
    )

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Silver Table Generation

# COMMAND ----------

def create_silver_scd1_table(table_cfg):
    """Create a Silver DLT table with SCD Type 1 (overwrite)."""
    expectations = apply_expectations(table_cfg)
    bronze_name = f"bronze_{table_cfg.target_name}"

    @dlt.table(
        name=table_cfg.target_name,
        comment=f"Silver cleansed: {table_cfg.target_name}",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
            "pipelines.autoOptimize.managed": "true",
        },
    )
    @dlt.expect_all(expectations["warn"])
    @dlt.expect_all_or_drop(expectations["drop"])
    @dlt.expect_all_or_fail(expectations["fail"])
    def silver_table():
        df = dlt.read(bronze_name)
        return apply_silver_transformations(df, table_cfg)

    return silver_table


def create_silver_scd2_table(table_cfg):
    """Create a Silver DLT table with SCD Type 2 (history tracking via apply_changes)."""
    bronze_name = f"bronze_{table_cfg.target_name}"
    sequence_col = table_cfg.watermark_column or "_ingestion_timestamp"

    dlt.create_streaming_table(
        name=table_cfg.target_name,
        comment=f"Silver SCD2: {table_cfg.target_name}",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
        },
    )

    dlt.apply_changes(
        target=table_cfg.target_name,
        source=bronze_name,
        keys=table_cfg.merge_keys,
        sequence_by=col(sequence_col),
        stored_as_scd_type=2,
    )


def create_silver_streaming_table(table_cfg):
    """Create a Silver streaming table (for stream-type sources)."""
    expectations = apply_expectations(table_cfg)
    bronze_name = f"bronze_{table_cfg.target_name}"

    @dlt.table(
        name=table_cfg.target_name,
        comment=f"Silver streaming: {table_cfg.target_name}",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
        },
    )
    @dlt.expect_all(expectations["warn"])
    @dlt.expect_all_or_drop(expectations["drop"])
    @dlt.expect_all_or_fail(expectations["fail"])
    def silver_stream():
        df = dlt.readStream(bronze_name)
        return apply_silver_transformations(df, table_cfg)

    return silver_stream


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate All Silver Tables from Config

# COMMAND ----------

all_tables = config.get_all_table_configs()
print(f"[Silver] Loading {len(all_tables)} tables from config (env: {environment})")

for table_cfg in all_tables:
    if table_cfg.load_type == "stream":
        create_silver_streaming_table(table_cfg)
        print(f"  [STREAM] {table_cfg.target_name}")
    elif table_cfg.scd_type == 2:
        create_silver_scd2_table(table_cfg)
        print(f"  [SCD2] {table_cfg.target_name}")
    else:
        create_silver_scd1_table(table_cfg)
        print(f"  [SCD1] {table_cfg.target_name}")
