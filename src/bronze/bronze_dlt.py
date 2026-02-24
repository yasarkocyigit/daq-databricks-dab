# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Metadata-Driven Ingestion
# MAGIC
# MAGIC Dynamically creates Bronze DLT tables from YAML configuration.
# MAGIC Supports batch (full/incremental) and streaming (Event Hub, Kafka, CDC, Auto Loader).

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit
import sys
import os

# Add framework to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from framework.config_reader import ConfigReader
from framework.source_connectors import SourceConnectorFactory
from framework.quality_engine import apply_expectations

# COMMAND ----------

# Pipeline parameters (set in DLT pipeline config / databricks.yml)
environment = spark.conf.get("environment", "dev")
bronze_storage_path = spark.conf.get("bronze_storage_path", "")

# Initialize framework
config = ConfigReader(environment)
connector_factory = SourceConnectorFactory(spark, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Bronze Table Generation

# COMMAND ----------

def create_bronze_batch_table(table_cfg):
    """Factory: creates a DLT table definition for a batch (full/incremental) table."""
    expectations = apply_expectations(table_cfg)
    target_name = f"bronze_{table_cfg.target_name}"

    table_path = (
        f"{bronze_storage_path}/{table_cfg.target_name}"
        if bronze_storage_path
        else None
    )

    table_props = {
        "quality": "bronze",
        "source": table_cfg.source_name,
        "source_schema": table_cfg.source_schema,
        "source_table": table_cfg.source_table,
        "load_type": table_cfg.load_type,
    }

    @dlt.table(
        name=target_name,
        comment=f"Bronze raw: {table_cfg.source_schema}.{table_cfg.source_table}",
        path=table_path,
        table_properties=table_props,
        spark_conf={
            "spark.databricks.delta.schema.autoMerge.enabled": str(
                table_cfg.schema_drift.mode == "auto_evolve"
            ).lower()
        },
    )
    @dlt.expect_all(expectations["warn"])
    @dlt.expect_all_or_drop(expectations["drop"])
    @dlt.expect_all_or_fail(expectations["fail"])
    def bronze_table():
        df = connector_factory.get_dataframe(table_cfg)
        return df.withColumns(
            {
                "_ingestion_timestamp": current_timestamp(),
                "_source_system": lit(table_cfg.source_name),
                "_source_schema": lit(table_cfg.source_schema),
                "_source_table": lit(table_cfg.source_table),
                "_load_type": lit(table_cfg.load_type),
            }
        )

    return bronze_table


def create_bronze_streaming_table(table_cfg):
    """Factory: creates a DLT streaming table definition."""
    expectations = apply_expectations(table_cfg)
    target_name = f"bronze_{table_cfg.target_name}"

    table_path = (
        f"{bronze_storage_path}/{table_cfg.target_name}"
        if bronze_storage_path
        else None
    )

    @dlt.table(
        name=target_name,
        comment=f"Bronze streaming: {table_cfg.source_schema}.{table_cfg.source_table}",
        path=table_path,
        table_properties={
            "quality": "bronze",
            "source": table_cfg.source_name,
            "load_type": "stream",
        },
        spark_conf={
            "spark.databricks.delta.schema.autoMerge.enabled": str(
                table_cfg.schema_drift.mode == "auto_evolve"
            ).lower()
        },
    )
    @dlt.expect_all(expectations["warn"])
    @dlt.expect_all_or_drop(expectations["drop"])
    @dlt.expect_all_or_fail(expectations["fail"])
    def streaming_table():
        df = connector_factory.get_dataframe(table_cfg)
        return df.withColumns(
            {
                "_ingestion_timestamp": current_timestamp(),
                "_source_system": lit(table_cfg.source_name),
                "_source_schema": lit(table_cfg.source_schema),
                "_source_table": lit(table_cfg.source_table),
            }
        )

    return streaming_table


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate All Bronze Tables from Config

# COMMAND ----------

# Dynamically create all Bronze DLT tables
all_tables = config.get_all_table_configs()
print(f"[Bronze] Loading {len(all_tables)} tables from config (env: {environment})")

for table_cfg in all_tables:
    if table_cfg.load_type == "stream":
        create_bronze_streaming_table(table_cfg)
        print(f"  [STREAM] bronze_{table_cfg.target_name}")
    else:
        create_bronze_batch_table(table_cfg)
        print(f"  [BATCH:{table_cfg.load_type.upper()}] bronze_{table_cfg.target_name}")
