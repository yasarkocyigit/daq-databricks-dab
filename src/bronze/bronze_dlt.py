# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Strategy-Based Ingestion
# MAGIC
# MAGIC Supports plan-aligned table strategies:
# MAGIC - snapshot
# MAGIC - incremental
# MAGIC - cdc

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, desc, expr, lit, row_number
import inspect
import os
import sys

# Add framework to path - in DLT notebooks __file__ is not available,
# so we discover the bundle root from the notebook context.
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _workspace_root = "/Workspace" + str(_nb_path).rsplit("/src/", 1)[0]
    sys.path.insert(0, os.path.join(_workspace_root, "src"))
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath("."))), "src"))

from framework.config_reader import ConfigReader
from framework.quality_engine import apply_expectations
from framework.source_connectors import SourceConnectorFactory
from framework.state_manager import StateManager

# COMMAND ----------

environment = spark.conf.get("environment", "dev")
catalog = spark.conf.get("catalog", "main")
bronze_schema = spark.conf.get("bronze_raw_schema", "bronze_raw")
bronze_storage_path = spark.conf.get("bronze_storage_path", "")
raw_prefix = spark.conf.get("bronze_raw_prefix", "raw_")
state_prefix = spark.conf.get("bronze_state_prefix", "state_")
control_schema = spark.conf.get("control_schema", "control")

config = ConfigReader(environment)
connector_factory = SourceConnectorFactory(spark, config)
state_manager = StateManager(spark, catalog=catalog, control_schema=control_schema)

# DDL must execute outside dataset functions.
try:
    state_manager.ensure_control_tables()
    print(f"[Bronze] Control tables ensured: {catalog}.{control_schema}.*")
except Exception as control_err:
    print(f"[Bronze][WARN] Control table init skipped: {control_err}")

SUPPORTS_STREAMING_TABLE = hasattr(dp, "create_streaming_table")
SUPPORTS_AUTO_CDC = hasattr(dp, "create_auto_cdc_flow")
SUPPORTS_AUTO_CDC_SNAPSHOT = hasattr(dp, "create_auto_cdc_from_snapshot_flow")

# COMMAND ----------


def _raw_name(table_cfg):
    return f"{raw_prefix}{table_cfg.target_name}"



def _state_name(table_cfg):
    return f"{state_prefix}{table_cfg.target_name}"



def _snapshot_view_name(table_cfg):
    return f"snapshot_{table_cfg.target_name}_source"



def _add_ingestion_metadata(df, table_cfg, load_label: str):
    return df.withColumns(
        {
            "_ingestion_timestamp": current_timestamp(),
            "_source_system": lit(table_cfg.source_name),
            "_source_database": lit(table_cfg.database),
            "_source_schema": lit(table_cfg.source_schema),
            "_source_table": lit(table_cfg.source_table),
            "_load_type": lit(load_label),
        }
    )



def _materialized_view_path(table_name: str):
    if not bronze_storage_path:
        return None
    return f"{bronze_storage_path}/{table_name}"


def _filter_supported_kwargs(func, kwargs: dict):
    """
    Keep only kwargs supported by the runtime function signature.
    Some DLT runtimes expose older AUTO CDC signatures that do not accept
    optional arguments like `name` / `comment`.
    """
    try:
        signature = inspect.signature(func)
        params = signature.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        return {k: v for k, v in kwargs.items() if k in params}
    except (TypeError, ValueError):
        # Fallback for callables without inspectable signatures.
        filtered = dict(kwargs)
        filtered.pop("name", None)
        filtered.pop("comment", None)
        return filtered



def _create_snapshot_temp_view(table_cfg, expectations):
    view_name = _snapshot_view_name(table_cfg)

    @dp.temporary_view(name=view_name)
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _snapshot_view():
        source_df = connector_factory.get_dataframe(table_cfg)
        return _add_ingestion_metadata(source_df, table_cfg, "snapshot")

    return view_name



def _create_snapshot_raw_audit(table_cfg, source_view_name):
    raw_name = _raw_name(table_cfg)

    @dp.materialized_view(
        name=raw_name,
        comment=f"Bronze raw snapshot audit: {table_cfg.source_schema}.{table_cfg.source_table}",
        path=_materialized_view_path(raw_name),
        table_properties={
            "quality": "bronze_raw",
            "strategy": "snapshot",
            "source": table_cfg.source_name,
            "source_table": table_cfg.source_table,
        },
    )
    def _raw_snapshot():
        return spark.read.table(source_view_name)



def _create_snapshot_state(table_cfg, source_view_name):
    state_name = _state_name(table_cfg)

    if SUPPORTS_STREAMING_TABLE and SUPPORTS_AUTO_CDC_SNAPSHOT and table_cfg.primary_key:
        dp.create_streaming_table(
            name=state_name,
            comment=f"Bronze state (snapshot-diff): {table_cfg.target_name}",
            table_properties={
                "quality": "bronze_state",
                "strategy": "snapshot",
                "source": table_cfg.source_name,
            },
            spark_conf={
                "spark.databricks.delta.schema.autoMerge.enabled": str(
                    table_cfg.schema_drift.mode == "auto_evolve"
                ).lower()
            },
        )

        flow_kwargs = {
            "target": state_name,
            "source": source_view_name,
            "keys": table_cfg.primary_key,
            "stored_as_scd_type": table_cfg.stored_as_scd_type,
            "name": f"snapshot_flow_{table_cfg.target_name}",
            "comment": f"Snapshot flow for {table_cfg.target_name}",
        }

        # TRACK HISTORY is valid only for SCD Type 2.
        if int(table_cfg.stored_as_scd_type) == 2:
            flow_kwargs["track_history_except_column_list"] = [
                "_ingestion_timestamp",
                "_load_type",
            ]

        dp.create_auto_cdc_from_snapshot_flow(
            **_filter_supported_kwargs(dp.create_auto_cdc_from_snapshot_flow, flow_kwargs)
        )
        return

    # Fallback where AUTO CDC snapshot API is unavailable.
    @dp.materialized_view(
        name=state_name,
        comment=f"Bronze state fallback MV: {table_cfg.target_name}",
        path=_materialized_view_path(state_name),
        table_properties={
            "quality": "bronze_state",
            "strategy": "snapshot_fallback",
            "source": table_cfg.source_name,
        },
    )
    def _state_snapshot_fallback():
        return spark.read.table(source_view_name)



def create_snapshot_strategy(table_cfg):
    expectations = apply_expectations(table_cfg)
    source_view_name = _create_snapshot_temp_view(table_cfg, expectations)

    if table_cfg.snapshot_audit.enabled:
        _create_snapshot_raw_audit(table_cfg, source_view_name)

    _create_snapshot_state(table_cfg, source_view_name)



def create_incremental_strategy(table_cfg):
    expectations = apply_expectations(table_cfg)
    raw_name = _raw_name(table_cfg)
    state_name = _state_name(table_cfg)

    @dp.materialized_view(
        name=raw_name,
        comment=f"Bronze raw incremental: {table_cfg.source_schema}.{table_cfg.source_table}",
        path=_materialized_view_path(raw_name),
        table_properties={
            "quality": "bronze_raw",
            "strategy": "incremental",
            "source": table_cfg.source_name,
            "watermark_column": table_cfg.watermark_column or "",
        },
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _raw_incremental():
        source_df = connector_factory.get_dataframe(table_cfg)
        return _add_ingestion_metadata(source_df, table_cfg, "incremental")

    @dp.materialized_view(
        name=state_name,
        comment=f"Bronze state (incremental merge approximation): {table_cfg.target_name}",
        path=_materialized_view_path(state_name),
        table_properties={
            "quality": "bronze_state",
            "strategy": "incremental",
            "source": table_cfg.source_name,
        },
    )
    def _state_incremental():
        try:
            df = spark.read.table(f"LIVE.{raw_name}")
        except Exception:
            df = spark.read.table(f"{catalog}.{bronze_schema}.{raw_name}")

        sequence_col = table_cfg.sequence_by_column or "_ingestion_timestamp"
        if not table_cfg.primary_key:
            return df

        w = Window.partitionBy(*[col(k) for k in table_cfg.primary_key]).orderBy(
            desc(sequence_col), desc("_ingestion_timestamp")
        )
        return (
            df.withColumn("_rn", row_number().over(w))
            .where(col("_rn") == 1)
            .drop("_rn")
        )



def create_cdc_strategy(table_cfg):
    expectations = apply_expectations(table_cfg)
    raw_name = _raw_name(table_cfg)
    state_name = _state_name(table_cfg)

    @dp.table(
        name=raw_name,
        comment=f"Bronze raw CDC stream: {table_cfg.source_schema}.{table_cfg.source_table}",
        path=_materialized_view_path(raw_name),
        table_properties={
            "quality": "bronze_raw",
            "strategy": "cdc",
            "source": table_cfg.source_name,
        },
        spark_conf={
            "spark.databricks.delta.schema.autoMerge.enabled": str(
                table_cfg.schema_drift.mode == "auto_evolve"
            ).lower()
        },
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _raw_cdc_stream():
        stream_df = connector_factory.get_dataframe(table_cfg)
        return _add_ingestion_metadata(stream_df, table_cfg, "cdc")

    if SUPPORTS_STREAMING_TABLE and SUPPORTS_AUTO_CDC and table_cfg.primary_key:
        dp.create_streaming_table(
            name=state_name,
            comment=f"Bronze state CDC apply: {table_cfg.target_name}",
            table_properties={
                "quality": "bronze_state",
                "strategy": "cdc",
                "source": table_cfg.source_name,
            },
        )

        sequence_col = table_cfg.sequence_by_column or "_ingestion_timestamp"
        flow_kwargs = {
            "target": state_name,
            "source": raw_name,
            "keys": table_cfg.primary_key,
            "sequence_by": col(sequence_col),
            "stored_as_scd_type": table_cfg.stored_as_scd_type,
            "name": f"cdc_flow_{table_cfg.target_name}",
            "comment": f"CDC flow for {table_cfg.target_name}",
        }

        if table_cfg.cdc.operation_column and table_cfg.cdc.delete_values:
            delete_values = ", ".join(
                [f"'{v.lower()}'" for v in table_cfg.cdc.delete_values]
            )
            delete_expr = f"lower({table_cfg.cdc.operation_column}) IN ({delete_values})"
            flow_kwargs["apply_as_deletes"] = expr(delete_expr)

        dp.create_auto_cdc_flow(
            **_filter_supported_kwargs(dp.create_auto_cdc_flow, flow_kwargs)
        )
        return

    @dp.table(
        name=state_name,
        comment=f"Bronze state CDC fallback stream: {table_cfg.target_name}",
        path=_materialized_view_path(state_name),
        table_properties={
            "quality": "bronze_state",
            "strategy": "cdc_fallback",
            "source": table_cfg.source_name,
        },
    )
    def _state_cdc_fallback():
        return spark.readStream.table(f"LIVE.{raw_name}")


# COMMAND ----------

all_tables = config.get_all_table_configs()
print(f"[Bronze] Loading {len(all_tables)} tables from config (env: {environment})")
print(
    f"[Bronze] API support - streaming_table={SUPPORTS_STREAMING_TABLE}, "
    f"auto_cdc={SUPPORTS_AUTO_CDC}, auto_cdc_snapshot={SUPPORTS_AUTO_CDC_SNAPSHOT}"
)

for table_cfg in all_tables:
    strategy = table_cfg.load_strategy

    if strategy == "snapshot":
        create_snapshot_strategy(table_cfg)
        print(f"  [SNAPSHOT] {_state_name(table_cfg)}")
    elif strategy == "incremental":
        create_incremental_strategy(table_cfg)
        print(f"  [INCREMENTAL] {_state_name(table_cfg)}")
    elif strategy in {"cdc", "stream"}:
        create_cdc_strategy(table_cfg)
        print(f"  [CDC] {_state_name(table_cfg)}")
    else:
        create_snapshot_strategy(table_cfg)
        print(f"  [SNAPSHOT:DEFAULT] {_state_name(table_cfg)}")
