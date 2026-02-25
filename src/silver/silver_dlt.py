# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleansing, Conformance, and Quarantine

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, lit, trim, when
import inspect
import os
import sys

try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _workspace_root = "/Workspace" + str(_nb_path).rsplit("/src/", 1)[0]
    sys.path.insert(0, os.path.join(_workspace_root, "src"))
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath("."))), "src"))

from framework.config_reader import ConfigReader
from framework.quality_engine import apply_expectations

# COMMAND ----------

environment = spark.conf.get("environment", "dev")
catalog = spark.conf.get("catalog", "main")
bronze_schema = spark.conf.get("bronze_raw_schema", "bronze_raw")
state_prefix = spark.conf.get("bronze_state_prefix", "state_")
quarantine_prefix = spark.conf.get("silver_quarantine_prefix", "quarantine_")

config = ConfigReader(environment)

SUPPORTS_STREAMING_TABLE = hasattr(dp, "create_streaming_table")
SUPPORTS_AUTO_CDC_SNAPSHOT = hasattr(dp, "create_auto_cdc_from_snapshot_flow")

# COMMAND ----------


def apply_silver_transformations(df: DataFrame, table_cfg) -> DataFrame:
    """Apply YAML-configured transformations to a DataFrame."""
    for transform in table_cfg.silver_transformations:
        t_type = transform["type"]

        if t_type == "add_column":
            df = df.withColumn(transform["name"], expr(transform["expression"]))

        elif t_type == "trim_strings":
            cols = transform.get("columns", "all")
            for col_name, col_type in df.dtypes:
                if col_type == "string" and (cols == "all" or col_name in cols):
                    df = df.withColumn(col_name, trim(col(col_name)))

        elif t_type == "null_standardize":
            null_values = transform.get("values", ["NULL", "null", "N/A", ""])
            for col_name, col_type in df.dtypes:
                if col_type == "string":
                    condition = col(col_name)
                    for nv in null_values:
                        condition = when(col(col_name) == nv, None).otherwise(condition)
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

    return df.withColumns(
        {
            "_silver_timestamp": current_timestamp(),
            "_is_current": lit(True),
        }
    )



def _source_state_fqn(table_cfg):
    return f"{catalog}.{bronze_schema}.{state_prefix}{table_cfg.target_name}"



def _read_source(table_cfg, streaming: bool) -> DataFrame:
    source_fqn = _source_state_fqn(table_cfg)
    return spark.readStream.table(source_fqn) if streaming else spark.read.table(source_fqn)



def _warn_violation_sql(table_cfg):
    warn_expectations = [e for e in table_cfg.expectations if e.action == "warn"]
    if not warn_expectations:
        return None, None

    clauses = [f"NOT ({exp.constraint})" for exp in warn_expectations]
    return " OR ".join(clauses), ",".join([exp.name for exp in warn_expectations])



def _filter_supported_kwargs(func, kwargs: dict):
    """
    Keep only kwargs supported by the runtime function signature.
    Some DLT runtimes do not accept optional args like `name` / `comment`
    for snapshot AUTO CDC APIs.
    """
    try:
        signature = inspect.signature(func)
        params = signature.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        return {k: v for k, v in kwargs.items() if k in params}
    except (TypeError, ValueError):
        filtered = dict(kwargs)
        filtered.pop("name", None)
        filtered.pop("comment", None)
        return filtered


def create_quarantine_table(table_cfg, streaming_mode: bool):
    violation_sql, expectation_names = _warn_violation_sql(table_cfg)
    if not violation_sql:
        return

    quarantine_name = f"{quarantine_prefix}{table_cfg.target_name}"

    if streaming_mode:

        @dp.table(
            name=quarantine_name,
            comment=f"Silver quarantine stream for {table_cfg.target_name}",
            table_properties={
                "quality": "silver_quarantine",
                "source": table_cfg.target_name,
            },
        )
        def _quarantine_stream():
            df = _read_source(table_cfg, streaming=True)
            transformed = apply_silver_transformations(df, table_cfg)
            return transformed.where(expr(violation_sql)).withColumns(
                {
                    "_quarantine_timestamp": current_timestamp(),
                    "_quarantine_expectations": lit(expectation_names),
                }
            )

        return

    @dp.materialized_view(
        name=quarantine_name,
        comment=f"Silver quarantine for {table_cfg.target_name}",
        table_properties={
            "quality": "silver_quarantine",
            "source": table_cfg.target_name,
        },
    )
    def _quarantine_batch():
        df = _read_source(table_cfg, streaming=False)
        transformed = apply_silver_transformations(df, table_cfg)
        return transformed.where(expr(violation_sql)).withColumns(
            {
                "_quarantine_timestamp": current_timestamp(),
                "_quarantine_expectations": lit(expectation_names),
            }
        )



def create_standard_silver_table(table_cfg, streaming_mode: bool):
    expectations = apply_expectations(table_cfg)

    if streaming_mode:

        @dp.table(
            name=table_cfg.target_name,
            comment=f"Silver streaming: {table_cfg.target_name}",
            table_properties={
                "quality": "silver",
                "delta.enableChangeDataFeed": "true",
            },
        )
        @dp.expect_all(expectations["warn"])
        @dp.expect_all_or_drop(expectations["drop"])
        @dp.expect_all_or_fail(expectations["fail"])
        def _silver_streaming():
            df = _read_source(table_cfg, streaming=True)
            return apply_silver_transformations(df, table_cfg)

        return

    @dp.materialized_view(
        name=table_cfg.target_name,
        comment=f"Silver cleansed: {table_cfg.target_name}",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
            "pipelines.autoOptimize.managed": "true",
        },
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _silver_batch():
        df = _read_source(table_cfg, streaming=False)
        return apply_silver_transformations(df, table_cfg)



def create_scd2_silver_table(table_cfg):
    """Create Silver SCD2 table using snapshot AUTO CDC when available."""
    expectations = apply_expectations(table_cfg)
    view_name = f"silver_snapshot_{table_cfg.target_name}"

    @dp.temporary_view(name=view_name)
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _scd2_snapshot_source():
        df = _read_source(table_cfg, streaming=False)
        return apply_silver_transformations(df, table_cfg)

    if SUPPORTS_STREAMING_TABLE and SUPPORTS_AUTO_CDC_SNAPSHOT and table_cfg.merge_keys:
        dp.create_streaming_table(
            name=table_cfg.target_name,
            comment=f"Silver SCD2: {table_cfg.target_name}",
            table_properties={
                "quality": "silver",
                "scd_type": "2",
                "delta.enableChangeDataFeed": "true",
            },
        )

        flow_kwargs = {
            "target": table_cfg.target_name,
            "source": view_name,
            "keys": table_cfg.merge_keys,
            "stored_as_scd_type": 2,
            "name": f"silver_scd2_{table_cfg.target_name}",
            "comment": f"Silver SCD2 snapshot flow for {table_cfg.target_name}",
            "track_history_except_column_list": ["_silver_timestamp"],
        }
        dp.create_auto_cdc_from_snapshot_flow(
            **_filter_supported_kwargs(dp.create_auto_cdc_from_snapshot_flow, flow_kwargs)
        )
        return

    # Fallback if AUTO CDC snapshot API is not available.
    create_standard_silver_table(table_cfg, streaming_mode=False)


# COMMAND ----------

all_tables = config.get_all_table_configs()
print(f"[Silver] Loading {len(all_tables)} tables from config (env: {environment})")

for table_cfg in all_tables:
    streaming_mode = (
        table_cfg.silver_mode == "streaming"
        or table_cfg.load_strategy in {"cdc", "stream"}
        or table_cfg.load_type == "stream"
    )

    if table_cfg.scd_type == 2 and not streaming_mode:
        create_scd2_silver_table(table_cfg)
        print(f"  [SCD2] {table_cfg.target_name}")
    else:
        create_standard_silver_table(table_cfg, streaming_mode=streaming_mode)
        mode_label = "STREAM" if streaming_mode else "BATCH"
        print(f"  [{mode_label}] {table_cfg.target_name}")

    create_quarantine_table(table_cfg, streaming_mode=streaming_mode)
    if _warn_violation_sql(table_cfg)[0]:
        print(f"  [QUARANTINE] {quarantine_prefix}{table_cfg.target_name}")
