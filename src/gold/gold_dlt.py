# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Star Schema Models
# MAGIC
# MAGIC Creates Gold dimension and fact tables from YAML model definitions.
# MAGIC dim_date is auto-generated; all others are defined in config/gold/*.yml

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from framework.config_reader import ConfigReader
from framework.quality_engine import apply_expectations_from_raw

# COMMAND ----------

environment = spark.conf.get("environment", "dev")

config = ConfigReader(environment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date (Auto-Generated)

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension - auto-generated (2010-2030)",
    table_properties={"quality": "gold"},
)
def dim_date():
    return spark.sql("""
        WITH date_spine AS (
            SELECT explode(
                sequence(DATE'2010-01-01', DATE'2030-12-31', INTERVAL 1 DAY)
            ) AS date_key
        )
        SELECT
            date_key,
            YEAR(date_key) AS year,
            QUARTER(date_key) AS quarter,
            MONTH(date_key) AS month,
            DAY(date_key) AS day,
            DAYOFWEEK(date_key) AS day_of_week,
            WEEKOFYEAR(date_key) AS week_of_year,
            DATE_FORMAT(date_key, 'MMMM') AS month_name,
            DATE_FORMAT(date_key, 'MMM') AS month_short,
            DATE_FORMAT(date_key, 'EEEE') AS day_name,
            CASE WHEN DAYOFWEEK(date_key) IN (1, 7) THEN true ELSE false END AS is_weekend,
            CONCAT('Q', QUARTER(date_key)) AS quarter_name,
            CONCAT(YEAR(date_key), '-Q', QUARTER(date_key)) AS year_quarter,
            DATE_FORMAT(date_key, 'yyyy-MM') AS year_month,
            current_timestamp() AS etl_timestamp
        FROM date_spine
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Gold Models from YAML Config

# COMMAND ----------

def create_gold_model(model_cfg):
    """Create a Gold DLT table from a YAML model definition."""
    expectations = apply_expectations_from_raw(
        model_cfg.get("quality", {}).get("expectations", [])
    )
    model_type = model_cfg.get("type", "materialized_view")

    @dlt.table(
        name=model_cfg["name"],
        comment=model_cfg.get("description", ""),
        table_properties={"quality": "gold"},
    )
    @dlt.expect_all(expectations["warn"])
    @dlt.expect_all_or_drop(expectations["drop"])
    @dlt.expect_all_or_fail(expectations["fail"])
    def gold_table():
        return spark.sql(model_cfg["sql"])

    return gold_table


# COMMAND ----------

# Generate all Gold models from YAML
gold_models = config.get_gold_models()
print(f"[Gold] Loading {len(gold_models)} models from config (env: {environment})")

for model_cfg in gold_models:
    if model_cfg["name"] != "dim_date":  # dim_date handled above
        create_gold_model(model_cfg)
        print(f"  [{model_cfg.get('type', 'materialized_view').upper()}] {model_cfg['name']}")
