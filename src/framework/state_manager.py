"""
State manager for framework control tables.

Creates and maintains tables under:
  {catalog}.{control_schema}

Tables:
  - watermarks
  - batch_state
  - reconciliation_log
  - source_metadata
"""


class StateManager:
    """State manager for watermarks, batch ids, and audit metadata."""

    def __init__(self, spark, catalog: str = "main", control_schema: str = "control"):
        self.spark = spark
        self.catalog = catalog
        self.control_schema = control_schema
        self.base = f"{catalog}.{control_schema}"

        self.watermarks = f"{self.base}.watermarks"
        self.batch_state = f"{self.base}.batch_state"
        self.reconciliation_log = f"{self.base}.reconciliation_log"
        self.source_metadata = f"{self.base}.source_metadata"

    def ensure_control_tables(self):
        """Ensure control schema and all framework tables exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.base}")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.watermarks} (
                table_name STRING,
                watermark_column STRING,
                watermark_value STRING,
                row_count LONG,
                updated_at TIMESTAMP
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.batch_state} (
                table_name STRING,
                batch_id BIGINT,
                batch_timestamp TIMESTAMP,
                updated_at TIMESTAMP
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.reconciliation_log} (
                table_name STRING,
                reconciliation_ts TIMESTAMP,
                method STRING,
                status STRING,
                details STRING
            ) USING DELTA
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.source_metadata} (
                table_name STRING,
                source_schema STRING,
                source_table STRING,
                schema_hash STRING,
                schema_json STRING,
                updated_at TIMESTAMP
            ) USING DELTA
        """)

    def get_watermark(self, table_name: str):
        try:
            result = self.spark.sql(f"""
                SELECT watermark_value
                FROM {self.watermarks}
                WHERE table_name = '{table_name}'
            """).collect()
            return result[0].watermark_value if result else None
        except Exception:
            return None

    def set_watermark(
        self,
        table_name: str,
        watermark_column: str,
        watermark_value: str,
        row_count: int = 0,
    ):
        self.ensure_control_tables()
        self.spark.sql(f"""
            MERGE INTO {self.watermarks} AS target
            USING (
                SELECT
                    '{table_name}' AS table_name,
                    '{watermark_column}' AS watermark_column,
                    '{watermark_value}' AS watermark_value,
                    {row_count} AS row_count,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def next_batch_id(self, table_name: str) -> int:
        """Return next monotonic batch_id and upsert into batch_state."""
        self.ensure_control_tables()

        result = self.spark.sql(f"""
            SELECT COALESCE(MAX(batch_id), 0) AS max_id
            FROM {self.batch_state}
            WHERE table_name = '{table_name}'
        """).collect()
        next_id = int(result[0].max_id) + 1

        self.spark.sql(f"""
            MERGE INTO {self.batch_state} AS target
            USING (
                SELECT
                    '{table_name}' AS table_name,
                    {next_id} AS batch_id,
                    current_timestamp() AS batch_timestamp,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        return next_id

    def log_reconciliation(self, table_name: str, method: str, status: str, details: str):
        self.ensure_control_tables()
        escaped = details.replace("'", "''")
        self.spark.sql(f"""
            INSERT INTO {self.reconciliation_log}
            VALUES (
                '{table_name}',
                current_timestamp(),
                '{method}',
                '{status}',
                '{escaped}'
            )
        """)

    def log_source_metadata(
        self,
        table_name: str,
        source_schema: str,
        source_table: str,
        schema_hash: str,
        schema_json: str,
    ):
        self.ensure_control_tables()
        escaped_json = schema_json.replace("'", "''")
        self.spark.sql(f"""
            MERGE INTO {self.source_metadata} AS target
            USING (
                SELECT
                    '{table_name}' AS table_name,
                    '{source_schema}' AS source_schema,
                    '{source_table}' AS source_table,
                    '{schema_hash}' AS schema_hash,
                    '{escaped_json}' AS schema_json,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
