"""
Tracks watermark values for incremental loads.
Stores watermarks in a Delta table: {catalog}.framework.watermarks
"""


class WatermarkManager:
    """Manages incremental load watermarks using a Delta table."""

    TABLE_TEMPLATE = "{catalog}.framework.watermarks"

    def __init__(self, spark, catalog: str = "main"):
        self.spark = spark
        self.catalog = catalog
        self.table_name = self.TABLE_TEMPLATE.format(catalog=catalog)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create the watermarks table and schema if they don't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.framework")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                table_name STRING,
                watermark_column STRING,
                watermark_value STRING,
                row_count LONG,
                updated_at TIMESTAMP
            ) USING DELTA
            COMMENT 'Tracks incremental load watermarks for metadata-driven ingestion'
        """)

    def get_watermark(self, table_name: str) -> str:
        """Get the last watermark value for a table.

        Returns:
            Watermark value as string, or None if no watermark exists.
        """
        result = self.spark.sql(f"""
            SELECT watermark_value
            FROM {self.table_name}
            WHERE table_name = '{table_name}'
        """).collect()
        return result[0].watermark_value if result else None

    def update_watermark(
        self, table_name: str, column: str, value: str, row_count: int = 0
    ):
        """Update or insert watermark after successful load.

        Args:
            table_name: Target table name
            column: Watermark column name
            value: New watermark value
            row_count: Number of rows loaded in this batch
        """
        self.spark.sql(f"""
            MERGE INTO {self.table_name} AS target
            USING (
                SELECT
                    '{table_name}' AS table_name,
                    '{column}' AS watermark_column,
                    '{value}' AS watermark_value,
                    {row_count} AS row_count,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def get_max_watermark_from_df(self, df, watermark_column: str) -> str:
        """Get the maximum watermark value from a DataFrame.

        Args:
            df: Spark DataFrame
            watermark_column: Column to find max value of

        Returns:
            Max value as string, or None if empty.
        """
        from pyspark.sql.functions import max as spark_max

        result = df.agg(spark_max(watermark_column)).collect()
        val = result[0][0] if result else None
        return str(val) if val is not None else None

    def get_all_watermarks(self):
        """Get all watermarks as a DataFrame."""
        return self.spark.table(self.table_name)
