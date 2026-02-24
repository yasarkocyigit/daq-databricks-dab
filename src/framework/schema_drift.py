"""
Schema drift detection and handling.

Modes:
  - auto_evolve: Allow schema changes, log and optionally alert
  - strict: Fail on unauthorized schema changes
"""
from .config_reader import TableConfig


class SchemaDriftError(Exception):
    """Raised when schema drift is detected in strict mode."""
    pass


class SchemaDriftHandler:
    """Detects and handles schema drift between incoming data and existing tables."""

    def __init__(self, spark):
        self.spark = spark

    def check_and_handle(self, incoming_df, target_table_name: str, table_config: TableConfig):
        """
        Compare incoming DataFrame schema against existing target table.

        Args:
            incoming_df: New data DataFrame
            target_table_name: Fully qualified table name (catalog.schema.table)
            table_config: Table configuration from YAML

        Returns:
            tuple: (df, drift_detected: bool, drift_details: list[str])
        """
        drift_details = []

        try:
            existing_df = self.spark.table(target_table_name)
            existing_fields = {f.name: f.dataType for f in existing_df.schema.fields}
            incoming_fields = {f.name: f.dataType for f in incoming_df.schema.fields}

            existing_names = set(existing_fields.keys())
            incoming_names = set(incoming_fields.keys())

            # Exclude metadata columns from comparison
            meta_cols = {
                "_ingestion_timestamp", "_source_system", "_source_schema",
                "_source_table", "_load_type", "_silver_timestamp", "_is_current",
            }
            existing_names -= meta_cols
            incoming_names -= meta_cols

            new_columns = incoming_names - existing_names
            dropped_columns = existing_names - incoming_names

            # Check type changes on common columns
            type_changes = []
            for col_name in existing_names & incoming_names:
                if str(existing_fields[col_name]) != str(incoming_fields[col_name]):
                    type_changes.append(
                        f"{col_name}: {existing_fields[col_name]} -> {incoming_fields[col_name]}"
                    )

            if new_columns:
                drift_details.append(f"New columns: {sorted(new_columns)}")
            if dropped_columns:
                drift_details.append(f"Dropped columns: {sorted(dropped_columns)}")
            if type_changes:
                drift_details.append(f"Type changes: {type_changes}")

            drift_detected = bool(drift_details)

            if drift_detected:
                self._log_drift(target_table_name, drift_details)

                if table_config.schema_drift.mode == "strict":
                    self._enforce_strict(
                        table_config, new_columns, dropped_columns, type_changes
                    )

                if table_config.schema_drift.alert_on_drift:
                    self._send_drift_alert(target_table_name, drift_details)

            return incoming_df, drift_detected, drift_details

        except Exception as e:
            error_msg = str(e)
            if "Table or view not found" in error_msg or "AnalysisException" in error_msg:
                # Table doesn't exist yet — no drift to check
                return incoming_df, False, []
            raise

    def _enforce_strict(self, table_config, new_columns, dropped_columns, type_changes):
        """Raise error if schema changes are not in the allowed list."""
        allowed = table_config.schema_drift.allowed_changes

        if new_columns and "add_column" not in allowed:
            raise SchemaDriftError(
                f"New columns detected in strict mode (not allowed): {sorted(new_columns)}"
            )
        if dropped_columns and "drop_column" not in allowed:
            raise SchemaDriftError(
                f"Dropped columns detected in strict mode: {sorted(dropped_columns)}"
            )
        if type_changes and "type_change" not in allowed:
            raise SchemaDriftError(
                f"Type changes detected in strict mode: {type_changes}"
            )

    def _log_drift(self, table_name: str, details: list):
        """Log schema drift event."""
        print(f"[SCHEMA_DRIFT] {table_name}: {details}")

    def _send_drift_alert(self, table_name: str, details: list):
        """Send drift alert. Extend with Teams webhook or email integration."""
        print(f"[SCHEMA_DRIFT_ALERT] {table_name}: {details}")
