"""
Source connector factory: returns a Spark DataFrame (batch or streaming)
based on the source type and table configuration from YAML.
"""
from .config_reader import ConfigReader, TableConfig


class SourceConnectorFactory:
    """Factory that creates Spark DataFrames from various source systems."""

    def __init__(self, spark, config_reader: ConfigReader):
        self.spark = spark
        self.config_reader = config_reader

    def get_dataframe(self, table_config: TableConfig):
        """Return batch or streaming DataFrame based on table config."""
        source_cfg = self.config_reader.get_source_config(table_config.source_name)

        if table_config.load_type == "stream":
            return self._get_streaming_df(table_config, source_cfg)
        else:
            return self._get_batch_df(table_config, source_cfg)

    def _get_batch_df(self, table_config: TableConfig, source_cfg: dict):
        """Route to the correct batch reader."""
        source_type = source_cfg["type"]
        if source_type == "jdbc":
            return self._read_jdbc(table_config, source_cfg)
        elif source_type == "autoloader":
            return self._read_autoloader_batch(table_config, source_cfg)
        else:
            raise ValueError(f"Unsupported batch source type: {source_type}")

    def _get_streaming_df(self, table_config: TableConfig, source_cfg: dict):
        """Route to the correct streaming reader."""
        stream_type = table_config.stream_config.source_type
        if stream_type == "autoloader":
            return self._read_autoloader_stream(table_config, source_cfg)
        elif stream_type == "eventhub":
            return self._read_eventhub(table_config, source_cfg)
        elif stream_type == "kafka":
            return self._read_kafka(table_config, source_cfg)
        elif stream_type == "cdc":
            return self._read_cdc(table_config, source_cfg)
        else:
            raise ValueError(f"Unsupported stream type: {stream_type}")

    # ── JDBC (SQL Server primary) ──────────────────────────────────────

    def _read_jdbc(self, table_config: TableConfig, source_cfg: dict):
        """Read from JDBC source with full or incremental load support."""
        conn = source_cfg["connection"]
        scope = conn["host_secret_scope"]

        # Resolve secrets
        dbutils = self._get_dbutils()
        host = dbutils.secrets.get(scope=scope, key=conn["host_secret_key"])
        user = dbutils.secrets.get(scope=scope, key=conn["username_secret_key"])
        password = dbutils.secrets.get(scope=scope, key=conn["password_secret_key"])

        jdbc_url = (
            f"jdbc:sqlserver://{host}:{conn.get('port', 1433)};"
            f"database={conn['database']};"
            f"encrypt=true;trustServerCertificate=true"
        )

        # Build query
        query = (
            f"SELECT * FROM [{table_config.source_schema}]"
            f".[{table_config.source_table}]"
        )

        # Incremental: add watermark filter
        if (
            table_config.load_type == "incremental"
            and table_config.watermark_column
        ):
            from .watermark_manager import WatermarkManager

            catalog = self.config_reader.get_env_value("catalog", default="main")
            wm = WatermarkManager(self.spark, catalog=catalog)
            last_watermark = wm.get_watermark(table_config.target_name)
            if last_watermark:
                query += (
                    f" WHERE [{table_config.watermark_column}] > '{last_watermark}'"
                )

        reader = (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({query}) AS src")
            .option("user", user)
            .option("password", password)
            .option(
                "driver",
                conn.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
            )
            .option("fetchsize", str(table_config.fetch_size))
        )

        # Additional JDBC options from config
        for k, v in conn.get("jdbc_options", {}).items():
            reader = reader.option(k, str(v))

        return reader.load()

    # ── Auto Loader ────────────────────────────────────────────────────

    def _read_autoloader_batch(self, table_config: TableConfig, source_cfg: dict):
        """Read files as a batch using Auto Loader format."""
        al_cfg = source_cfg.get("autoloader", {})
        return (
            self.spark.read.format(al_cfg.get("format", "parquet"))
            .load(al_cfg["path"])
        )

    def _read_autoloader_stream(self, table_config: TableConfig, source_cfg: dict):
        """Read using Auto Loader (cloudFiles) for streaming."""
        al_cfg = source_cfg.get("autoloader", {})
        schema_evolution = (
            "addNewColumns"
            if table_config.schema_drift.mode == "auto_evolve"
            else "failOnNewColumns"
        )

        return (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", al_cfg.get("format", "json"))
            .option("cloudFiles.schemaLocation", al_cfg.get("schema_location", ""))
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", schema_evolution)
            .load(al_cfg["path"])
        )

    # ── Event Hub ──────────────────────────────────────────────────────

    def _read_eventhub(self, table_config: TableConfig, source_cfg: dict):
        """Read from Azure Event Hub."""
        eh_cfg = source_cfg["eventhub"]
        scope = source_cfg["connection"]["host_secret_scope"]
        dbutils = self._get_dbutils()

        conn_str = dbutils.secrets.get(
            scope=scope, key=eh_cfg["connection_string_secret_key"]
        )

        # Encrypt connection string for Event Hubs connector
        encrypted = self.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            conn_str
        )

        starting = eh_cfg.get("starting_position", "earliest")
        start_pos = (
            '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
            if starting == "earliest"
            else '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":false}'
        )

        eh_conf = {
            "eventhubs.connectionString": encrypted,
            "eventhubs.consumerGroup": eh_cfg.get("consumer_group", "$Default"),
            "eventhubs.startingPosition": start_pos,
        }

        if table_config.stream_config.max_offsets_per_trigger:
            eh_conf["maxEventsPerTrigger"] = str(
                table_config.stream_config.max_offsets_per_trigger
            )

        return self.spark.readStream.format("eventhubs").options(**eh_conf).load()

    # ── Kafka ──────────────────────────────────────────────────────────

    def _read_kafka(self, table_config: TableConfig, source_cfg: dict):
        """Read from Apache Kafka."""
        k_cfg = source_cfg["kafka"]
        scope = source_cfg["connection"]["host_secret_scope"]
        dbutils = self._get_dbutils()

        brokers = dbutils.secrets.get(
            scope=scope, key=k_cfg["bootstrap_servers_secret_key"]
        )

        reader = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", k_cfg["topic"])
            .option("startingOffsets", k_cfg.get("starting_offsets", "earliest"))
        )

        if k_cfg.get("security_protocol"):
            reader = reader.option(
                "kafka.security.protocol", k_cfg["security_protocol"]
            )

        # SASL authentication
        if k_cfg.get("sasl_mechanism"):
            sasl_user = dbutils.secrets.get(
                scope=scope, key=k_cfg["sasl_username_secret_key"]
            )
            sasl_pass = dbutils.secrets.get(
                scope=scope, key=k_cfg["sasl_password_secret_key"]
            )
            jaas_config = (
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{sasl_user}" password="{sasl_pass}";'
            )
            reader = (
                reader.option("kafka.sasl.mechanism", k_cfg["sasl_mechanism"])
                .option("kafka.sasl.jaas.config", jaas_config)
            )

        if table_config.stream_config.max_offsets_per_trigger:
            reader = reader.option(
                "maxOffsetsPerTrigger",
                str(table_config.stream_config.max_offsets_per_trigger),
            )

        return reader.load()

    # ── CDC (via Kafka Connect / Debezium) ─────────────────────────────

    def _read_cdc(self, table_config: TableConfig, source_cfg: dict):
        """Read CDC changes via Kafka (Debezium topic naming convention)."""
        k_cfg = source_cfg.get("kafka", source_cfg.get("cdc", {}))

        # Debezium default topic: {connector}.{schema}.{table}
        topic = k_cfg.get("topic") or (
            f"dbserver1.{table_config.source_schema}.{table_config.source_table}"
        )

        # Create a modified source config with the CDC topic
        source_cfg_copy = dict(source_cfg)
        if "kafka" not in source_cfg_copy:
            source_cfg_copy["kafka"] = dict(k_cfg)
        else:
            source_cfg_copy["kafka"] = dict(source_cfg_copy["kafka"])
        source_cfg_copy["kafka"]["topic"] = topic

        return self._read_kafka(table_config, source_cfg_copy)

    # ── Utilities ──────────────────────────────────────────────────────

    def _get_dbutils(self):
        """Get dbutils from the Spark session (Databricks runtime only)."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            # Fallback for Databricks notebook context
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
