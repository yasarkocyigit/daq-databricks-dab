"""
Integration tests for Unity Catalog - Connection operations.

Tests:
- list_connections
- get_connection

Note: Creating connections requires external database credentials
(Snowflake, PostgreSQL, etc.) and is not tested in CI.
"""

import logging
import pytest

from databricks_tools_core.unity_catalog import (
    list_connections,
    get_connection,
)

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestListConnections:
    """Tests for listing Lakehouse Federation connections."""

    def test_list_connections(self):
        """Should list foreign connections (may be empty)."""
        connections = list_connections()

        logger.info(f"Found {len(connections)} connections")
        for conn in connections[:5]:
            logger.info(f"  - {conn.name} (type: {conn.connection_type})")

        assert isinstance(connections, list)

    def test_list_connections_structure(self):
        """Should return ConnectionInfo objects."""
        connections = list_connections()

        if len(connections) > 0:
            conn = connections[0]
            assert hasattr(conn, "name")
            assert hasattr(conn, "connection_type")
            logger.info(f"First connection: {conn.name} ({conn.connection_type})")


@pytest.mark.integration
class TestGetConnection:
    """Tests for getting a specific connection."""

    def test_get_existing_connection(self):
        """Should get details of an existing connection."""
        connections = list_connections()
        if not connections:
            pytest.skip("No connections in workspace")

        conn_name = connections[0].name
        logger.info(f"Getting connection: {conn_name}")

        conn = get_connection(conn_name)
        assert conn.name == conn_name
        logger.info(f"Got connection: {conn.name} (type: {conn.connection_type})")

    def test_get_nonexistent_connection(self):
        """Should raise error for non-existent connection."""
        with pytest.raises(Exception):
            get_connection("nonexistent_connection_xyz_12345")
