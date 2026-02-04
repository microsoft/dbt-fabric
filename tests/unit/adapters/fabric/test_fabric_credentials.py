"""Unit tests for fabric_credentials module."""

import pytest

from dbt.adapters.fabric.fabric_credentials import FabricCredentials


class TestFabricCredentials:
    """Tests for FabricCredentials dataclass."""

    def test_default_values(self):
        """Verify default values are set correctly."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        assert creds.driver_backend == "auto"
        assert creds.driver is None
        assert creds.UID is None
        assert creds.PWD is None
        assert creds.windows_login is False
        assert creds.trace_flag is False
        assert creds.authentication == "ActiveDirectoryServicePrincipal"
        assert creds.encrypt is True
        assert creds.trust_cert is False
        assert creds.retries == 3
        assert creds.login_timeout == 0
        assert creds.query_timeout == 0

    def test_type_property(self):
        """Verify type property returns 'fabric'."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        assert creds.type == "fabric"

    def test_unique_field_property(self):
        """Verify unique_field property returns host."""
        creds = FabricCredentials(
            host="my-server.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        assert creds.unique_field == "my-server.database.fabric.microsoft.com"

    def test_aliases(self):
        """Verify aliases are defined correctly."""
        assert FabricCredentials._ALIASES["user"] == "UID"
        assert FabricCredentials._ALIASES["username"] == "UID"
        assert FabricCredentials._ALIASES["pass"] == "PWD"
        assert FabricCredentials._ALIASES["password"] == "PWD"
        assert FabricCredentials._ALIASES["server"] == "host"
        assert FabricCredentials._ALIASES["trusted_connection"] == "windows_login"
        assert FabricCredentials._ALIASES["auth"] == "authentication"

    def test_validate_snapshot_properties_both_none(self):
        """Verify validation passes when both snapshot properties are None."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            workspace_id=None,
            warehouse_snapshot_name=None,
        )

        # Should not raise
        creds.validate_snapshot_properties()

    def test_validate_snapshot_properties_both_set(self):
        """Verify validation passes when both snapshot properties are set."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            workspace_id="workspace-123",
            warehouse_snapshot_name="my-snapshot",
        )

        # Should not raise
        creds.validate_snapshot_properties()

    def test_validate_snapshot_properties_only_workspace_id(self):
        """Verify validation fails when only workspace_id is set."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            workspace_id="workspace-123",
            warehouse_snapshot_name=None,
        )

        with pytest.raises(ValueError, match="Both workspace_id and warehouse_snapshot_name must be provided together"):
            creds.validate_snapshot_properties()

    def test_validate_snapshot_properties_only_snapshot_name(self):
        """Verify validation fails when only snapshot_name is set."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            workspace_id=None,
            warehouse_snapshot_name="my-snapshot",
        )

        with pytest.raises(ValueError, match="Both workspace_id and warehouse_snapshot_name must be provided together"):
            creds.validate_snapshot_properties()

    def test_connection_keys_returns_expected_keys(self):
        """Verify _connection_keys returns expected tuple."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        keys = creds._connection_keys()

        expected_keys = (
            "server",
            "database",
            "schema",
            "warehouse_snapshot_name",
            "snapshot_timestamp",
            "UID",
            "workspace_id",
            "authentication",
            "retries",
            "login_timeout",
            "query_timeout",
            "trace_flag",
            "encrypt",
            "trust_cert",
            "api_url",
        )
        assert keys == expected_keys

    def test_connection_keys_normalizes_windows_login(self):
        """Verify _connection_keys sets authentication to 'Windows Login' when windows_login is True."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            windows_login=True,
        )

        creds._connection_keys()

        assert creds.authentication == "Windows Login"

    def test_connection_keys_normalizes_service_principal(self):
        """Verify _connection_keys normalizes 'serviceprincipal' to full name."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="serviceprincipal",
        )

        creds._connection_keys()

        assert creds.authentication == "ActiveDirectoryServicePrincipal"

    def test_driver_backend_options(self):
        """Verify driver_backend accepts valid options."""
        for backend in ["auto", "mssql-python", "pyodbc"]:
            creds = FabricCredentials(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                schema="dbo",
                driver_backend=backend,
            )
            assert creds.driver_backend == backend

    def test_driver_only_used_for_pyodbc(self):
        """Verify driver field works with pyodbc backend."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            driver_backend="pyodbc",
            driver="ODBC Driver 18 for SQL Server",
        )

        assert creds.driver == "ODBC Driver 18 for SQL Server"

    def test_optional_fields(self):
        """Verify optional fields can be set."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            tenant_id="tenant-123",
            client_id="client-456",
            client_secret="secret-789",
            access_token="token-abc",
            access_token_expires_on=1234567890,
            schema_authorization="dbo",
            workspace_id="workspace-123",
            warehouse_snapshot_name="snapshot-name",
            warehouse_snapshot_id="snapshot-id",
            snapshot_timestamp="2024-01-01T00:00:00Z",
            api_url="https://custom.api.fabric.microsoft.com/v1",
        )

        assert creds.tenant_id == "tenant-123"
        assert creds.client_id == "client-456"
        assert creds.client_secret == "secret-789"
        assert creds.access_token == "token-abc"
        assert creds.access_token_expires_on == 1234567890
        assert creds.schema_authorization == "dbo"
        assert creds.workspace_id == "workspace-123"
        assert creds.warehouse_snapshot_name == "snapshot-name"
        assert creds.warehouse_snapshot_id == "snapshot-id"
        assert creds.snapshot_timestamp == "2024-01-01T00:00:00Z"
        assert creds.api_url == "https://custom.api.fabric.microsoft.com/v1"
