"""Unit tests for fabric_connection_manager module."""

import datetime as dt
import json
import sys
import time
from unittest import mock
from unittest.mock import patch, MagicMock

import pytest
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential

from dbt.adapters.fabric.fabric_connection_manager import (
    bool_to_connection_string_arg,
    byte_array_to_datetime,
    convert_access_token_to_mswindows_byte_string,
    get_token_attrs_before,
    get_pyodbc_attrs_before_credentials,
    get_cli_access_token,
    get_auto_access_token,
    get_environment_access_token,
    _should_run_init,
    get_dbt_run_status,
    AZURE_CREDENTIAL_SCOPE,
    AZURE_AUTH_FUNCTIONS,
)
from dbt.adapters.fabric.fabric_credentials import FabricCredentials


CHECK_OUTPUT = AzureCliCredential.__module__ + ".subprocess.check_output"


class TestConvertAccessTokenToMswindowsByteString:
    """Tests for convert_access_token_to_mswindows_byte_string function."""

    def test_converts_token_to_byte_string(self):
        """Verify access token is converted to MS Windows byte string."""
        token = AccessToken(token="test_token_value", expires_on=1234567890)

        result = convert_access_token_to_mswindows_byte_string(token)

        assert isinstance(result, bytes)
        # Length prefix (4 bytes) + UTF-16LE encoded string
        expected_content_length = len("test_token_value") * 2
        assert len(result) == 4 + expected_content_length

    def test_handles_empty_token(self):
        """Verify empty token produces minimal output."""
        token = AccessToken(token="", expires_on=1234567890)

        result = convert_access_token_to_mswindows_byte_string(token)

        # Just the length prefix (0)
        assert result == b"\x00\x00\x00\x00"


class TestBoolToConnectionStringArg:
    """Tests for bool_to_connection_string_arg function."""

    def test_true_value(self):
        """Verify True converts to 'Yes'."""
        result = bool_to_connection_string_arg("Encrypt", True)
        assert result == "Encrypt=Yes"

    def test_false_value(self):
        """Verify False converts to 'No'."""
        result = bool_to_connection_string_arg("TrustServerCertificate", False)
        assert result == "TrustServerCertificate=No"


class TestByteArrayToDatetime:
    """Tests for byte_array_to_datetime function."""

    def test_converts_datetimeoffset_bytes(self):
        """Verify DATETIMEOFFSET bytes convert to datetime."""
        # SQL_SS_TIMESTAMPOFFSET_STRUCT for 2022-12-17 17:52:18.123456 -02:30
        value = bytes([
            0xE6, 0x07,  # 2022 year
            0x0C, 0x00,  # 12 month
            0x11, 0x00,  # 17 day
            0x11, 0x00,  # 17 hour
            0x34, 0x00,  # 52 minute
            0x12, 0x00,  # 18 second
            0xBC, 0xCC, 0x5B, 0x07,  # 123456700 nanoseconds
            0xFE, 0xFF,  # -2 offset hour
            0xE2, 0xFF,  # -30 offset minute
        ])

        result = byte_array_to_datetime(value)

        assert result.year == 2022
        assert result.month == 12
        assert result.day == 17
        assert result.hour == 17
        assert result.minute == 52
        assert result.second == 18
        assert result.microsecond == 123456
        assert result.tzinfo is not None

    def test_converts_positive_offset(self):
        """Verify positive timezone offset is handled."""
        # SQL_SS_TIMESTAMPOFFSET_STRUCT for 2023-06-15 10:30:00.000000 +05:30
        value = bytes([
            0xE7, 0x07,  # 2023 year
            0x06, 0x00,  # 6 month
            0x0F, 0x00,  # 15 day
            0x0A, 0x00,  # 10 hour
            0x1E, 0x00,  # 30 minute
            0x00, 0x00,  # 0 second
            0x00, 0x00, 0x00, 0x00,  # 0 nanoseconds
            0x05, 0x00,  # +5 offset hour
            0x1E, 0x00,  # +30 offset minute
        ])

        result = byte_array_to_datetime(value)

        assert result.year == 2023
        assert result.month == 6
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30


class TestGetTokenAttrsBefore:
    """Tests for get_token_attrs_before function."""

    @pytest.fixture
    def mock_pyodbc_backend(self):
        """Create a mock pyodbc backend."""
        mock_backend = MagicMock()
        mock_backend.requires_token_bytes.return_value = True
        mock_backend.name = "pyodbc"
        return mock_backend

    @pytest.fixture
    def mock_mssql_python_backend(self):
        """Create a mock mssql-python backend."""
        mock_backend = MagicMock()
        mock_backend.requires_token_bytes.return_value = False
        mock_backend.name = "mssql-python"
        return mock_backend

    def test_returns_empty_dict_for_mssql_python(self, mock_mssql_python_backend):
        """mssql-python backend should return empty dict."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="cli",
        )

        result = get_token_attrs_before(creds, mock_mssql_python_backend)

        assert result == {}

    def test_returns_empty_dict_for_service_principal(self, mock_pyodbc_backend):
        """Service principal auth should return empty dict (token in connection string)."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="ActiveDirectoryServicePrincipal",
        )

        result = get_token_attrs_before(creds, mock_pyodbc_backend)

        assert result == {}

    def test_returns_token_for_cli_auth(self, mock_pyodbc_backend):
        """CLI auth should return token in attrs_before."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="cli",
        )

        mock_token = AccessToken(
            token="test_token", expires_on=int(time.time()) + 3600)

        with patch.dict("dbt.adapters.fabric.fabric_connection_manager.AZURE_AUTH_FUNCTIONS",
                        {"cli": lambda c, s: mock_token}):
            result = get_token_attrs_before(creds, mock_pyodbc_backend)

        assert 1256 in result
        assert isinstance(result[1256], bytes)

    def test_returns_token_for_access_token_auth(self, mock_pyodbc_backend):
        """ActiveDirectoryAccessToken auth should use provided token."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="ActiveDirectoryAccessToken",
            access_token="my_access_token",
            access_token_expires_on=int(time.time()) + 3600,
        )

        result = get_token_attrs_before(creds, mock_pyodbc_backend)

        assert 1256 in result
        assert isinstance(result[1256], bytes)

    def test_raises_for_access_token_without_token(self, mock_pyodbc_backend):
        """ActiveDirectoryAccessToken without token should raise."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="ActiveDirectoryAccessToken",
            access_token=None,
            access_token_expires_on=None,
        )

        with pytest.raises(ValueError, match="Access token and access token expiry are required"):
            get_token_attrs_before(creds, mock_pyodbc_backend)


class TestGetPyodbcAttrsBeforeCredentials:
    """Tests for get_pyodbc_attrs_before_credentials backward compat function."""

    def test_returns_empty_dict_for_service_principal(self):
        """Service principal auth should return empty dict."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
            authentication="ActiveDirectoryServicePrincipal",
        )

        # Mock PyodbcBackend
        with patch.dict(sys.modules, {"pyodbc": MagicMock()}):
            result = get_pyodbc_attrs_before_credentials(creds)

        assert result == {}


class TestAzureAuthFunctions:
    """Tests for Azure authentication functions."""

    def test_azure_auth_functions_mapping_exists(self):
        """Verify all expected auth functions are in mapping."""
        assert "cli" in AZURE_AUTH_FUNCTIONS
        assert "auto" in AZURE_AUTH_FUNCTIONS
        assert "environment" in AZURE_AUTH_FUNCTIONS
        assert "synapsespark" in AZURE_AUTH_FUNCTIONS
        assert "fabricnotebook" in AZURE_AUTH_FUNCTIONS

    def test_get_cli_access_token(self):
        """Verify get_cli_access_token calls AzureCliCredential."""
        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        mock_token = AccessToken(token="test_token", expires_on=1234567890)

        with patch.object(AzureCliCredential, "get_token", return_value=mock_token):
            result = get_cli_access_token(creds)

        assert result.token == "test_token"

    def test_get_auto_access_token(self):
        """Verify get_auto_access_token calls DefaultAzureCredential."""
        from azure.identity import DefaultAzureCredential

        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        mock_token = AccessToken(token="auto_token", expires_on=1234567890)

        with patch.object(DefaultAzureCredential, "get_token", return_value=mock_token):
            result = get_auto_access_token(creds)

        assert result.token == "auto_token"

    def test_get_environment_access_token(self):
        """Verify get_environment_access_token calls EnvironmentCredential."""
        from azure.identity import EnvironmentCredential

        creds = FabricCredentials(
            host="test.database.fabric.microsoft.com",
            database="testdb",
            schema="dbo",
        )

        mock_token = AccessToken(token="env_token", expires_on=1234567890)

        with patch.object(EnvironmentCredential, "get_token", return_value=mock_token):
            result = get_environment_access_token(creds)

        assert result.token == "env_token"


class TestShouldRunInit:
    """Tests for _should_run_init function."""

    def test_returns_true_for_run_command(self):
        """Verify returns True when 'run' is in argv."""
        with patch.object(sys, "argv", ["dbt", "run"]):
            assert _should_run_init() is True

    def test_returns_true_for_build_command(self):
        """Verify returns True when 'build' is in argv."""
        with patch.object(sys, "argv", ["dbt", "build"]):
            assert _should_run_init() is True

    def test_returns_true_for_snapshot_command(self):
        """Verify returns True when 'snapshot' is in argv."""
        with patch.object(sys, "argv", ["dbt", "snapshot"]):
            assert _should_run_init() is True

    def test_returns_false_for_other_commands(self):
        """Verify returns False for non-target commands."""
        with patch.object(sys, "argv", ["dbt", "debug"]):
            assert _should_run_init() is False

        with patch.object(sys, "argv", ["dbt", "compile"]):
            assert _should_run_init() is False

    def test_returns_false_on_exception(self):
        """Verify returns False on exception."""
        with patch.object(sys, "argv", None):
            assert _should_run_init() is False


class TestGetDbtRunStatus:
    """Tests for get_dbt_run_status function."""

    def test_returns_unknown_when_no_file(self):
        """Verify returns 'unknown' when run_results.json doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            result = get_dbt_run_status()
        assert result == "unknown"

    def test_returns_success_when_all_success(self):
        """Verify returns 'success' when all results are successful."""
        mock_results = {
            "results": [
                {"status": "success"},
                {"status": "success"},
            ]
        }

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock.mock_open(read_data=json.dumps(mock_results))):
                result = get_dbt_run_status()

        assert result == "success"

    def test_returns_error_when_any_error(self):
        """Verify returns 'error' when any result has error status."""
        mock_results = {
            "results": [
                {"status": "success"},
                {"status": "error"},
            ]
        }

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock.mock_open(read_data=json.dumps(mock_results))):
                result = get_dbt_run_status()

        assert result == "error"

    def test_returns_unknown_on_empty_results(self):
        """Verify returns 'unknown' when results array is empty."""
        mock_results = {"results": []}

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock.mock_open(read_data=json.dumps(mock_results))):
                result = get_dbt_run_status()

        assert result == "unknown"

    def test_returns_unknown_on_exception(self):
        """Verify returns 'unknown' on any exception."""
        with patch("pathlib.Path.exists", side_effect=Exception("test error")):
            result = get_dbt_run_status()

        assert result == "unknown"


class TestAzureCredentialScopes:
    """Tests for Azure credential scope constants."""

    def test_azure_credential_scope(self):
        """Verify Azure credential scope is correct."""
        assert AZURE_CREDENTIAL_SCOPE == "https://database.windows.net//.default"

    def test_power_bi_scope_import(self):
        """Verify Power BI scope can be imported."""
        from dbt.adapters.fabric.fabric_connection_manager import POWER_BI_CREDENTIAL_SCOPE
        assert POWER_BI_CREDENTIAL_SCOPE == "https://api.fabric.microsoft.com/.default"

    def test_fabric_notebook_scope_import(self):
        """Verify Fabric Notebook scope can be imported."""
        from dbt.adapters.fabric.fabric_connection_manager import FABRIC_NOTEBOOK_CREDENTIAL_SCOPE
        assert FABRIC_NOTEBOOK_CREDENTIAL_SCOPE == "https://database.windows.net/"


class TestDatatypesMapping:
    """Tests for datatypes mapping constant."""

    def test_datatypes_mapping_exists(self):
        """Verify datatypes mapping has expected entries."""
        from dbt.adapters.fabric.fabric_connection_manager import datatypes

        assert datatypes["str"] == "varchar"
        assert datatypes["int"] == "int"
        assert datatypes["float"] == "bigint"
        assert datatypes["bool"] == "bit"
        assert datatypes["bytes"] == "varbinary"
        assert datatypes["datetime.date"] == "date"
        assert datatypes["datetime.datetime"] == "datetime2(6)"
