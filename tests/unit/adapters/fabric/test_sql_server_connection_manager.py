import datetime as dt
import json
from unittest import mock

import pyodbc
import pytest
from azure.identity import AzureCliCredential

from dbt.adapters.fabric.fabric_connection_manager import (
    FabricConnectionManager,
    bool_to_connection_string_arg,
    byte_array_to_datetime,
    get_pyodbc_attrs_before_credentials,
)
from dbt.adapters.fabric.fabric_credentials import FabricCredentials

# See
# https://github.com/Azure/azure-sdk-for-python/blob/azure-identity_1.5.0/sdk/identity/azure-identity/tests/test_cli_credential.py
CHECK_OUTPUT = AzureCliCredential.__module__ + ".subprocess.check_output"


@pytest.fixture
def credentials() -> FabricCredentials:
    credentials = FabricCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.fabric.net",
        database="dbt",
        schema="fabric",
    )
    return credentials


@pytest.fixture
def mock_cli_access_token() -> str:
    access_token = "access token"
    expected_expires_on = 1602015811
    successful_output = json.dumps(
        {
            "expiresOn": dt.datetime.fromtimestamp(expected_expires_on).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
            "accessToken": access_token,
            "subscription": "some-guid",
            "tenant": "some-guid",
            "tokenType": "Bearer",
        }
    )
    return successful_output


def test_get_pyodbc_attrs_before_empty_dict_when_service_principal(
    credentials: FabricCredentials,
) -> None:
    """
    When the authentication is set to sql we expect an empty attrs before.
    """
    attrs_before = get_pyodbc_attrs_before_credentials(credentials)
    assert attrs_before == {}


@pytest.mark.parametrize("authentication", ["CLI", "cli", "cLi"])
def test_get_pyodbc_attrs_before_contains_access_token_key_for_cli_authentication(
    credentials: FabricCredentials,
    authentication: str,
    mock_cli_access_token: str,
) -> None:
    """
    When the cli authentication is used, the attrs before should contain an
    access token key.
    """
    credentials.authentication = authentication
    with mock.patch(CHECK_OUTPUT, mock.Mock(return_value=mock_cli_access_token)):
        attrs_before = get_pyodbc_attrs_before_credentials(credentials)
    assert 1256 in attrs_before.keys()


@pytest.mark.parametrize(
    "key, value, expected", [("somekey", False, "somekey=No"), ("somekey", True, "somekey=Yes")]
)
def test_bool_to_connection_string_arg(key: str, value: bool, expected: str) -> None:
    assert bool_to_connection_string_arg(key, value) == expected


@pytest.mark.parametrize(
    "value, expected_datetime, expected_str",
    [
        (
            bytes(
                [
                    0xE6,
                    0x07,  # 2022       year            unsigned short
                    0x0C,
                    0x00,  # 12         month           unsigned short
                    0x11,
                    0x00,  # 17         day             unsigned short
                    0x11,
                    0x00,  # 17         hour            unsigned short
                    0x34,
                    0x00,  # 52         minute          unsigned short
                    0x12,
                    0x00,  # 18         second          unsigned short
                    0xBC,
                    0xCC,
                    0x5B,
                    0x07,  # 123456700  10⁻⁷ second     unsigned long
                    0xFE,
                    0xFF,  # -2          offset hour     signed short
                    0xE2,
                    0xFF,  # -30         offset minute   signed short
                ]
            ),
            dt.datetime(
                year=2022,
                month=12,
                day=17,
                hour=17,
                minute=52,
                second=18,
                microsecond=123456700 // 1000,  # 10⁻⁶ second
                tzinfo=dt.timezone(dt.timedelta(hours=-2, minutes=-30)),
            ),
            "2022-12-17 17:52:18.123456-02:30",
        )
    ],
)
def test_byte_array_to_datetime(
    value: bytes, expected_datetime: dt.datetime, expected_str: str
) -> None:
    """
    Assert SQL_SS_TIMESTAMPOFFSET_STRUCT bytes are converted to datetime and str
    https://learn.microsoft.com/sql/relational-databases/native-client-odbc-date-time/data-type-support-for-odbc-date-and-time-improvements#sql_ss_timestampoffset_struct
    """
    assert byte_array_to_datetime(value) == expected_datetime
    assert str(byte_array_to_datetime(value)) == expected_str


class TestGetRetryableExceptions:
    """FabricConnectionManager._get_retryable_exceptions builds the tuple that
    is threaded into both retry_connection() and execute()."""

    def test_includes_operational_and_internal_error(self, credentials: FabricCredentials) -> None:
        credentials.authentication = "ActiveDirectoryServicePrincipal"
        retryable = FabricConnectionManager._get_retryable_exceptions(credentials)
        assert pyodbc.OperationalError in retryable
        assert pyodbc.InternalError in retryable
        # InterfaceError is only added for token/AAD auth modes handled by
        # AZURE_AUTH_FUNCTIONS (cli/auto/environment/...), not for the ODBC
        # ActiveDirectory* driver auth modes.
        assert pyodbc.InterfaceError not in retryable

    def test_adds_interface_error_for_token_auth(self, credentials: FabricCredentials) -> None:
        credentials.authentication = "CLI"
        retryable = FabricConnectionManager._get_retryable_exceptions(credentials)
        assert pyodbc.InterfaceError in retryable


class _NextsetCursor:
    """
    Minimal cursor stand-in for execute()'s result-set walk.

    nextset() raises `error` on its first `fail_times` invocations for THIS
    cursor, then returns False (no further result sets). A fresh cursor is
    handed out per execute() attempt, so "fail once then succeed" is modelled
    with one failing cursor followed by a clean one.
    """

    def __init__(self, error=None, fail_times=0, rowcount=10):
        self._error = error
        self._fail_times = fail_times
        self._calls = 0
        self.rowcount = rowcount
        self.description = None

    def nextset(self):
        self._calls += 1
        if self._error is not None and self._calls <= self._fail_times:
            raise self._error
        return False


class TestExecuteResultSetRetry:
    """
    execute() must retry a connection reset (08S01 / SQLMoreResults) that
    surfaces while stepping through result sets, when the opt-in
    retry_result_set_errors credential is set. See issue #417.
    """

    @pytest.fixture
    def connection_manager(self):
        return FabricConnectionManager.__new__(FabricConnectionManager)

    def _run_execute(self, cm, credentials, cursors):
        """
        Drive execute() with add_query handing out the given cursors in order
        and _reconnect stubbed out. Returns (response, table, add_query_mock,
        reconnect_mock).
        """
        connection = mock.MagicMock()
        connection.credentials = credentials

        add_query = mock.MagicMock(side_effect=[(connection, cur) for cur in cursors])
        reconnect = mock.MagicMock(return_value=connection)

        with mock.patch.object(cm, "_add_query_comment", return_value="sql"), mock.patch.object(
            cm, "get_thread_connection", return_value=connection
        ), mock.patch.object(cm, "add_query", add_query), mock.patch.object(
            cm, "_reconnect", reconnect
        ), mock.patch(
            "time.sleep"
        ):
            response, table = cm.execute("sql", fetch=False)

        return response, table, add_query, reconnect

    def test_retries_and_succeeds_on_reset_during_nextset(self, connection_manager, credentials):
        """A retryable reset during the walk re-runs the statement on a fresh
        connection and then succeeds."""
        credentials.retry_result_set_errors = True
        credentials.retries = 3  # effective retry limit = 2

        reset = pyodbc.OperationalError("08S01", "TCP Provider: Error code 0x68 (SQLMoreResults)")
        cursors = [
            _NextsetCursor(error=reset, fail_times=1),  # attempt 1: reset
            _NextsetCursor(rowcount=42),  # attempt 2: clean
        ]

        response, _, add_query, reconnect = self._run_execute(
            connection_manager, credentials, cursors
        )

        assert response.rows_affected == 42
        assert add_query.call_count == 2
        assert reconnect.call_count == 1

    def test_raises_after_exhausting_retry_limit(self, connection_manager, credentials):
        """When every attempt resets, execute() gives up and re-raises."""
        credentials.retry_result_set_errors = True
        credentials.retries = 3  # effective retry limit = 2 -> 3 attempts total

        reset = pyodbc.OperationalError("08S01", "SQLMoreResults")
        cursors = [_NextsetCursor(error=reset, fail_times=1) for _ in range(3)]

        with pytest.raises(pyodbc.OperationalError):
            self._run_execute(connection_manager, credentials, cursors)

    def test_non_retryable_exception_not_swallowed(self, connection_manager, credentials):
        """A non-retryable error during the walk is not caught by the retry."""
        credentials.retry_result_set_errors = True
        credentials.retries = 3

        cursors = [_NextsetCursor(error=ValueError("boom"), fail_times=1)]

        with pytest.raises(ValueError):
            self._run_execute(connection_manager, credentials, cursors)

    def test_disabled_by_default_does_not_retry(self, connection_manager, credentials):
        """With the flag off (default), a reset during the walk is not retried;
        it propagates on the first occurrence, preserving prior behaviour."""
        assert credentials.retry_result_set_errors is False

        connection = mock.MagicMock()
        connection.credentials = credentials
        reset = pyodbc.OperationalError("08S01", "SQLMoreResults")
        cursor = _NextsetCursor(error=reset, fail_times=1)
        add_query = mock.MagicMock(return_value=(connection, cursor))
        reconnect = mock.MagicMock(return_value=connection)

        with mock.patch.object(
            connection_manager, "_add_query_comment", return_value="sql"
        ), mock.patch.object(
            connection_manager, "get_thread_connection", return_value=connection
        ), mock.patch.object(
            connection_manager, "add_query", add_query
        ), mock.patch.object(
            connection_manager, "_reconnect", reconnect
        ), mock.patch(
            "time.sleep"
        ):
            with pytest.raises(pyodbc.OperationalError):
                connection_manager.execute("sql", fetch=False)

        assert add_query.call_count == 1
        assert reconnect.call_count == 0

        connection = mock.MagicMock()
        connection.credentials = credentials
        reset = pyodbc.OperationalError("08S01", "SQLMoreResults")
        cursor = _NextsetCursor(error=reset, fail_times=1)
        add_query = mock.MagicMock(return_value=(connection, cursor))
        reconnect = mock.MagicMock(return_value=connection)

        with mock.patch.object(
            connection_manager, "_add_query_comment", return_value="sql"
        ), mock.patch.object(
            connection_manager, "get_thread_connection", return_value=connection
        ), mock.patch.object(
            connection_manager, "add_query", add_query
        ), mock.patch.object(
            connection_manager, "_reconnect", reconnect
        ), mock.patch(
            "time.sleep"
        ):
            with pytest.raises(pyodbc.OperationalError):
                connection_manager.execute("sql", fetch=False)

        assert add_query.call_count == 1
        assert reconnect.call_count == 0
