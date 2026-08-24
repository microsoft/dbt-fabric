import datetime as dt
import inspect
import struct
from types import SimpleNamespace
from unittest import mock

import pytest
from dbt_common.exceptions import DbtInternalError

from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.fabric.fabric_connection_manager import (
    FabricConnectionManager,
    bool_to_connection_string_arg,
    byte_array_to_datetime,
)
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.sql.connections import SQLConnectionManager


class TestBoolToConnectionStringArg:
    def test_true_value(self):
        assert bool_to_connection_string_arg("Encrypt", True) == "Encrypt=Yes"

    def test_false_value(self):
        assert bool_to_connection_string_arg("Encrypt", False) == "Encrypt=No"

    def test_different_key(self):
        assert (
            bool_to_connection_string_arg("TrustServerCertificate", True)
            == "TrustServerCertificate=Yes"
        )


class TestByteArrayToDatetime:
    def _pack(self, year, month, day, hour, minute, second, nanoseconds, tz_hour, tz_minute):
        return struct.pack(
            "<6hI2h", year, month, day, hour, minute, second, nanoseconds, tz_hour, tz_minute
        )

    def test_utc_datetime(self):
        data = self._pack(2024, 3, 15, 10, 30, 45, 123456000, 0, 0)
        result = byte_array_to_datetime(data)
        assert result == dt.datetime(2024, 3, 15, 10, 30, 45, 123456, tzinfo=dt.UTC)

    def test_positive_offset(self):
        data = self._pack(2025, 12, 31, 23, 59, 59, 0, 5, 30)
        result = byte_array_to_datetime(data)
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 31
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59
        assert result.microsecond == 0
        assert result.tzinfo == dt.timezone(dt.timedelta(hours=5, minutes=30))

    def test_negative_offset(self):
        data = self._pack(2023, 1, 1, 0, 0, 0, 500000000, -8, 0)
        result = byte_array_to_datetime(data)
        assert result.microsecond == 500000
        assert result.tzinfo == dt.timezone(dt.timedelta(hours=-8))

    def test_nanoseconds_truncated_to_microseconds(self):
        data = self._pack(2024, 6, 15, 12, 0, 0, 999999999, 0, 0)
        result = byte_array_to_datetime(data)
        assert result.microsecond == 999999


class TestDataTypeCodeToName:
    @pytest.mark.parametrize(
        ("type_code", "expected"),
        [
            ("<class 'str'>", "varchar"),
            ("<class 'int'>", "int"),
            ("<class 'float'>", "bigint"),
            ("<class 'bool'>", "bit"),
            ("<class 'datetime.datetime'>", "datetime2(6)"),
            ("<class 'datetime.date'>", "date"),
            ("<class 'decimal.Decimal'>", "decimal"),
        ],
    )
    def test_known_types(self, type_code, expected):
        assert FabricConnectionManager.data_type_code_to_name(type_code) == expected

    def test_unknown_type_raises(self):
        with pytest.raises(KeyError):
            FabricConnectionManager.data_type_code_to_name("<class 'unknown'>")


class TestServicePrincipalConnectionString:
    def test_uses_mssql_python_supported_keywords(self):
        credentials = FabricCredentials(
            database="warehouse",
            schema="dbo",
            host="server.datawarehouse.fabric.microsoft.com",
            authentication="ActiveDirectoryServicePrincipal",
            tenant_id="tenant-id",
            client_id="client-id",
            client_secret="client-secret",
            lock_timeout=0,
        )
        connection = SimpleNamespace(
            state=ConnectionState.INIT,
            credentials=credentials,
            handle=None,
        )
        handle = mock.MagicMock()
        token_provider = mock.MagicMock()
        token_provider.get_sql_attrs_before.return_value = None

        def retry_connection(connection, connect, **kwargs):
            connection.handle = connect()
            connection.state = ConnectionState.OPEN
            return connection

        FabricConnectionManager._host = None
        with (
            mock.patch("mssql_python.connect", return_value=handle) as connect,
            mock.patch.object(
                FabricConnectionManager,
                "get_fabric_token_provider",
                return_value=token_provider,
            ),
            mock.patch.object(
                FabricConnectionManager,
                "retry_connection",
                side_effect=retry_connection,
            ),
        ):
            FabricConnectionManager.open(connection)

        connection_string = connect.call_args.args[0]
        assert "Authentication=ActiveDirectoryServicePrincipal" in connection_string
        assert "UID={client-id}" in connection_string
        assert "PWD={client-secret}" in connection_string
        assert "Authority Id" not in connection_string
        assert "tenant-id" not in connection_string


class TestTransactionManagement:
    def test_execute_does_not_auto_begin_by_default(self):
        assert (
            inspect.signature(FabricConnectionManager.execute).parameters["auto_begin"].default
            is False
        )

    def test_begin_and_commit_update_connection_state(self):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)
        connection = SimpleNamespace(name="transaction-test", transaction_open=False)

        with (
            mock.patch.object(manager, "get_thread_connection", return_value=connection),
            mock.patch.object(manager, "add_begin_query") as add_begin,
            mock.patch.object(manager, "add_commit_query") as add_commit,
        ):
            assert manager.begin() is connection
            assert connection.transaction_open is True
            add_begin.assert_called_once_with()

            assert manager.commit() is connection
            assert connection.transaction_open is False
            add_commit.assert_called_once_with()

    def test_begin_rejects_nested_transaction(self):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)
        connection = SimpleNamespace(name="transaction-test", transaction_open=True)

        with mock.patch.object(manager, "get_thread_connection", return_value=connection):
            with pytest.raises(DbtInternalError, match="already had one open"):
                manager.begin()

    def test_begin_failure_leaves_transaction_closed(self):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)
        connection = SimpleNamespace(name="transaction-test", transaction_open=False)

        with (
            mock.patch.object(manager, "get_thread_connection", return_value=connection),
            mock.patch.object(
                manager, "add_begin_query", side_effect=RuntimeError("begin failed")
            ),
        ):
            with pytest.raises(RuntimeError, match="begin failed"):
                manager.begin()

        assert connection.transaction_open is False

    def test_commit_requires_open_transaction(self):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)
        connection = SimpleNamespace(name="transaction-test", transaction_open=False)

        with mock.patch.object(manager, "get_thread_connection", return_value=connection):
            with pytest.raises(DbtInternalError, match="does not have one open"):
                manager.commit()

    def test_commit_failure_leaves_transaction_open_for_rollback(self):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)
        connection = SimpleNamespace(name="transaction-test", transaction_open=True)

        with (
            mock.patch.object(manager, "get_thread_connection", return_value=connection),
            mock.patch.object(
                manager, "add_commit_query", side_effect=RuntimeError("commit failed")
            ),
        ):
            with pytest.raises(RuntimeError, match="commit failed"):
                manager.commit()

        assert connection.transaction_open is True

    def test_transaction_control_uses_guarded_tsql(self):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)

        with mock.patch.object(
            manager,
            "add_query",
            return_value=(mock.sentinel.connection, mock.sentinel.cursor),
        ) as add_query:
            manager.add_begin_query()
            manager.add_commit_query()

        assert add_query.call_args_list == [
            mock.call("BEGIN TRANSACTION", auto_begin=False),
            mock.call("IF @@TRANCOUNT > 0 COMMIT TRANSACTION", auto_begin=False),
        ]

    def test_rollback_executes_tsql_and_updates_connection_state(self):
        cursor = mock.MagicMock()
        handle = mock.MagicMock()
        handle.cursor.return_value = cursor
        connection = SimpleNamespace(
            type="fabric",
            name="transaction-test",
            state=ConnectionState.OPEN,
            transaction_open=True,
            handle=handle,
        )

        FabricConnectionManager._rollback(connection)

        cursor.execute.assert_called_once_with("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
        cursor.close.assert_called_once_with()
        assert connection.transaction_open is False

    def test_rollback_driver_failure_still_clears_dbt_transaction_state(self):
        cursor = mock.MagicMock()
        cursor.execute.side_effect = RuntimeError("rollback failed")
        handle = mock.MagicMock()
        handle.cursor.return_value = cursor
        connection = SimpleNamespace(
            type="fabric",
            name="transaction-test",
            state=ConnectionState.OPEN,
            transaction_open=True,
            handle=handle,
        )

        FabricConnectionManager._rollback(connection)

        cursor.close.assert_called_once_with()
        assert connection.transaction_open is False

    @pytest.mark.parametrize(
        ("transaction_open", "auto_begin", "expected_retry_limit"),
        [
            (False, False, 3),
            (False, True, 1),
            (True, False, 1),
            (True, True, 1),
        ],
    )
    def test_transactional_statements_are_not_retried(
        self, transaction_open, auto_begin, expected_retry_limit
    ):
        manager = FabricConnectionManager.__new__(FabricConnectionManager)
        connection = SimpleNamespace(transaction_open=transaction_open)

        with (
            mock.patch.object(manager, "get_thread_connection", return_value=connection),
            mock.patch.object(
                SQLConnectionManager,
                "add_query",
                return_value=(connection, mock.sentinel.cursor),
            ) as add_query,
        ):
            manager.add_query("select 1", auto_begin=auto_begin, retry_limit=3)

        assert add_query.call_args.args[-1] == expected_retry_limit


class MockCursor:
    def __init__(self, messages=None, rowcount=-1):
        self.messages = messages
        self.rowcount = rowcount


class TestGetResponse:
    def test_no_messages_returns_ok(self):
        cursor = MockCursor(messages=None)
        response = FabricConnectionManager.get_response(cursor)
        assert response._message == "OK"
        assert response.query_id is None

    def test_empty_messages_returns_ok(self):
        cursor = MockCursor(messages=[])
        response = FabricConnectionManager.get_response(cursor)
        assert response._message == "OK"

    def test_extracts_statement_id(self):
        cursor = MockCursor(messages=[("info", "Statement id: {abc-123-def}")])
        response = FabricConnectionManager.get_response(cursor)
        assert response.query_id == "abc-123-def"
        assert response._message == "OK"

    def test_filters_changed_database_context(self):
        cursor = MockCursor(messages=[("info", "Changed database context to 'mydb'.")])
        response = FabricConnectionManager.get_response(cursor)
        assert response._message == "OK"

    def test_keeps_other_messages(self):
        cursor = MockCursor(messages=[("info", "Warning: something happened")])
        response = FabricConnectionManager.get_response(cursor)
        assert response._message == "Warning: something happened"

    def test_multiple_messages_joined(self):
        cursor = MockCursor(
            messages=[
                ("info", "First message"),
                ("info", "Changed database context to 'x'."),
                ("info", "Second message"),
            ]
        )
        response = FabricConnectionManager.get_response(cursor)
        assert response._message == "First message\nSecond message"

    def test_rows_affected(self):
        cursor = MockCursor(messages=[], rowcount=42)
        response = FabricConnectionManager.get_response(cursor)
        assert response.rows_affected == 42

    def test_statement_id_with_other_messages(self):
        cursor = MockCursor(
            messages=[
                ("info", "statement id: {uuid-value-here}"),
                ("info", "Some useful info"),
            ]
        )
        response = FabricConnectionManager.get_response(cursor)
        assert response.query_id == "uuid-value-here"
        assert response._message == "Some useful info"
