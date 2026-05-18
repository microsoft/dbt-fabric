from unittest import mock

import pytest

from dbt.adapters.fabric.fabric_connection_manager import FabricConnectionManager


class MockCursor:
    """Mock pyodbc cursor that simulates multiple result sets."""

    def __init__(self, rowcounts, descriptions=None):
        """
        Parameters
        ----------
        rowcounts : list[int]
            Row counts for each result set. The last value is what cursor.rowcount
            should be after stepping through all result sets.
        descriptions : list[tuple|None]
            Description per result set. None means no result set (DDL).
        """
        self._rowcounts = list(rowcounts)
        self._descriptions = descriptions or [None] * len(rowcounts)
        self._index = 0
        self.rowcount = rowcounts[0] if rowcounts else -1
        self.description = self._descriptions[0] if self._descriptions else None

    def nextset(self):
        self._index += 1
        if self._index < len(self._rowcounts):
            self.rowcount = self._rowcounts[self._index]
            self.description = self._descriptions[self._index]
            return True
        return False


class MockConnection:
    def __init__(self):
        self.name = "test_connection"
        self.transaction_open = True
        self.credentials = mock.MagicMock()
        self.credentials.retries = 1
        self.handle = mock.MagicMock()


@pytest.fixture
def connection_manager():
    cm = FabricConnectionManager.__new__(FabricConnectionManager)
    return cm


class TestExecuteRowsAffected:
    """
    Tests that execute() captures cursor.rowcount from the LAST result set,
    not the first. This is critical for table materializations where
    CREATE VIEW (rowcount=-1) runs before INSERT/CTAS (rowcount=N).
    """

    def test_rows_affected_from_last_result_set(self, connection_manager):
        """
        Simulates table materialization: CREATE VIEW (-1) then CTAS (42 rows).
        rows_affected should be 42, not -1.
        """
        cursor = MockCursor(rowcounts=[-1, 42])

        with mock.patch.object(connection_manager, "_add_query_comment", return_value="sql"):
            with mock.patch.object(
                connection_manager, "add_query", return_value=(MockConnection(), cursor)
            ):
                response, table = connection_manager.execute("sql", fetch=False)

        assert response.rows_affected == 42

    def test_rows_affected_single_statement(self, connection_manager):
        """
        Single statement returning 10 rows. rows_affected should be 10.
        """
        cursor = MockCursor(rowcounts=[10])

        with mock.patch.object(connection_manager, "_add_query_comment", return_value="sql"):
            with mock.patch.object(
                connection_manager, "add_query", return_value=(MockConnection(), cursor)
            ):
                response, table = connection_manager.execute("sql", fetch=False)

        assert response.rows_affected == 10

    def test_rows_affected_ddl_only(self, connection_manager):
        """
        DDL-only statement (CREATE VIEW). rowcount is -1 throughout.
        """
        cursor = MockCursor(rowcounts=[-1])

        with mock.patch.object(connection_manager, "_add_query_comment", return_value="sql"):
            with mock.patch.object(
                connection_manager, "add_query", return_value=(MockConnection(), cursor)
            ):
                response, table = connection_manager.execute("sql", fetch=False)

        assert response.rows_affected == -1

    def test_rows_affected_contract_path(self, connection_manager):
        """
        Simulates contract-enforced table materialization:
        CREATE VIEW (-1) -> CREATE TABLE (-1) -> INSERT INTO (100 rows).
        rows_affected should be 100.
        """
        cursor = MockCursor(rowcounts=[-1, -1, 100])

        with mock.patch.object(connection_manager, "_add_query_comment", return_value="sql"):
            with mock.patch.object(
                connection_manager, "add_query", return_value=(MockConnection(), cursor)
            ):
                response, table = connection_manager.execute("sql", fetch=False)

        assert response.rows_affected == 100

    def test_rows_affected_incremental_delete_insert(self, connection_manager):
        """
        Simulates incremental delete+insert: DELETE (5 rows) then INSERT (10 rows).
        rows_affected should be 10 (from the last INSERT).
        """
        cursor = MockCursor(rowcounts=[5, 10])

        with mock.patch.object(connection_manager, "_add_query_comment", return_value="sql"):
            with mock.patch.object(
                connection_manager, "add_query", return_value=(MockConnection(), cursor)
            ):
                response, table = connection_manager.execute("sql", fetch=False)

        assert response.rows_affected == 10
