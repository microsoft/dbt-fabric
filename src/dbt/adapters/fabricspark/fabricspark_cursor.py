from collections.abc import Iterator
from types import TracebackType
from typing import Any, Self

from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.fabric.fabric_livy_session import LivySession, LivySessionResult


class FabricSparkCursor:
    """A DB-API 2.0 (PEP 249) compatible cursor for Fabric Spark."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._result: LivySessionResult | None = None

    @property
    def connection(self) -> Any:
        assert self._connection is not None, "Cursor is closed"
        return self._connection

    def close(self) -> None:
        self._connection = None
        self._result = None

    def get_livy_session(self) -> LivySession:
        return self.connection.get_livy_session()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: BaseException | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

    def execute(self, sql: str, *parameters: Any) -> None:
        params_not_none = [p for p in parameters if p is not None]
        if len(params_not_none) > 0:
            raise NotImplementedError(
                "Parameterized queries are not supported in Fabric Spark adapter."
            )

        self._result = self.get_livy_session().run_statement(sql, "sql")
        if not self._result.success:
            raise DbtDatabaseError(f"Error executing SQL statement: {self._result.error_message}")

    @property
    def messages(self) -> list[tuple[type[Exception], Any]]:
        raise NotImplementedError

    @property
    def rowcount(self) -> int:
        raise NotImplementedError

    @property
    def description(
        self,
    ) -> list[tuple[str, Any, None, None, None, None, None]] | None:
        raise NotImplementedError

    def callproc(self, procname: str, parameters: tuple[Any, ...] = ()) -> tuple[Any, ...]:
        raise NotImplementedError

    def cancel(self) -> None:
        raise NotImplementedError

    def executemany(
        self, sql: str, seq_of_parameters: list[tuple[Any, ...] | dict[str, Any]]
    ) -> None:
        raise NotImplementedError

    def fetchone(self) -> tuple[Any, ...] | None:
        raise NotImplementedError

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        raise NotImplementedError

    def fetchall(self) -> list[tuple[Any, ...]]:
        raise NotImplementedError

    def nextset(self) -> bool | None:
        raise NotImplementedError

    @property
    def arraysize(self) -> int:
        raise NotImplementedError

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        raise NotImplementedError

    def setinputsizes(self, sizes: list[Any]) -> None:
        raise NotImplementedError

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        raise NotImplementedError

    @property
    def rownumber(self) -> int | None:
        raise NotImplementedError

    def scroll(self, value: int, mode: str = "relative") -> None:
        raise NotImplementedError

    def next(self) -> tuple[Any, ...]:
        raise NotImplementedError

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        raise NotImplementedError

    @property
    def lastrowid(self) -> int | None:
        raise NotImplementedError
