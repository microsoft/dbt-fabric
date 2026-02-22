import weakref

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric.fabric_livy_session import LivySession
from dbt.adapters.fabricspark.fabricspark_cursor import FabricSparkCursor

logger = AdapterLogger("fabricspark")


class FabricSparkConnection:
    """A DB-API 2.0 (PEP 249) compatible connection for Fabric Spark."""

    def __init__(self, livy_session: LivySession) -> None:
        self._livy_session: LivySession | None = livy_session
        self._cursors: weakref.WeakSet[FabricSparkCursor] = weakref.WeakSet()

    def close(self) -> None:
        for cursor in self._cursors:
            cursor.close()
        self._cursors.clear()
        self._livy_session = None

    def cancel(self) -> None:
        for cursor in self._cursors:
            cursor.cancel()

    def rollback(self) -> None:
        logger.debug("Rollback is not supported in Fabric Spark, skipping.")

    def cursor(self) -> FabricSparkCursor:
        cursor = FabricSparkCursor(self)
        self._cursors.add(cursor)
        return cursor

    def get_livy_session(self) -> LivySession:
        assert self._livy_session is not None, "Connection is closed"
        return self._livy_session
