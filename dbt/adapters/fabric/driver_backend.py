"""
Driver backend abstraction for dbt-fabric.

Provides a unified interface for database connections using either
mssql-python (preferred) or pyodbc (fallback).
"""

import os
import struct
import sys
from abc import ABC, abstractmethod
from itertools import chain, repeat
from typing import Any, Callable, Dict, Optional, Tuple, Type

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabric")

# Valid driver backend options
DEFAULT_BACKEND = "auto"
VALID_BACKENDS = ("auto", "mssql-python", "pyodbc")


def convert_bytes_to_mswindows_byte_string(value: bytes) -> bytes:
    """
    Convert bytes to a Microsoft Windows byte string.

    Parameters
    ----------
    value : bytes
        The bytes.

    Returns
    -------
    out : bytes
        The Microsoft byte string.
    """
    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes


class DriverBackend(ABC):
    """Abstract base class for SQL Server driver backends."""

    name: str = "base"

    @abstractmethod
    def connect(
        self,
        connection_string: str,
        timeout: int,
        autocommit: bool,
        attrs_before: Optional[Dict] = None,
    ) -> Any:
        """Establish database connection."""
        pass

    @abstractmethod
    def set_pooling(self, enabled: bool, max_size: int = 100) -> None:
        """Configure connection pooling."""
        pass

    @abstractmethod
    def add_output_converter(
        self, connection: Any, sql_type: int, func: Callable
    ) -> None:
        """Register output type converter."""
        pass

    @abstractmethod
    def get_error_types(self) -> Tuple[Type[Exception], ...]:
        """Return tuple of exception types for error handling."""
        pass

    @abstractmethod
    def get_retryable_exceptions(self) -> Tuple[Type[Exception], ...]:
        """Return tuple of retryable exception types."""
        pass

    @abstractmethod
    def get_database_error(self) -> Type[Exception]:
        """Return the DatabaseError exception type."""
        pass

    @abstractmethod
    def build_connection_string(
        self,
        host: str,
        database: str,
        authentication: str,
        encrypt: bool,
        trust_cert: bool,
        application_name: str,
        trace_flag: bool = False,
        driver: Optional[str] = None,
        uid: Optional[str] = None,
        pwd: Optional[str] = None,
        windows_login: bool = False,
    ) -> str:
        """Build the connection string for this backend."""
        pass

    @abstractmethod
    def requires_token_bytes(self) -> bool:
        """Return True if this backend requires token byte conversion for auth."""
        pass


class MssqlPythonBackend(DriverBackend):
    """mssql-python driver backend (preferred)."""

    name = "mssql-python"

    def __init__(self):
        import mssql_python

        self._driver = mssql_python

    def connect(
        self,
        connection_string: str,
        timeout: int,
        autocommit: bool,
        attrs_before: Optional[Dict] = None,
    ) -> Any:
        # mssql-python doesn't use attrs_before - auth is in connection string
        conn = self._driver.connect(connection_string, timeout=timeout)
        conn.setautocommit(autocommit)
        return conn

    def set_pooling(self, enabled: bool, max_size: int = 100) -> None:
        if enabled:
            self._driver.pooling(max_size=max_size)
        # Note: mssql-python pooling is enabled by default and controlled per-call
        # Disabling requires not calling pooling() - the driver handles this internally

    def add_output_converter(
        self, connection: Any, sql_type: int, func: Callable
    ) -> None:
        connection.add_output_converter(sql_type, func)

    def get_error_types(self) -> Tuple[Type[Exception], ...]:
        return (
            self._driver.Error,
            self._driver.DatabaseError,
            self._driver.OperationalError,
        )

    def get_retryable_exceptions(self) -> Tuple[Type[Exception], ...]:
        return (
            self._driver.OperationalError,
            self._driver.InterfaceError,
        )

    def get_database_error(self) -> Type[Exception]:
        return self._driver.DatabaseError

    def build_connection_string(
        self,
        host: str,
        database: str,
        authentication: str,
        encrypt: bool,
        trust_cert: bool,
        application_name: str,
        trace_flag: bool = False,
        driver: Optional[str] = None,
        uid: Optional[str] = None,
        pwd: Optional[str] = None,
        windows_login: bool = False,
    ) -> str:
        """Build connection string for mssql-python (no DRIVER= prefix needed)."""
        con_str = []

        con_str.append(f"SERVER={host}")
        con_str.append(f"Database={database}")

        # Authentication
        if windows_login:
            con_str.append("Trusted_Connection=Yes")
        elif authentication.lower() == "sql":
            raise self._driver.DatabaseError(
                "SQL Authentication is not supported by Microsoft Fabric"
            )
        elif "ActiveDirectory" in authentication and authentication != "ActiveDirectoryAccessToken":
            con_str.append(f"Authentication={authentication}")
            if uid:
                con_str.append(f"UID={uid}")
            if pwd:
                con_str.append(f"PWD={pwd}")

        # Encryption settings
        con_str.append(f"Encrypt={'Yes' if encrypt else 'No'}")
        con_str.append(
            f"TrustServerCertificate={'Yes' if trust_cert else 'No'}")

        # Application name
        con_str.append(f"APP={application_name}")

        # Retry settings
        con_str.append("ConnectRetryCount=3")
        con_str.append("ConnectRetryInterval=10")

        return ";".join(con_str)

    def requires_token_bytes(self) -> bool:
        return False


class PyodbcBackend(DriverBackend):
    """pyodbc driver backend (fallback)."""

    name = "pyodbc"

    def __init__(self):
        import pyodbc

        self._driver = pyodbc

    def connect(
        self,
        connection_string: str,
        timeout: int,
        autocommit: bool,
        attrs_before: Optional[Dict] = None,
    ) -> Any:
        handle = self._driver.connect(
            connection_string,
            attrs_before=attrs_before or {},
            autocommit=autocommit,
            timeout=timeout,
        )
        return handle

    def set_pooling(self, enabled: bool, max_size: int = 100) -> None:
        self._driver.pooling = enabled

    def add_output_converter(
        self, connection: Any, sql_type: int, func: Callable
    ) -> None:
        connection.add_output_converter(sql_type, func)

    def get_error_types(self) -> Tuple[Type[Exception], ...]:
        return (
            self._driver.Error,
            self._driver.DatabaseError,
            self._driver.OperationalError,
        )

    def get_retryable_exceptions(self) -> Tuple[Type[Exception], ...]:
        return (
            self._driver.InternalError,
            self._driver.OperationalError,
            self._driver.InterfaceError,
        )

    def get_database_error(self) -> Type[Exception]:
        return self._driver.DatabaseError

    def build_connection_string(
        self,
        host: str,
        database: str,
        authentication: str,
        encrypt: bool,
        trust_cert: bool,
        application_name: str,
        trace_flag: bool = False,
        driver: Optional[str] = None,
        uid: Optional[str] = None,
        pwd: Optional[str] = None,
        windows_login: bool = False,
    ) -> str:
        """Build connection string for pyodbc (requires DRIVER= prefix)."""
        if not driver:
            driver = "ODBC Driver 18 for SQL Server"

        con_str = [f"DRIVER={{{driver}}}"]

        con_str.append(f"SERVER={host}")
        con_str.append(f"Database={database}")
        con_str.append("Pooling=true")

        # Trace flag
        if trace_flag:
            con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")
        else:
            con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF")

        # Authentication
        if windows_login:
            con_str.append("trusted_connection=Yes")
        elif authentication.lower() == "sql":
            raise self._driver.DatabaseError(
                "SQL Authentication is not supported by Microsoft Fabric"
            )
        elif "ActiveDirectory" in authentication and authentication != "ActiveDirectoryAccessToken":
            con_str.append(f"Authentication={authentication}")
            # UID/PWD handling for AD auth types
            if authentication in ("ActiveDirectoryPassword", "ActiveDirectoryServicePrincipal", "ActiveDirectoryInteractive"):
                if uid:
                    con_str.append(f"UID={{{uid}}}")
                if pwd and authentication != "ActiveDirectoryInteractive":
                    con_str.append(f"PWD={{{pwd}}}")

        # Encryption settings
        con_str.append(f"encrypt={'Yes' if encrypt else 'No'}")
        con_str.append(
            f"TrustServerCertificate={'Yes' if trust_cert else 'No'}")

        # Application name
        con_str.append(f"APP={application_name}")

        # Retry settings
        con_str.append("ConnectRetryCount=3")
        con_str.append("ConnectRetryInterval=10")

        return ";".join(con_str)

    def requires_token_bytes(self) -> bool:
        return True


def get_effective_driver_backend(driver_backend_setting: Optional[str] = None) -> str:
    """
    Determine effective driver backend with precedence:
    1. Environment variable DBT_FABRIC_DRIVER_BACKEND
    2. Profile setting driver_backend
    3. Default "auto"

    Parameters
    ----------
    driver_backend_setting : Optional[str]
        The driver_backend setting from credentials.

    Returns
    -------
    str
        The effective driver backend setting.
    """
    env_override = os.environ.get("DBT_FABRIC_DRIVER_BACKEND")
    if env_override:
        if env_override not in VALID_BACKENDS:
            raise ValueError(
                f"Invalid DBT_FABRIC_DRIVER_BACKEND='{env_override}'. "
                f"Valid values: {', '.join(VALID_BACKENDS)}"
            )
        logger.debug(f"Using driver backend from environment: {env_override}")
        return env_override

    return driver_backend_setting or DEFAULT_BACKEND


def get_driver_backend(preferred: str = DEFAULT_BACKEND) -> DriverBackend:
    """
    Get the appropriate driver backend.

    Parameters
    ----------
    preferred : str
        "mssql-python", "pyodbc", or "auto" (default)
        "auto" tries mssql-python first, falls back to pyodbc

    Returns
    -------
    DriverBackend
        The driver backend instance.

    Raises
    ------
    ImportError
        If the requested driver is not available.
    ValueError
        If an invalid driver backend is specified.
    """
    if preferred not in VALID_BACKENDS:
        raise ValueError(
            f"Invalid driver_backend='{preferred}'. "
            f"Valid values: {', '.join(VALID_BACKENDS)}"
        )

    if preferred == "pyodbc":
        return PyodbcBackend()

    if preferred == "mssql-python":
        return MssqlPythonBackend()

    # Auto mode: try mssql-python first, fall back to pyodbc
    # mssql-python requires Python 3.10+
    if sys.version_info < (3, 10):
        logger.warning(
            "Python 3.9 does not support mssql-python driver. Using pyodbc fallback. "
            "Upgrade to Python 3.10+ for improved performance and simpler setup."
        )
        return PyodbcBackend()

    try:
        backend = MssqlPythonBackend()
        logger.debug("Using mssql-python driver backend")
        return backend
    except ImportError:
        logger.warning(
            "mssql-python not available, falling back to pyodbc. "
            "Install mssql-python for better performance: pip install mssql-python"
        )
        return PyodbcBackend()


# Cache for the active backend instance
_active_backend: Optional[DriverBackend] = None
_active_backend_preference: Optional[str] = None


def get_cached_driver_backend(preferred: str = DEFAULT_BACKEND) -> DriverBackend:
    """
    Get the driver backend, using a cached instance if available.

    This avoids repeated import checks during a dbt run.
    Caches based on the preference string (auto/mssql-python/pyodbc),
    not the resolved backend name.
    """
    global _active_backend, _active_backend_preference
    if _active_backend is None or _active_backend_preference != preferred:
        _active_backend = get_driver_backend(preferred)
        _active_backend_preference = preferred
    return _active_backend
