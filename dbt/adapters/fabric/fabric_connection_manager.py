import atexit
import datetime as dt
import struct
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Type

import agate
import dbt_common.exceptions
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, DefaultAzureCredential, EnvironmentCredential
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import AdapterEventDebug, ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.utils.casting import cast_to_str

from dbt.adapters.fabric import __version__
from dbt.adapters.fabric.driver_backend import (
    DriverBackend,
    convert_bytes_to_mswindows_byte_string,
    get_cached_driver_backend,
    get_effective_driver_backend,
)
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.warehouse_snapshots import WarehouseSnapshotManager as wh_snapshot_manager

_init_done = False
_init_lock = threading.Lock()
_snapshot_manager = None

# Command filtering
TARGET_COMMANDS = {"run", "build", "snapshot"}


AZURE_CREDENTIAL_SCOPE = "https://database.windows.net//.default"
POWER_BI_CREDENTIAL_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_NOTEBOOK_CREDENTIAL_SCOPE = "https://database.windows.net/"
SYNAPSE_SPARK_CREDENTIAL_SCOPE = "DW"
_TOKEN: Optional[AccessToken] = None
AZURE_AUTH_FUNCTION_TYPE = Callable[[
    FabricCredentials, Optional[str]], AccessToken]

logger = AdapterLogger("fabric")

# https://github.com/mkleehammer/pyodbc/wiki/Data-Types
datatypes = {
    # "str": "char",
    "str": "varchar",
    "uuid.UUID": "uniqueidentifier",
    "uuid": "uniqueidentifier",
    # "float": "real",
    # "float": "float",
    "float": "bigint",
    # "int": "smallint",
    # "int": "tinyint",
    "int": "int",
    "bytes": "varbinary",
    "bytearray": "varbinary",
    "bool": "bit",
    "datetime.date": "date",
    "datetime.datetime": "datetime2(6)",
    "datetime.time": "time",
    "decimal.Decimal": "decimal",
    # "decimal.Decimal": "numeric",
}


def convert_access_token_to_mswindows_byte_string(token: AccessToken) -> bytes:
    """
    Convert an access token to a Microsoft windows byte string.

    Parameters
    ----------
    token : AccessToken
        The token.

    Returns
    -------
    out : bytes
        The Microsoft byte string.
    """
    value = bytes(token.token, "UTF-8")
    return convert_bytes_to_mswindows_byte_string(value)


def get_synapse_spark_access_token(
    credentials: FabricCredentials, scope: Optional[str] = SYNAPSE_SPARK_CREDENTIAL_SCOPE
) -> AccessToken:
    """
    Get an Azure access token by using mspsarkutils
    Parameters
    -----------
    credentials: FabricCredentials
        Credentials.
    Returns
    -------
    out : AccessToken
        The access token.
    """
    from notebookutils import mssparkutils

    aad_token = mssparkutils.credentials.getToken(scope)
    expires_on = int(time.time() + 4500.0)
    token = AccessToken(
        token=aad_token,
        expires_on=expires_on,
    )
    return token


def get_fabric_notebook_access_token(
    credentials: FabricCredentials, scope: Optional[str] = FABRIC_NOTEBOOK_CREDENTIAL_SCOPE
) -> AccessToken:
    """
    Get an Azure access token by using notebookutils. Works in both Fabric pyspark and python notebooks.
    Parameters
    -----------
    credentials: FabricCredentials
        Credentials.
    Returns
    -------
    out : AccessToken
        The access token.
    """
    import notebookutils

    aad_token = notebookutils.credentials.getToken(
        FABRIC_NOTEBOOK_CREDENTIAL_SCOPE)
    expires_on = int(time.time() + 4500.0)
    token = AccessToken(
        token=aad_token,
        expires_on=expires_on,
    )
    return token


def get_cli_access_token(
    credentials: FabricCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricConnectionManager
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    token = AzureCliCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_auto_access_token(
    credentials: FabricCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
    """
    Get an Azure access token automatically through azure-identity

    Parameters
    -----------
    credentials: FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    token = DefaultAzureCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_environment_access_token(
    credentials: FabricCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
    """
    Get an Azure access token by reading environment variables

    Parameters
    -----------
    credentials: FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    token = EnvironmentCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


AZURE_AUTH_FUNCTIONS: Mapping[str, AZURE_AUTH_FUNCTION_TYPE] = {
    "cli": get_cli_access_token,
    "auto": get_auto_access_token,
    "environment": get_environment_access_token,
    "synapsespark": get_synapse_spark_access_token,
    "fabricnotebook": get_fabric_notebook_access_token,
}


def get_token_attrs_before(credentials: FabricCredentials, backend: DriverBackend) -> Dict:
    """
    Get the authentication attributes for pyodbc backend.

    This function is only used when the backend requires token byte conversion
    (i.e., pyodbc). For mssql-python, authentication is handled in the
    connection string.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.
    backend : DriverBackend
        The active driver backend.

    Returns
    -------
    Dict
        The pyodbc attributes for authentication, or empty dict for mssql-python.
    """
    # mssql-python handles auth in connection string, no attrs_before needed
    if not backend.requires_token_bytes():
        return {}

    global _TOKEN
    sql_copt_ss_access_token = 1256  # ODBC constant for access token
    MAX_REMAINING_TIME = 300

    if credentials.authentication.lower() in AZURE_AUTH_FUNCTIONS:
        if not _TOKEN or (_TOKEN.expires_on - time.time() < MAX_REMAINING_TIME):
            _TOKEN = AZURE_AUTH_FUNCTIONS[credentials.authentication.lower()](
                credentials, AZURE_CREDENTIAL_SCOPE
            )
        return {sql_copt_ss_access_token: convert_access_token_to_mswindows_byte_string(_TOKEN)}

    if credentials.authentication.lower() == "activedirectoryaccesstoken":
        if credentials.access_token is None or credentials.access_token_expires_on is None:
            raise ValueError(
                "Access token and access token expiry are required for ActiveDirectoryAccessToken authentication."
            )
        _TOKEN = AccessToken(
            token=credentials.access_token,
            expires_on=int(
                time.time() + 4500.0
                if credentials.access_token_expires_on == 0
                else credentials.access_token_expires_on
            ),
        )
        return {sql_copt_ss_access_token: convert_access_token_to_mswindows_byte_string(_TOKEN)}

    return {}


# Keep old function name for backwards compatibility
def get_pyodbc_attrs_before_credentials(credentials: FabricCredentials) -> Dict:
    """
    Get the pyodbc attributes for authentication.

    .. deprecated::
        Use get_token_attrs_before() instead.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    Dict
        The pyodbc attributes for authentication.
    """
    # For backwards compatibility, create a simple mock that behaves like PyodbcBackend
    # without actually importing pyodbc (which may not be installed)
    class _PyodbcBackendStub:
        def requires_token_bytes(self) -> bool:
            return True

    return get_token_attrs_before(credentials, _PyodbcBackendStub())


def bool_to_connection_string_arg(key: str, value: bool) -> str:
    """
    Convert a boolean to a connection string argument.

    Parameters
    ----------
    key : str
        The key to use in the connection string.
    value : bool
        The boolean to convert.

    Returns
    -------
    out : str
        The connection string argument.
    """
    return f'{key}={"Yes" if value else "No"}'


def byte_array_to_datetime(value: bytes) -> dt.datetime:
    """
    Converts a DATETIMEOFFSET byte array to a timezone-aware datetime object

    Parameters
    ----------
    value : buffer
        A binary value conforming to SQL_SS_TIMESTAMPOFFSET_STRUCT

    Returns
    -------
    out : datetime

    Source
    ------
    SQL_SS_TIMESTAMPOFFSET datatype and SQL_SS_TIMESTAMPOFFSET_STRUCT layout:
    https://learn.microsoft.com/sql/relational-databases/native-client-odbc-date-time/data-type-support-for-odbc-date-and-time-improvements
    """
    # unpack 20 bytes of data into a tuple of 9 values
    tup = struct.unpack("<6hI2h", value)

    # construct a datetime object
    return dt.datetime(
        year=tup[0],
        month=tup[1],
        day=tup[2],
        hour=tup[3],
        minute=tup[4],
        second=tup[5],
        microsecond=tup[6] // 1000,  # https://bugs.python.org/issue15443
        tzinfo=dt.timezone(dt.timedelta(hours=tup[7], minutes=tup[8])),
    )


def _should_run_init() -> bool:
    """Check if we should run init for this command."""
    try:
        argv_lower = [a.lower() for a in sys.argv]
        # Only run for run, build, snapshot
        return any(cmd in argv_lower for cmd in TARGET_COMMANDS)
    except Exception:
        return False


def _run_start_action(credentials: FabricCredentials) -> Dict[str, Any]:
    """Enhanced run start action with snapshot management."""
    global _snapshot_manager

    try:
        # Get credentials from dbt context
        workspace_id = credentials.workspace_id
        if workspace_id is None:
            logger.warning(
                "No workspace_id provided; skipping snapshot management.")
            return {}

        access_token = AZURE_AUTH_FUNCTIONS[credentials.authentication.lower()](
            credentials, POWER_BI_CREDENTIAL_SCOPE
        ).token
        _snapshot_manager = wh_snapshot_manager(
            workspace_id, access_token, credentials.api_url)

        if credentials.warehouse_snapshot_name is None:
            logger.info(
                "No warehouse snapshot name provided; skipping pre-run snapshot management."
            )
            return {}

        snapshot_Result = _snapshot_manager.orchestrate_snapshot_management(
            warehouse_name=credentials.database,
            snapshot_name=credentials.warehouse_snapshot_name,
        )
        return snapshot_Result
    except Exception as e:
        logger.error(f"Pre-run snapshot failed: {e}")
        raise e


def get_dbt_run_status() -> str:
    """
    Get simple status of dbt run: 'success', 'error', or 'unknown'
    """
    import json
    from pathlib import Path

    try:
        run_results_path = Path("target/run_results.json")

        if not run_results_path.exists():
            return "unknown"

        with open(run_results_path, "r") as f:
            run_results = json.load(f)

        results = run_results.get("results", [])

        if not results:
            return "unknown"

        # Check if any result has error status
        has_errors = any(result.get("status") == "error" for result in results)

        return "error" if has_errors else "success"

    except Exception:
        return "unknown"


def _run_end_action(snapshot_result: Optional[Dict[str, Any]] = None):
    """Enhanced run end action with snapshot result."""
    global _snapshot_manager

    # Get simple status
    status = get_dbt_run_status()

    if status != "success":
        logger.info(f"Skipping warehouse snapshot update: {status}")
        return

    try:
        if snapshot_result and _snapshot_manager is not None:
            print(
                "Updating warehouse snapshot timestamp at end of run...",
                snapshot_result["displayName"],
                snapshot_result["snapshot_id"],
            )
            _snapshot_manager.update_warehouse_snapshot(
                snapshot_id=snapshot_result["snapshot_id"])
    except Exception as e:
        logger.error(f"Post-run action failed: {e}")


class FabricConnectionManager(SQLConnectionManager):
    TYPE = "fabric"
    _backend: Optional[DriverBackend] = None
    _backend_lock = threading.Lock()

    @classmethod
    def get_backend(cls, credentials: FabricCredentials) -> DriverBackend:
        """Get the driver backend for this connection (thread-safe)."""
        with cls._backend_lock:
            if cls._backend is None:
                effective_backend = get_effective_driver_backend(
                    credentials.driver_backend)
                cls._backend = get_cached_driver_backend(effective_backend)

                # Emit deprecation warning if driver field is set but using mssql-python
                if credentials.driver and cls._backend.name == "mssql-python":
                    logger.warning(
                        "DEPRECATION: The 'driver' field is ignored when using mssql-python backend. "
                        "Remove 'driver' from your profile to silence this warning."
                    )
            return cls._backend

    @contextmanager
    def exception_handler(self, sql):
        # Get backend error types dynamically
        credentials = self.profile.credentials if hasattr(
            self, 'profile') else None
        if credentials:
            backend = self.get_backend(credentials)
            database_error = backend.get_database_error()
            error_types = backend.get_error_types()
        else:
            # Fallback for when we don't have credentials context
            database_error = Exception
            error_types = (Exception,)

        try:
            yield

        except error_types as e:
            if isinstance(e, database_error):
                logger.debug("Database error: {}".format(str(e)))

                try:
                    # attempt to release the connection
                    self.release()
                except Exception:
                    logger.debug("Failed to release connection!")

                raise dbt_common.exceptions.DbtDatabaseError(
                    str(e).strip()) from e
            raise

        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e)

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        # Get the driver backend
        backend = cls.get_backend(credentials)
        logger.debug(f"Using driver backend: {backend.name}")

        # Determine UID/PWD based on authentication type
        uid = None
        pwd = None
        if credentials.authentication == "ActiveDirectoryPassword":
            uid = credentials.UID
            pwd = credentials.PWD
        elif credentials.authentication == "ActiveDirectoryServicePrincipal":
            uid = credentials.client_id
            pwd = credentials.client_secret
        elif credentials.authentication == "ActiveDirectoryInteractive":
            uid = credentials.UID

        # Build connection string using backend
        plugin_version = __version__.version
        application_name = f"dbt-{credentials.type}/{plugin_version}"

        con_str_concat = backend.build_connection_string(
            host=credentials.host,
            database=credentials.database,
            authentication=credentials.authentication,
            encrypt=credentials.encrypt,
            trust_cert=credentials.trust_cert,
            application_name=application_name,
            trace_flag=credentials.trace_flag,
            driver=credentials.driver,
            uid=uid,
            pwd=pwd,
            windows_login=credentials.windows_login,
        )

        # Build display string (mask password)
        con_str_display = con_str_concat
        if pwd:
            con_str_display = con_str_concat.replace(pwd, "***")

        # Get retryable exceptions from backend
        # InterfaceError is already included by both backends for AAD auth scenarios
        retryable_exceptions = list(backend.get_retryable_exceptions())

        def connect():
            logger.debug(f"Using connection string: {con_str_display}")

            # Get token attrs for pyodbc backend
            attrs_before = get_token_attrs_before(credentials, backend)

            handle = backend.connect(
                connection_string=con_str_concat,
                timeout=credentials.login_timeout,
                autocommit=True,
                attrs_before=attrs_before,
            )
            handle.timeout = credentials.query_timeout
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

        # Open the connection (with retries) and capture the returned Connection.
        conn = cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

        # Simple one-time init with command detection
        if _should_run_init():
            global _init_done
            with _init_lock:
                if not _init_done:
                    try:
                        result = _run_start_action(credentials)
                        atexit.register(lambda: _run_end_action(result))
                    except Exception as e:
                        logger.debug("Failed to run init actions", e)
                    _init_done = True

        return conn

    def cancel(self, connection: Connection):
        logger.debug("Cancel query")

    def add_begin_query(self):
        # return self.add_query('BEGIN TRANSACTION', auto_begin=False)
        pass

    def add_commit_query(self):
        # return self.add_query('COMMIT TRANSACTION', auto_begin=False)
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[Type[Exception], ...] = (),
        retry_limit: int = 2,
    ) -> Tuple[Connection, Any]:
        """
        Retry function encapsulated here to avoid commitment to some
        user-facing interface. Right now, Redshift commits to a 1 second
        retry timeout so this serves as a default.
        """

        def _execute_query_with_retry(
            cursor: Any,
            sql: str,
            bindings: Optional[Any],
            retryable_exceptions: Tuple[Type[Exception], ...],
            retry_limit: int,
            attempt: int,
        ):
            """
            A success sees the try exit cleanly and avoid any recursive
            retries. Failure begins a sleep and retry routine.
            """
            try:
                # pyodbc does not handle a None type binding!
                if bindings is None:
                    cursor.execute(sql)
                else:
                    bindings = [
                        binding if not isinstance(
                            binding, dt.datetime) else binding.isoformat()
                        for binding in bindings
                    ]
                    cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                # Cease retries and fail when limit is hit.
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        message=f"Got a retryable error {type(e)}. {retry_limit-attempt} retries left. Retrying in 1 second.\nError:\n{e}"
                    )
                )
                time.sleep(1)

                return _execute_query_with_retry(
                    cursor=cursor,
                    sql=sql,
                    bindings=bindings,
                    retryable_exceptions=retryable_exceptions,
                    retry_limit=retry_limit,
                    attempt=attempt + 1,
                )

        connection = self.get_thread_connection()

        if auto_begin and connection.transaction_open is False:
            self.begin()

        fire_event(
            ConnectionUsed(
                conn_type=self.TYPE,
                conn_name=cast_to_str(connection.name),
                node_info=get_node_info(),
            )
        )

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name), sql=log_sql, node_info=get_node_info()
                )
            )

            pre = time.time()

            cursor = connection.handle.cursor()
            credentials = self.get_credentials(connection.credentials)

            _execute_query_with_retry(
                cursor=cursor,
                sql=sql,
                bindings=bindings,
                retryable_exceptions=retryable_exceptions,
                retry_limit=credentials.retries if credentials.retries > 3 else retry_limit,
                attempt=1,
            )

            # convert DATETIMEOFFSET binary structures to datetime objects
            # https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
            backend = self.get_backend(credentials)
            backend.add_output_converter(
                connection.handle, -155, byte_array_to_datetime)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre)),
                    node_info=get_node_info(),
                )
            )

            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials: FabricCredentials) -> FabricCredentials:
        return credentials

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # message = str(cursor.statusmessage)
        message = "OK"
        rows = cursor.rowcount
        # status_message_parts = message.split() if message is not None else []
        # status_messsage_strings = [
        #    part
        #    for part in status_message_parts
        #    if not part.isdigit()
        # ]
        # code = ' '.join(status_messsage_strings)
        return AdapterResponse(
            _message=message,
            # code=code,
            rows_affected=rows,
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: str) -> str:
        data_type = str(type_code)[str(type_code).index(
            "'") + 1: str(type_code).rindex("'")]
        return datatypes[data_type]

    def execute(
        self, sql: str, auto_begin: bool = True, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            # Get the result of the first non-empty result set (if any)
            while cursor.description is None:
                if not cursor.nextset():
                    break
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        # Step through all result sets so we process all errors
        while cursor.nextset():
            pass
        return response, table
