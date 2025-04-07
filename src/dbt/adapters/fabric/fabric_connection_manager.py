import datetime as dt
import struct
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple, Union

import agate
import dbt_common.exceptions
import pyodbc
from dbt_common.clients.agate_helper import empty_table
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.utils.casting import cast_to_str

from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.fabric import __version__
from dbt.adapters.fabric.fabric_credentials import FabricCredentials
from dbt.adapters.fabric.fabric_token_provider import FabricTokenProvider
from dbt.adapters.sql import SQLConnectionManager

logger = AdapterLogger("fabric")

# https://github.com/mkleehammer/pyodbc/wiki/Data-Types
datatypes = {
    "str": "varchar",
    "uuid.UUID": "uniqueidentifier",
    "uuid": "uniqueidentifier",
    "float": "bigint",
    "int": "int",
    "bytes": "varbinary",
    "bytearray": "varbinary",
    "bool": "bit",
    "datetime.date": "date",
    "datetime.datetime": "datetime2(6)",
    "datetime.time": "time",
    "decimal.Decimal": "decimal",
}


def get_pyodbc_attrs_before_accesstoken(accessToken: str) -> Dict:
    """
    Get the pyodbc attrs before.

    Parameters
    ----------
    credentials : Access Token for Integration Tests
        Credentials.

    Returns
    -------
    out : Dict
        The pyodbc attrs before.

    Source
    ------
    Authentication for SQL server with an access token:
    https://docs.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory?view=sql-server-ver15#authenticating-with-an-access-token
    """

    access_token_utf16 = accessToken.encode("utf-16-le")
    token_struct = struct.pack(
        f"<I{len(access_token_utf16)}s", len(access_token_utf16), access_token_utf16
    )
    sql_copt_ss_access_token = 1256  # see source in docstring
    attrs_before = {sql_copt_ss_access_token: token_struct}

    return attrs_before


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
    return f"{key}={'Yes' if value else 'No'}"


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


class FabricConnectionManager(SQLConnectionManager):
    TYPE = "fabric"
    _fabric_token_provider = None

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except pyodbc.DatabaseError as e:
            logger.debug("Database error: {}".format(str(e)))

            try:
                # attempt to release the connection
                self.release()
            except pyodbc.Error:
                logger.debug("Failed to release connection!")

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

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
    def get_fabric_token_provider(cls, credentials: FabricCredentials) -> FabricTokenProvider:
        if cls._fabric_token_provider is None:
            cls._fabric_token_provider = FabricTokenProvider(credentials)
        return cls._fabric_token_provider

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        con_str = [f"DRIVER={{{credentials.driver}}}"]

        if "\\" in credentials.host:
            # If there is a backslash \ in the host name, the host is a
            # SQL Server named instance. In this case then port number has to be omitted.
            con_str.append(f"SERVER={credentials.host}")
        else:
            con_str.append(f"SERVER={credentials.host}")

        con_str.append(f"Database={credentials.database}")
        con_str.append("Pooling=true")

        # Enabling trace flag
        if credentials.trace_flag:
            con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")
        else:
            con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF")

        assert credentials.authentication is not None

        if "ActiveDirectory" in credentials.authentication:
            con_str.append(f"Authentication={credentials.authentication}")

            if credentials.authentication == "ActiveDirectoryPassword":
                con_str.append(f"UID={{{credentials.UID}}}")
                con_str.append(f"PWD={{{credentials.PWD}}}")
            if credentials.authentication == "ActiveDirectoryServicePrincipal":
                con_str.append(f"UID={{{credentials.client_id}}}")
                con_str.append(f"PWD={{{credentials.client_secret}}}")
            elif credentials.authentication == "ActiveDirectoryInteractive":
                con_str.append(f"UID={{{credentials.UID}}}")

        elif credentials.windows_login:
            con_str.append("trusted_connection=Yes")
        elif credentials.authentication == "sql":
            raise pyodbc.DatabaseError("SQL Authentication is not supported by Microsoft Fabric")

        # https://docs.microsoft.com/en-us/sql/relational-databases/native-client/features/using-encryption-without-validation?view=sql-server-ver15
        assert credentials.encrypt is not None
        assert credentials.trust_cert is not None

        con_str.append(bool_to_connection_string_arg("encrypt", credentials.encrypt))
        con_str.append(
            bool_to_connection_string_arg("TrustServerCertificate", credentials.trust_cert)
        )

        plugin_version = __version__.version
        application_name = f"dbt-{credentials.type}/{plugin_version}"
        con_str.append(f"APP={application_name}")

        try:
            con_str.append(f"ConnectRetryCount={credentials.retries}")
            con_str.append("ConnectRetryInterval=10")

        except Exception as e:
            logger.debug(
                "Retry count should be a integer value. Skipping retries in the connection string.",
                str(e),
            )

        con_str_concat = ";".join(con_str)

        index = []
        for i, elem in enumerate(con_str):
            if "pwd=" in elem.lower():
                index.append(i)

        if len(index) != 0:
            con_str[index[0]] = "PWD=***"

        con_str_display = ";".join(con_str)

        retryable_exceptions = [  # https://github.com/mkleehammer/pyodbc/wiki/Exceptions
            pyodbc.InternalError,  # not used according to docs, but defined in PEP-249
            pyodbc.OperationalError,
            pyodbc.InterfaceError,
        ]

        def connect():
            logger.debug(f"Using connection string: {con_str_display}")
            pyodbc.pooling = True
            attrs_before = cls.get_fabric_token_provider(credentials).get_pyodbc_attributes()

            handle = pyodbc.connect(
                con_str_concat,
                attrs_before=attrs_before,
                autocommit=True,
                timeout=credentials.login_timeout,
            )
            handle.timeout = credentials.query_timeout
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection: Connection):
        pass

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
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

            # pyodbc does not handle a None type binding!
            if bindings is None:
                cursor.execute(sql)
            else:
                bindings = [
                    binding if not isinstance(binding, dt.datetime) else binding.isoformat()
                    for binding in bindings
                ]
                cursor.execute(sql, bindings)

            # convert DATETIMEOFFSET binary structures to datetime ojbects
            # https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
            connection.handle.add_output_converter(-155, byte_array_to_datetime)

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
    def get_response(cls, cursor: pyodbc.Cursor) -> AdapterResponse:
        message = "\n".join(msg[1] for msg in cursor.messages) if cursor.messages else ""
        return AdapterResponse(
            _message=message,
            rows_affected=cursor.rowcount,
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[str, str]) -> str:
        data_type = str(type_code)[str(type_code).index("'") + 1 : str(type_code).rindex("'")]
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
