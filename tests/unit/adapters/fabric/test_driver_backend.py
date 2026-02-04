"""Unit tests for driver_backend module."""

import os
import pytest
from unittest.mock import patch, MagicMock
import sys


class TestConvertBytesToMswindowsByteString:
    """Tests for convert_bytes_to_mswindows_byte_string function."""

    def test_converts_bytes_correctly(self):
        """Verify bytes are converted to MS Windows byte string format."""
        from dbt.adapters.fabric.driver_backend import convert_bytes_to_mswindows_byte_string

        result = convert_bytes_to_mswindows_byte_string(b"test")

        # Result should be: 4-byte length prefix + UTF-16LE encoded bytes
        assert isinstance(result, bytes)
        assert len(result) > 4  # At least the length prefix

    def test_empty_bytes(self):
        """Empty bytes should produce length prefix only."""
        from dbt.adapters.fabric.driver_backend import convert_bytes_to_mswindows_byte_string

        result = convert_bytes_to_mswindows_byte_string(b"")
        # 4 bytes for length (0) prefix
        assert result == b"\x00\x00\x00\x00"

    def test_unicode_string_bytes(self):
        """Unicode string bytes should be properly converted."""
        from dbt.adapters.fabric.driver_backend import convert_bytes_to_mswindows_byte_string

        # UTF-8 encoded "hello"
        result = convert_bytes_to_mswindows_byte_string(b"hello")
        # Length prefix (4 bytes) + 10 bytes (5 chars * 2 for UTF-16LE)
        assert len(result) == 14

    def test_long_token_bytes(self):
        """Long token-like bytes should be properly converted."""
        from dbt.adapters.fabric.driver_backend import convert_bytes_to_mswindows_byte_string

        # Simulate a token-like string
        token = b"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1uQ19"
        result = convert_bytes_to_mswindows_byte_string(token)

        # Should have length prefix + doubled bytes for UTF-16LE
        expected_content_length = len(token) * 2
        assert len(result) == 4 + expected_content_length


class TestGetDriverBackend:
    """Tests for get_driver_backend() factory function."""

    def test_auto_prefers_mssql_python_when_available_and_python_310_plus(self):
        """Auto mode should return MssqlPythonBackend when mssql_python is installed and Python >= 3.10."""
        from dbt.adapters.fabric.driver_backend import get_driver_backend
        import dbt.adapters.fabric.driver_backend as db_module

        # Reset cached backend
        db_module._active_backend = None

        with patch.dict(sys.modules, {"mssql_python": MagicMock()}):
            if sys.version_info >= (3, 10):
                backend = get_driver_backend("auto")
                assert backend.name == "mssql-python"

    def test_auto_falls_back_to_pyodbc_when_mssql_python_unavailable(self):
        """Auto mode should return PyodbcBackend when mssql_python import fails."""
        from dbt.adapters.fabric.driver_backend import get_driver_backend, _active_backend
        import dbt.adapters.fabric.driver_backend as db_module

        # Reset cached backend
        db_module._active_backend = None

        # Mock mssql_python import to fail
        with patch.dict(sys.modules, {"mssql_python": None}):
            with patch.object(
                db_module.MssqlPythonBackend,
                "__init__",
                side_effect=ImportError("No module named 'mssql_python'"),
            ):
                with patch.dict(sys.modules, {"pyodbc": MagicMock()}):
                    backend = get_driver_backend("auto")
                    assert backend.name == "pyodbc"

    def test_auto_falls_back_to_pyodbc_on_python_39(self):
        """Auto mode should return PyodbcBackend on Python 3.9."""
        from dbt.adapters.fabric.driver_backend import get_driver_backend
        import dbt.adapters.fabric.driver_backend as db_module

        # Reset cached backend
        db_module._active_backend = None

        with patch.dict(sys.modules, {"pyodbc": MagicMock()}):
            with patch.object(sys, "version_info", (3, 9, 0)):
                backend = get_driver_backend("auto")
                assert backend.name == "pyodbc"

    def test_explicit_mssql_python_raises_when_unavailable(self):
        """Explicit mssql-python should raise ImportError, not fall back."""
        from dbt.adapters.fabric.driver_backend import get_driver_backend, MssqlPythonBackend
        import dbt.adapters.fabric.driver_backend as db_module

        # Reset cached backend
        db_module._active_backend = None

        with patch.object(
            MssqlPythonBackend,
            "__init__",
            side_effect=ImportError("No module named 'mssql_python'"),
        ):
            with pytest.raises(ImportError):
                get_driver_backend("mssql-python")

    def test_explicit_pyodbc_returns_pyodbc_backend(self):
        """Explicit pyodbc should return PyodbcBackend."""
        from dbt.adapters.fabric.driver_backend import get_driver_backend
        import dbt.adapters.fabric.driver_backend as db_module

        # Reset cached backend
        db_module._active_backend = None

        with patch.dict(sys.modules, {"pyodbc": MagicMock()}):
            backend = get_driver_backend("pyodbc")
            assert backend.name == "pyodbc"

    def test_invalid_backend_raises_value_error(self):
        """Invalid driver_backend value should raise ValueError."""
        from dbt.adapters.fabric.driver_backend import get_driver_backend

        with pytest.raises(ValueError, match="Invalid driver_backend"):
            get_driver_backend("invalid-backend")


class TestGetEffectiveDriverBackend:
    """Tests for get_effective_driver_backend() function."""

    def test_env_var_takes_precedence(self):
        """Environment variable should override profile setting."""
        from dbt.adapters.fabric.driver_backend import get_effective_driver_backend

        with patch.dict("os.environ", {"DBT_FABRIC_DRIVER_BACKEND": "pyodbc"}):
            result = get_effective_driver_backend("mssql-python")
            assert result == "pyodbc"

    def test_profile_setting_used_when_no_env_var(self):
        """Profile setting should be used when env var is not set."""
        from dbt.adapters.fabric.driver_backend import get_effective_driver_backend

        with patch.dict("os.environ", {}, clear=True):
            # Ensure DBT_FABRIC_DRIVER_BACKEND is not set
            import os
            os.environ.pop("DBT_FABRIC_DRIVER_BACKEND", None)

            result = get_effective_driver_backend("pyodbc")
            assert result == "pyodbc"

    def test_default_auto_when_no_setting(self):
        """Should default to 'auto' when no setting provided."""
        from dbt.adapters.fabric.driver_backend import get_effective_driver_backend

        with patch.dict("os.environ", {}, clear=True):
            import os
            os.environ.pop("DBT_FABRIC_DRIVER_BACKEND", None)

            result = get_effective_driver_backend(None)
            assert result == "auto"

    def test_invalid_env_var_raises_value_error(self):
        """Invalid env var value should raise ValueError."""
        from dbt.adapters.fabric.driver_backend import get_effective_driver_backend

        with patch.dict("os.environ", {"DBT_FABRIC_DRIVER_BACKEND": "invalid"}):
            with pytest.raises(ValueError, match="Invalid DBT_FABRIC_DRIVER_BACKEND"):
                get_effective_driver_backend("auto")


class TestMssqlPythonBackend:
    """Tests for MssqlPythonBackend class."""

    @pytest.fixture
    def mock_mssql_python(self):
        """Create a mock mssql_python module."""
        mock_module = MagicMock()
        mock_module.Error = Exception
        mock_module.DatabaseError = Exception
        mock_module.OperationalError = Exception
        mock_module.InterfaceError = Exception
        return mock_module

    def test_connect_calls_mssql_python_connect(self, mock_mssql_python):
        """Verify connect() delegates to mssql_python.connect()."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            mock_conn = MagicMock()
            mock_mssql_python.connect.return_value = mock_conn

            result = backend.connect(
                "SERVER=test;", timeout=30, autocommit=True)

            mock_mssql_python.connect.assert_called_once_with(
                "SERVER=test;", timeout=30)
            mock_conn.setautocommit.assert_called_once_with(True)

    def test_connect_with_autocommit_false(self, mock_mssql_python):
        """Verify connect() passes autocommit=False correctly."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            mock_conn = MagicMock()
            mock_mssql_python.connect.return_value = mock_conn

            backend.connect("SERVER=test;", timeout=30, autocommit=False)

            mock_conn.setautocommit.assert_called_once_with(False)

    def test_connect_ignores_attrs_before(self, mock_mssql_python):
        """Verify connect() ignores attrs_before (mssql-python doesn't use it)."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            mock_conn = MagicMock()
            mock_mssql_python.connect.return_value = mock_conn

            backend.connect(
                "SERVER=test;",
                timeout=30,
                autocommit=True,
                attrs_before={1256: b"token"}  # Should be ignored
            )

            # attrs_before should not be passed to connect
            mock_mssql_python.connect.assert_called_once_with(
                "SERVER=test;", timeout=30)

    def test_connection_string_has_no_driver_prefix(self, mock_mssql_python):
        """Verify connection string does NOT include DRIVER=."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                driver="ODBC Driver 18",  # Should be ignored
            )

            assert "DRIVER=" not in conn_str
            assert "SERVER=test.database.fabric.microsoft.com" in conn_str

    def test_connection_string_windows_login(self, mock_mssql_python):
        """Verify Windows login sets Trusted_Connection."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="Windows",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                windows_login=True,
            )

            assert "Trusted_Connection=Yes" in conn_str

    def test_connection_string_encryption_settings(self, mock_mssql_python):
        """Verify encryption settings are included correctly."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()

            # Encrypt=Yes, Trust=No
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
            )
            assert "Encrypt=Yes" in conn_str
            assert "TrustServerCertificate=No" in conn_str

            # Encrypt=No, Trust=Yes
            conn_str2 = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=False,
                trust_cert=True,
                application_name="dbt-fabric/1.0",
            )
            assert "Encrypt=No" in conn_str2
            assert "TrustServerCertificate=Yes" in conn_str2

    def test_connection_string_retry_settings(self, mock_mssql_python):
        """Verify retry settings are included."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
            )

            assert "ConnectRetryCount=3" in conn_str
            assert "ConnectRetryInterval=10" in conn_str

    def test_sql_auth_raises_database_error(self, mock_mssql_python):
        """SQL Authentication should raise DatabaseError for Fabric."""
        mock_mssql_python.DatabaseError = ValueError  # Use a testable exception

        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()

            with pytest.raises(ValueError, match="SQL Authentication is not supported"):
                backend.build_connection_string(
                    host="test.database.fabric.microsoft.com",
                    database="testdb",
                    authentication="sql",
                    encrypt=True,
                    trust_cert=False,
                    application_name="dbt-fabric/1.0",
                )

    def test_requires_token_bytes_returns_false(self, mock_mssql_python):
        """mssql-python should not require token byte conversion."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            assert backend.requires_token_bytes() is False

    def test_set_pooling_enabled(self, mock_mssql_python):
        """Verify set_pooling calls mssql_python.pooling when enabled."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            backend.set_pooling(enabled=True, max_size=50)

            mock_mssql_python.pooling.assert_called_once_with(max_size=50)

    def test_set_pooling_disabled(self, mock_mssql_python):
        """Verify set_pooling does nothing when disabled."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            backend.set_pooling(enabled=False)

            mock_mssql_python.pooling.assert_not_called()

    def test_add_output_converter(self, mock_mssql_python):
        """Verify add_output_converter delegates to connection."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            mock_conn = MagicMock()
            mock_func = MagicMock()

            backend.add_output_converter(mock_conn, 123, mock_func)

            mock_conn.add_output_converter.assert_called_once_with(
                123, mock_func)

    def test_get_error_types(self, mock_mssql_python):
        """Verify get_error_types returns correct tuple."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            error_types = backend.get_error_types()

            assert len(error_types) == 3

    def test_get_retryable_exceptions(self, mock_mssql_python):
        """Verify get_retryable_exceptions returns correct tuple."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            exceptions = backend.get_retryable_exceptions()

            assert len(exceptions) == 2

    def test_get_database_error(self, mock_mssql_python):
        """Verify get_database_error returns correct exception type."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            db_error = backend.get_database_error()

            assert db_error is mock_mssql_python.DatabaseError


class TestPyodbcBackend:
    """Tests for PyodbcBackend class."""

    @pytest.fixture
    def mock_pyodbc(self):
        """Create a mock pyodbc module."""
        mock_module = MagicMock()
        mock_module.Error = Exception
        mock_module.DatabaseError = Exception
        mock_module.OperationalError = Exception
        mock_module.InternalError = Exception
        mock_module.InterfaceError = Exception
        mock_module.pooling = True
        return mock_module

    def test_connect_calls_pyodbc_connect(self, mock_pyodbc):
        """Verify connect() delegates to pyodbc.connect()."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            mock_conn = MagicMock()
            mock_pyodbc.connect.return_value = mock_conn

            result = backend.connect(
                "DRIVER={ODBC Driver 18};SERVER=test;",
                timeout=30,
                autocommit=True,
                attrs_before={1256: b"token"},
            )

            mock_pyodbc.connect.assert_called_once()
            call_kwargs = mock_pyodbc.connect.call_args
            assert call_kwargs[1]["attrs_before"] == {1256: b"token"}
            assert call_kwargs[1]["autocommit"] is True

    def test_connect_without_attrs_before(self, mock_pyodbc):
        """Verify connect() passes empty dict when no attrs_before."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            mock_conn = MagicMock()
            mock_pyodbc.connect.return_value = mock_conn

            backend.connect(
                "DRIVER={ODBC Driver 18};SERVER=test;",
                timeout=30,
                autocommit=True,
            )

            call_kwargs = mock_pyodbc.connect.call_args
            assert call_kwargs[1]["attrs_before"] == {}

    def test_connection_string_includes_driver_prefix(self, mock_pyodbc):
        """Verify connection string includes DRIVER=."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                driver="ODBC Driver 18 for SQL Server",
            )

            assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str
            assert "SERVER=test.database.fabric.microsoft.com" in conn_str

    def test_connection_string_default_driver(self, mock_pyodbc):
        """Verify default driver is used when not specified."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                driver=None,  # No driver specified
            )

            assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str

    def test_connection_string_trace_flag_on(self, mock_pyodbc):
        """Verify trace flag is included when enabled."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                trace_flag=True,
            )

            assert "SQL_ATTR_TRACE=SQL_OPT_TRACE_ON" in conn_str

    def test_connection_string_trace_flag_off(self, mock_pyodbc):
        """Verify trace flag is off when disabled."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                trace_flag=False,
            )

            assert "SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF" in conn_str

    def test_connection_string_windows_login(self, mock_pyodbc):
        """Verify Windows login is handled correctly."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="Windows",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                windows_login=True,
            )

            assert "trusted_connection=Yes" in conn_str

    def test_connection_string_pooling(self, mock_pyodbc):
        """Verify pooling is enabled in connection string."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryServicePrincipal",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
            )

            assert "Pooling=true" in conn_str

    def test_sql_auth_raises_database_error(self, mock_pyodbc):
        """SQL Authentication should raise DatabaseError for Fabric."""
        mock_pyodbc.DatabaseError = ValueError

        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()

            with pytest.raises(ValueError, match="SQL Authentication is not supported"):
                backend.build_connection_string(
                    host="test.database.fabric.microsoft.com",
                    database="testdb",
                    authentication="sql",
                    encrypt=True,
                    trust_cert=False,
                    application_name="dbt-fabric/1.0",
                )

    def test_requires_token_bytes_returns_true(self, mock_pyodbc):
        """pyodbc should require token byte conversion."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            assert backend.requires_token_bytes() is True

    def test_set_pooling(self, mock_pyodbc):
        """Verify set_pooling sets pyodbc.pooling."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            backend.set_pooling(enabled=True)

            assert mock_pyodbc.pooling is True

            backend.set_pooling(enabled=False)
            assert mock_pyodbc.pooling is False

    def test_add_output_converter(self, mock_pyodbc):
        """Verify add_output_converter delegates to connection."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            mock_conn = MagicMock()
            mock_func = MagicMock()

            backend.add_output_converter(mock_conn, 456, mock_func)

            mock_conn.add_output_converter.assert_called_once_with(
                456, mock_func)

    def test_get_error_types(self, mock_pyodbc):
        """Verify get_error_types returns correct tuple."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            error_types = backend.get_error_types()

            assert len(error_types) == 3

    def test_get_retryable_exceptions(self, mock_pyodbc):
        """Verify get_retryable_exceptions returns correct tuple."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            exceptions = backend.get_retryable_exceptions()

            assert len(exceptions) == 2

    def test_get_database_error(self, mock_pyodbc):
        """Verify get_database_error returns correct exception type."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            db_error = backend.get_database_error()

            assert db_error is mock_pyodbc.DatabaseError

    def test_connection_string_active_directory_password_credentials(self, mock_pyodbc):
        """Verify UID/PWD are wrapped in braces for ActiveDirectoryPassword."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryPassword",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                uid="user@domain.com",
                pwd="mypassword",
            )

            assert "UID={user@domain.com}" in conn_str
            assert "PWD={mypassword}" in conn_str

    def test_connection_string_active_directory_interactive(self, mock_pyodbc):
        """Verify UID is included for ActiveDirectoryInteractive."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication="ActiveDirectoryInteractive",
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                uid="user@domain.com",
            )

            assert "UID={user@domain.com}" in conn_str
            assert "PWD=" not in conn_str


class TestConvertBytesToMswindowsByteString:
    """Tests for convert_bytes_to_mswindows_byte_string function."""

    def test_converts_bytes_correctly(self):
        """Verify bytes are converted to MS Windows byte string format."""
        from dbt.adapters.fabric.driver_backend import convert_bytes_to_mswindows_byte_string

        result = convert_bytes_to_mswindows_byte_string(b"test")

        # Result should be: 4-byte length prefix + UTF-16LE encoded bytes
        assert isinstance(result, bytes)
        assert len(result) > 4  # At least the length prefix


class TestBackendAuthentication:
    """Tests for authentication across both backends."""

    @pytest.fixture
    def mock_mssql_python(self):
        mock_module = MagicMock()
        mock_module.Error = Exception
        mock_module.DatabaseError = Exception
        mock_module.OperationalError = Exception
        mock_module.InterfaceError = Exception
        return mock_module

    @pytest.fixture
    def mock_pyodbc(self):
        mock_module = MagicMock()
        mock_module.Error = Exception
        mock_module.DatabaseError = Exception
        mock_module.OperationalError = Exception
        mock_module.InternalError = Exception
        mock_module.InterfaceError = Exception
        mock_module.pooling = True
        return mock_module

    @pytest.mark.parametrize("auth_type", [
        "ActiveDirectoryServicePrincipal",
        "ActiveDirectoryPassword",
        "ActiveDirectoryInteractive",
    ])
    def test_active_directory_auth_in_connection_string_mssql_python(
        self, mock_mssql_python, auth_type
    ):
        """ActiveDirectory auth methods should be in connection string for mssql-python."""
        with patch.dict(sys.modules, {"mssql_python": mock_mssql_python}):
            from dbt.adapters.fabric.driver_backend import MssqlPythonBackend

            backend = MssqlPythonBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication=auth_type,
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                uid="test_uid",
                pwd="test_pwd",
            )

            assert f"Authentication={auth_type}" in conn_str

    @pytest.mark.parametrize("auth_type", [
        "ActiveDirectoryServicePrincipal",
        "ActiveDirectoryPassword",
        "ActiveDirectoryInteractive",
    ])
    def test_active_directory_auth_in_connection_string_pyodbc(
        self, mock_pyodbc, auth_type
    ):
        """ActiveDirectory auth methods should be in connection string for pyodbc."""
        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc}):
            from dbt.adapters.fabric.driver_backend import PyodbcBackend

            backend = PyodbcBackend()
            conn_str = backend.build_connection_string(
                host="test.database.fabric.microsoft.com",
                database="testdb",
                authentication=auth_type,
                encrypt=True,
                trust_cert=False,
                application_name="dbt-fabric/1.0",
                driver="ODBC Driver 18 for SQL Server",
                uid="test_uid",
                pwd="test_pwd",
            )

            assert f"Authentication={auth_type}" in conn_str


class TestCachedDriverBackend:
    """Tests for get_cached_driver_backend() function."""

    def test_caches_backend_instance(self):
        """Verify backend is cached across calls."""
        import dbt.adapters.fabric.driver_backend as db_module

        # Reset cache
        db_module._active_backend = None

        with patch.dict(sys.modules, {"pyodbc": MagicMock()}):
            backend1 = db_module.get_cached_driver_backend("pyodbc")
            backend2 = db_module.get_cached_driver_backend("pyodbc")

            assert backend1 is backend2

    def test_cache_reset_on_different_preference(self):
        """Verify cache is reset when preference changes."""
        import dbt.adapters.fabric.driver_backend as db_module

        mock_mssql = MagicMock()
        mock_mssql.Error = Exception
        mock_mssql.DatabaseError = Exception
        mock_mssql.OperationalError = Exception
        mock_mssql.InterfaceError = Exception

        mock_pyodbc = MagicMock()
        mock_pyodbc.Error = Exception
        mock_pyodbc.DatabaseError = Exception
        mock_pyodbc.OperationalError = Exception
        mock_pyodbc.InternalError = Exception

        # Reset cache
        db_module._active_backend = None

        with patch.dict(sys.modules, {"pyodbc": mock_pyodbc, "mssql_python": mock_mssql}):
            backend1 = db_module.get_cached_driver_backend("pyodbc")
            assert backend1.name == "pyodbc"

            # Change preference - should get new backend
            backend2 = db_module.get_cached_driver_backend("mssql-python")
            assert backend2.name == "mssql-python"
            assert backend1 is not backend2
