# Plan: Migrate dbt-fabric from pyodbc to mssql-python driver

Microsoft's dbt-fabric adapter currently uses **pyodbc** with ODBC Driver 18 for SQL Server. Migrating to Microsoft's new **mssql-python** driver (native Python driver with no external ODBC dependency) will simplify installation, improve cross-platform support, and align with Microsoft's recommended tooling. 

**Approach**: Implement a driver abstraction layer that defaults to **mssql-python** with automatic fallback to **pyodbc**. This avoids a breaking major version bump while enabling gradual migration.

---

## Architecture: Driver Abstraction Layer

Create a new `driver_backend.py` module that abstracts the connection interface:

```python
# dbt/adapters/fabric/driver_backend.py

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class DriverBackend(ABC):
    """Abstract base class for SQL Server driver backends."""
    
    @abstractmethod
    def connect(self, connection_string: str, timeout: int, autocommit: bool) -> Any:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def set_pooling(self, enabled: bool, max_size: int = 100) -> None:
        """Configure connection pooling."""
        pass
    
    @abstractmethod
    def add_output_converter(self, connection: Any, sql_type: int, func: callable) -> None:
        """Register output type converter."""
        pass
    
    @abstractmethod
    def get_error_types(self) -> tuple:
        """Return tuple of exception types for error handling."""
        pass


class MssqlPythonBackend(DriverBackend):
    """mssql-python driver backend (preferred)."""
    
    def __init__(self):
        import mssql_python
        self._driver = mssql_python
    
    def connect(self, connection_string: str, timeout: int, autocommit: bool) -> Any:
        conn = self._driver.connect(connection_string, timeout=timeout)
        conn.setautocommit(autocommit)
        return conn
    
    # ... etc


class PyodbcBackend(DriverBackend):
    """pyodbc driver backend (fallback)."""
    
    def __init__(self):
        import pyodbc
        self._driver = pyodbc
    
    def connect(self, connection_string: str, timeout: int, autocommit: bool, 
                attrs_before: Optional[Dict] = None) -> Any:
        return self._driver.connect(
            connection_string,
            attrs_before=attrs_before,
            autocommit=autocommit,
            timeout=timeout
        )
    
    # ... etc


def get_driver_backend(preferred: str = "auto") -> DriverBackend:
    """
    Get the appropriate driver backend.
    
    Args:
        preferred: "mssql-python", "pyodbc", or "auto" (default)
                   "auto" tries mssql-python first, falls back to pyodbc
    
    Returns:
        DriverBackend instance
    """
    if preferred == "pyodbc":
        return PyodbcBackend()
    
    if preferred == "mssql-python":
        return MssqlPythonBackend()
    
    # Auto mode: try mssql-python first
    try:
        return MssqlPythonBackend()
    except ImportError:
        return PyodbcBackend()
```

---

## Steps

1. **Update dependencies in [pyproject.toml](pyproject.toml) and [setup.py](setup.py)**: Add `mssql-python>=1.3.0` as the primary dependency, keep `pyodbc>=5.2.0` as an optional fallback extra. Support Python 3.9+ (pyodbc fallback) with 3.10+ recommended (mssql-python).

   ```toml
   # pyproject.toml
   dependencies = [
       "mssql-python>=1.3.0; python_version>='3.10'",
       "azure-identity>=1.14.0",
       # ...
   ]
   
   [project.optional-dependencies]
   pyodbc = ["pyodbc>=5.2.0"]
   legacy = ["pyodbc>=5.2.0"]  # For Python 3.9 or platforms without mssql-python
   ```

2. **Create driver abstraction in [driver_backend.py](dbt/adapters/fabric/driver_backend.py)** (new file): Implement `DriverBackend` ABC with `MssqlPythonBackend` and `PyodbcBackend` concrete classes, plus `get_driver_backend()` factory function.

3. **Add `driver_backend` credential field in [fabric_credentials.py](dbt/adapters/fabric/fabric_credentials.py)**: Add new optional field `driver_backend: str = "auto"` (values: `"auto"`, `"mssql-python"`, `"pyodbc"`). Deprecate but keep `driver` field for pyodbc fallback compatibility.

4. **Refactor [fabric_connection_manager.py](dbt/adapters/fabric/fabric_connection_manager.py)**: Replace direct pyodbc usage with `DriverBackend` interface. Keep `attrs_before` token logic only for `PyodbcBackend`. Build appropriate connection string based on active backend. Route exceptions through backend's `get_error_types()`.

5. **Add unit tests for driver backends** — see [Test Cases](#test-cases) section below.

6. **Update existing tests** — see [Test Cases](#test-cases) section below.

7. **Update documentation** — see [Documentation Updates](#documentation-updates) section below.

8. **Update CI/CD in [devops/CI.Dockerfile](devops/CI.Dockerfile) and [devops/server.Dockerfile](devops/server.Dockerfile)**: Keep ODBC driver installation for pyodbc fallback testing, add separate test matrix for mssql-python-only and pyodbc-only runs.

---

## Test Cases

### New File: [test_driver_backend.py](tests/unit/adapters/fabric/test_driver_backend.py)

```python
# tests/unit/adapters/fabric/test_driver_backend.py

import pytest
from unittest.mock import patch, MagicMock

class TestGetDriverBackend:
    """Tests for get_driver_backend() factory function."""
    
    def test_auto_prefers_mssql_python_when_available(self):
        """Auto mode should return MssqlPythonBackend when mssql_python is installed."""
        # Arrange/Act/Assert: verify MssqlPythonBackend is returned
        
    def test_auto_falls_back_to_pyodbc_when_mssql_python_unavailable(self):
        """Auto mode should return PyodbcBackend when mssql_python import fails."""
        # Mock mssql_python import to raise ImportError
        
    def test_explicit_mssql_python_raises_when_unavailable(self):
        """Explicit mssql-python should raise ImportError, not fall back."""
        
    def test_explicit_pyodbc_returns_pyodbc_backend(self):
        """Explicit pyodbc should return PyodbcBackend."""
        
    def test_invalid_backend_raises_value_error(self):
        """Invalid driver_backend value should raise ValueError."""


class TestMssqlPythonBackend:
    """Tests for MssqlPythonBackend class."""
    
    @patch('mssql_python.connect')
    def test_connect_calls_mssql_python_connect(self, mock_connect):
        """Verify connect() delegates to mssql_python.connect()."""
        
    @patch('mssql_python.connect')
    def test_connect_sets_autocommit(self, mock_connect):
        """Verify autocommit is set on connection."""
        
    @patch('mssql_python.pooling')
    def test_set_pooling_configures_pool(self, mock_pooling):
        """Verify set_pooling() calls mssql_python.pooling()."""
        
    def test_connection_string_has_no_driver_prefix(self):
        """Verify connection string does NOT include DRIVER=."""
        
    def test_get_error_types_returns_mssql_python_exceptions(self):
        """Verify correct exception types are returned."""


class TestPyodbcBackend:
    """Tests for PyodbcBackend class."""
    
    @patch('pyodbc.connect')
    def test_connect_calls_pyodbc_connect(self, mock_connect):
        """Verify connect() delegates to pyodbc.connect()."""
        
    @patch('pyodbc.connect')
    def test_connect_passes_attrs_before(self, mock_connect):
        """Verify attrs_before is passed for token auth."""
        
    def test_connection_string_includes_driver_prefix(self):
        """Verify connection string includes DRIVER=."""
        
    def test_get_error_types_returns_pyodbc_exceptions(self):
        """Verify correct exception types are returned."""


class TestBackendAuthentication:
    """Tests for authentication across both backends."""
    
    @pytest.mark.parametrize("backend_type", ["mssql-python", "pyodbc"])
    def test_service_principal_auth(self, backend_type):
        """ServicePrincipal auth works on both backends."""
        
    @pytest.mark.parametrize("backend_type", ["mssql-python", "pyodbc"])
    def test_cli_auth(self, backend_type):
        """CLI (interactive) auth works on both backends."""
        
    @pytest.mark.parametrize("backend_type", ["mssql-python", "pyodbc"])
    def test_environment_auth(self, backend_type):
        """Environment credential auth works on both backends."""
        
    @pytest.mark.parametrize("backend_type", ["mssql-python", "pyodbc"])
    def test_auto_auth(self, backend_type):
        """Auto (DefaultAzureCredential) auth works on both backends."""
```

### Updates to [test_sql_server_connection_manager.py](tests/unit/adapters/fabric/test_sql_server_connection_manager.py)

| Existing Test | Change Required |
|---------------|-----------------|
| `test_get_pyodbc_attrs_before_credentials` | Keep for pyodbc backend, skip for mssql-python |
| `test_convert_bytes_to_mswindows_byte_string` | Keep for pyodbc backend only |
| `test_byte_array_to_datetime` | Parameterize to test both backends |
| Connection string tests | Parameterize: verify DRIVER= for pyodbc, no DRIVER= for mssql-python |

```python
# Add to existing test file

@pytest.fixture(params=["mssql-python", "pyodbc"])
def driver_backend(request):
    """Fixture to run tests against both backends."""
    return request.param

class TestConnectionManagerWithBackends:
    """Connection manager tests parameterized for both backends."""
    
    def test_open_connection(self, driver_backend):
        """Test connection opens successfully with both backends."""
        
    def test_connection_retry_on_transient_error(self, driver_backend):
        """Test retry logic works with both backends' exception types."""
        
    def test_query_timeout_applied(self, driver_backend):
        """Test query timeout is set correctly on both backends."""
```

### Updates to [conftest.py](tests/conftest.py)

```python
# Add new fixtures

@pytest.fixture(params=["auto", "mssql-python", "pyodbc"])
def driver_backend_config(request):
    """Fixture for driver_backend profile configuration."""
    return request.param

@pytest.fixture
def fabric_profile_with_backend(driver_backend_config):
    """Profile fixture that includes driver_backend setting."""
    profile = {
        "type": "fabric",
        "server": "test.database.fabric.microsoft.com",
        "database": "testdb",
        "driver_backend": driver_backend_config,
    }
    if driver_backend_config == "pyodbc":
        profile["driver"] = "ODBC Driver 18 for SQL Server"
    return profile
```

### Functional Tests to Update

| Test File | Changes |
|-----------|---------|
| [test_basic.py](tests/functional/adapter/test_basic.py) | Run full suite with both backends via CI matrix |
| [test_incremental.py](tests/functional/adapter/test_incremental.py) | Verify incremental models work with both backends |
| [test_snapshot_new_record_mode.py](tests/functional/adapter/test_snapshot_new_record_mode.py) | Verify snapshots work with both backends |
| [test_data_types.py](tests/functional/adapter/test_data_types.py) | Add tests for unsupported types with mssql-python (expect graceful error) |

### CI Matrix for Backend Testing

The project currently supports Python 3.8-3.11. This migration will update support to **Python 3.9-3.14**:

| Python Version | mssql-python | pyodbc | Notes |
|----------------|--------------|--------|-------|
| 3.9 | ❌ | ✅ | pyodbc only (mssql-python requires 3.10+) |
| 3.10 | ✅ | ✅ | Both drivers supported |
| 3.11 | ✅ | ✅ | Both drivers supported |
| 3.12 | ✅ | ✅ | Both drivers supported |
| 3.13 | ✅ | ✅ | Both drivers supported |
| 3.14 | ✅ | ✅ | Both drivers supported |

```yaml
# .github/workflows/test.yml (or equivalent)
jobs:
  test:
    strategy:
      matrix:
        python-version: ["3.9", "3.10", "3.11", "3.12", "3.13", "3.14"]
        driver-backend: ["mssql-python", "pyodbc"]
        exclude:
          # mssql-python requires Python 3.10+
          - python-version: "3.9"
            driver-backend: "mssql-python"
    
    steps:
      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install ODBC driver (pyodbc only)
        if: matrix.driver-backend == 'pyodbc'
        run: |
          # Install ODBC Driver 18 for SQL Server
          curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
          sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
          sudo apt-get update
          sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
      
      - name: Install dependencies
        run: |
          if [ "${{ matrix.driver-backend }}" == "pyodbc" ]; then
            pip install .[pyodbc]
          else
            pip install .
          fi
      
      - name: Run tests
        env:
          DBT_FABRIC_DRIVER_BACKEND: ${{ matrix.driver-backend }}
        run: pytest tests/
```

### setup.py Classifier Updates

```python
classifiers=[
    "Development Status :: 5 - Production/Stable",
    "License :: OSI Approved :: MIT License",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: POSIX :: Linux",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
],
python_requires=">=3.9",
```

### Notes on Python Version Support

- **Python 3.8**: Dropped (EOL December 2024)
- **Python 3.9-3.14**: Fully supported
- Both mssql-python and pyodbc support 3.9-3.14
- Verify dbt-core supports the same range before finalizing

---

## Documentation Updates

### [README.md](README.md) Updates

Add new section after installation:

```markdown
## Driver Configuration

dbt-fabric supports two database drivers:

| Driver | Python Version | ODBC Required | Recommended |
|--------|----------------|---------------|-------------|
| **mssql-python** (default) | 3.10+ | No | ✅ Yes |
| **pyodbc** (fallback) | 3.9+ | Yes | Legacy |

### Automatic Driver Selection (Default)

By default, dbt-fabric uses `mssql-python` if available, falling back to `pyodbc`:

```yaml
# profiles.yml - no driver_backend needed
fabric:
  outputs:
    dev:
      type: fabric
      server: myserver.database.fabric.microsoft.com
      database: mydb
      authentication: ServicePrincipal
      # ...
```

### Explicit Driver Selection

Force a specific driver with `driver_backend`:

```yaml
# Force mssql-python (fail if unavailable)
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: mssql-python
      # ...

# Force pyodbc (requires ODBC driver installed)
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: pyodbc
      driver: "ODBC Driver 18 for SQL Server"
      # ...
```

### When to Use pyodbc

Use `driver_backend: pyodbc` if you:
- Need Python 3.9 support
- Use unsupported SQL types (`geography`, `geometry`, `xml`, etc.)
- Run on SUSE Linux ARM64
```

### [CONTRIBUTING.md](CONTRIBUTING.md) Updates

Add to development setup section:

```markdown
## Driver Backend Development

### Running Tests with Both Backends

```bash
# Test with mssql-python (default)
pytest tests/

# Test with pyodbc
pip install .[pyodbc]
DBT_FABRIC_DRIVER_BACKEND=pyodbc pytest tests/

# Test auto-fallback (uninstall mssql-python first)
pip uninstall mssql-python
pytest tests/unit/adapters/fabric/test_driver_backend.py
```

### Adding Backend-Specific Code

When adding connection-related code, use the `DriverBackend` abstraction:

```python
# Good: Use backend abstraction
backend = get_driver_backend(credentials.driver_backend)
conn = backend.connect(conn_str, timeout=30, autocommit=True)

# Avoid: Direct driver imports in connection manager
import pyodbc  # Don't do this
```
```

### [CHANGELOG.md](CHANGELOG.md) Entry

```markdown
## [Unreleased]

### Added
- New `driver_backend` configuration option to select database driver
- Support for Microsoft's mssql-python driver (now the default)
- Automatic driver fallback: mssql-python → pyodbc

### Changed
- Default driver changed from pyodbc to mssql-python for Python 3.10+
- Simplified installation: ODBC driver no longer required by default

### Deprecated
- The `driver` field is deprecated when using mssql-python backend (ignored with warning)

### Migration Guide
No action required for most users. The adapter auto-detects the best driver.

To continue using pyodbc explicitly:
```yaml
driver_backend: pyodbc
driver: "ODBC Driver 18 for SQL Server"
```
```

### Inline Docstrings

Update docstrings in source files:

```python
# fabric_credentials.py
@dataclass
class FabricCredentials:
    """
    ...
    
    Attributes:
        driver_backend: Driver to use for database connections.
            - "auto" (default): Use mssql-python if available, fallback to pyodbc
            - "mssql-python": Force mssql-python (fails if unavailable)
            - "pyodbc": Force pyodbc (requires ODBC driver installed)
        driver: ODBC driver name. Only used when driver_backend is "pyodbc".
            Deprecated when using mssql-python backend.
    """
    driver_backend: str = "auto"
    driver: Optional[str] = None  # Deprecated for mssql-python
```

---

## Credential Configuration Examples

### Default (auto-detect, prefers mssql-python)
```yaml
# profiles.yml
fabric:
  target: dev
  outputs:
    dev:
      type: fabric
      server: myserver.database.fabric.microsoft.com
      database: mydb
      authentication: ServicePrincipal
      tenant_id: "{{ env_var('AZURE_TENANT_ID') }}"
      client_id: "{{ env_var('AZURE_CLIENT_ID') }}"
      client_secret: "{{ env_var('AZURE_CLIENT_SECRET') }}"
      # driver_backend: auto  # implicit default
```

### Force mssql-python
```yaml
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: mssql-python  # Fail if not available
      # ...
```

### Force pyodbc (legacy/compatibility)
```yaml
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: pyodbc
      driver: "ODBC Driver 18 for SQL Server"  # Required for pyodbc
      # ...
```

---

## Potential Gotchas

### Behavioral Differences Between Backends

| Aspect | mssql-python | pyodbc |
|--------|--------------|--------|
| Python version | 3.10+ only | 3.9+ |
| ODBC driver install | Not needed | Required |
| Token auth | Native support | `attrs_before` byte conversion |
| Connection string | No `DRIVER=` prefix | Requires `DRIVER=` |
| SUSE ARM64 | Not supported | Supported |

### Unsupported SQL Types in mssql-python

The following types are **not supported** by mssql-python backend:
- `geography`, `geometry`, `hierarchyid`
- `xml`, `sql_variant`, `rowversion`, `vector`

Users needing these types should set `driver_backend: pyodbc`.

### Fallback Logging

When auto-fallback occurs, log a warning:
```python
logger.warning(
    "mssql-python not available, falling back to pyodbc. "
    "Install mssql-python for better performance: pip install mssql-python"
)
```

---

## Version Strategy

- **Minor version bump** (e.g., 1.9.0 → 1.10.0) since no breaking changes
- `driver_backend: auto` preserves existing pyodbc behavior when mssql-python unavailable
- Deprecation warning on `driver` field when using mssql-python backend (field ignored)
- Future major version (2.0) can remove pyodbc support entirely if desired
---

## Performance Testing

### Benchmark Script

Create `scripts/benchmark_drivers.py` to compare driver performance:

```python
#!/usr/bin/env python
"""Benchmark script to compare mssql-python vs pyodbc performance."""

import time
import statistics
from dataclasses import dataclass
from typing import List

@dataclass
class BenchmarkResult:
    driver: str
    metric: str
    values: List[float]
    
    @property
    def mean(self) -> float:
        return statistics.mean(self.values)
    
    @property
    def stdev(self) -> float:
        return statistics.stdev(self.values) if len(self.values) > 1 else 0

def benchmark_connection_time(backend, credentials, iterations=10) -> BenchmarkResult:
    """Measure time to establish a connection."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        conn = backend.connect(...)
        conn.close()
        times.append(time.perf_counter() - start)
    return BenchmarkResult(backend.name, "connection_time_ms", [t * 1000 for t in times])

def benchmark_simple_query(backend, credentials, iterations=100) -> BenchmarkResult:
    """Measure time for SELECT 1 query."""
    # ...

def benchmark_bulk_insert(backend, credentials, row_count=10000) -> BenchmarkResult:
    """Measure time to insert N rows."""
    # ...

def benchmark_result_fetch(backend, credentials, row_count=10000) -> BenchmarkResult:
    """Measure time to fetch N rows."""
    # ...

if __name__ == "__main__":
    # Run benchmarks for both drivers, output comparison table
    pass
```

### Metrics to Capture

| Metric | Description | Target |
|--------|-------------|--------|
| **Connection time** | Time to establish new connection | mssql-python ≤ pyodbc |
| **Simple query latency** | `SELECT 1` round-trip | mssql-python ≤ pyodbc |
| **Bulk insert throughput** | 10K rows insert time | mssql-python ≤ pyodbc |
| **Result fetch throughput** | 10K rows fetch time | mssql-python ≤ pyodbc |
| **Memory usage** | Peak memory during operations | Document difference |
| **Connection pool reuse** | Time for pooled connection | Both should be fast |

### Baseline Capture

Before merging, run benchmarks on:
- Windows 11, Python 3.12
- Ubuntu 22.04, Python 3.12
- macOS (ARM64), Python 3.12

Store results in `docs/benchmarks/` for future comparison.

---

## Environment Variable Override

Support `DBT_FABRIC_DRIVER_BACKEND` environment variable to override `driver_backend` without changing profiles.yml:

```python
# In fabric_credentials.py or driver_backend.py

import os

def get_effective_driver_backend(credentials) -> str:
    """
    Determine effective driver backend with precedence:
    1. Environment variable DBT_FABRIC_DRIVER_BACKEND
    2. Profile setting driver_backend
    3. Default "auto"
    """
    env_override = os.environ.get("DBT_FABRIC_DRIVER_BACKEND")
    if env_override:
        if env_override not in ("auto", "mssql-python", "pyodbc"):
            raise ValueError(
                f"Invalid DBT_FABRIC_DRIVER_BACKEND='{env_override}'. "
                f"Valid values: auto, mssql-python, pyodbc"
            )
        logger.info(f"Using driver backend from environment: {env_override}")
        return env_override
    
    return credentials.driver_backend or "auto"
```

### Use Cases

| Scenario | Command |
|----------|---------|
| Force pyodbc for debugging | `DBT_FABRIC_DRIVER_BACKEND=pyodbc dbt run` |
| Force mssql-python in CI | `DBT_FABRIC_DRIVER_BACKEND=mssql-python dbt run` |
| Test fallback behavior | `pip uninstall mssql-python && dbt run` |

---

## Deprecation Warnings

### When to Warn

| Condition | Warning Message |
|-----------|-----------------|
| `driver` field set + mssql-python active | "The 'driver' field is ignored when using mssql-python backend. Remove it from your profile." |
| `driver` field set + pyodbc active | No warning (field is valid) |
| Python 3.9 + auto mode | "Python 3.9 does not support mssql-python. Using pyodbc. Upgrade to Python 3.10+ for better performance." |

### Implementation

```python
# In fabric_connection_manager.py

import warnings
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabric")

def _emit_deprecation_warnings(credentials, active_backend: str):
    """Emit warnings for deprecated configurations."""
    
    # Warn if driver field is set but using mssql-python
    if credentials.driver and active_backend == "mssql-python":
        logger.warning(
            "DEPRECATION: The 'driver' field is ignored when using mssql-python backend. "
            "Remove 'driver' from your profile to silence this warning."
        )
    
    # Warn on Python 3.9 fallback
    import sys
    if sys.version_info < (3, 10) and credentials.driver_backend == "auto":
        logger.warning(
            "Python 3.9 does not support mssql-python driver. Using pyodbc fallback. "
            "Upgrade to Python 3.10+ for improved performance and simpler setup."
        )
```

### Deprecation Timeline

| Version | Behavior |
|---------|----------|
| 1.10.0 | Warning when `driver` field used with mssql-python |
| 1.11.0 | Warning becomes more prominent (shown at dbt startup) |
| 2.0.0 | Consider removing pyodbc support entirely (TBD) |

---

## Platform CI Matrix

Expand CI to test on Windows, macOS, and Linux:

```yaml
# .github/workflows/test.yml
jobs:
  test:
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
        python-version: ["3.9", "3.10", "3.11", "3.12", "3.13", "3.14"]
        driver-backend: ["mssql-python", "pyodbc"]
        exclude:
          # mssql-python requires Python 3.10+
          - python-version: "3.9"
            driver-backend: "mssql-python"
          # mssql-python doesn't support SUSE ARM64 (not applicable to GitHub runners)
          # macOS ARM64 is supported
    
    runs-on: ${{ matrix.os }}
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install ODBC driver (Ubuntu, pyodbc only)
        if: matrix.os == 'ubuntu-latest' && matrix.driver-backend == 'pyodbc'
        run: |
          curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
          curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
          sudo apt-get update
          sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
      
      - name: Install ODBC driver (Windows, pyodbc only)
        if: matrix.os == 'windows-latest' && matrix.driver-backend == 'pyodbc'
        run: |
          # ODBC Driver 18 is typically pre-installed on Windows runners
          # Verify installation
          Get-OdbcDriver | Where-Object {$_.Name -like "*SQL Server*"}
      
      - name: Install ODBC driver (macOS, pyodbc only)
        if: matrix.os == 'macos-latest' && matrix.driver-backend == 'pyodbc'
        run: |
          brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
          brew update
          HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"
          if [ "${{ matrix.driver-backend }}" == "pyodbc" ]; then
            pip install pyodbc>=5.2.0
          fi
        shell: bash
      
      - name: Run unit tests
        env:
          DBT_FABRIC_DRIVER_BACKEND: ${{ matrix.driver-backend }}
        run: pytest tests/unit/ -v
      
      - name: Run functional tests
        if: github.event_name == 'push' || github.event_name == 'pull_request'
        env:
          DBT_FABRIC_DRIVER_BACKEND: ${{ matrix.driver-backend }}
          # Add Fabric connection secrets
          FABRIC_SERVER: ${{ secrets.FABRIC_SERVER }}
          FABRIC_DATABASE: ${{ secrets.FABRIC_DATABASE }}
          AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
        run: pytest tests/functional/ -v
```

### Platform-Specific Notes

| Platform | mssql-python | pyodbc | Notes |
|----------|--------------|--------|-------|
| Ubuntu x64 | ✅ | ✅ | Primary CI platform |
| Ubuntu ARM64 | ✅ | ✅ | Supported |
| Windows x64 | ✅ | ✅ | ODBC driver pre-installed |
| Windows ARM64 | ✅ | ✅ | Supported |
| macOS x64 | ✅ | ✅ | Intel Macs |
| macOS ARM64 | ✅ | ✅ | Apple Silicon |
| SUSE ARM64 | ❌ | ✅ | mssql-python not supported |

---

## Acceptance Criteria

### Must Have (P0)

- [ ] `driver_backend: auto` works and prefers mssql-python
- [ ] `driver_backend: mssql-python` works on Python 3.10+
- [ ] `driver_backend: pyodbc` works on Python 3.9+
- [ ] All authentication methods work with both backends:
  - [ ] ServicePrincipal
  - [ ] CLI
  - [ ] environment
  - [ ] auto (DefaultAzureCredential)
- [ ] All existing functional tests pass with both backends
- [ ] Connection pooling works with both backends
- [ ] Query timeout works with both backends
- [ ] DATETIMEOFFSET conversion works with both backends
- [ ] Error handling/retry logic works with both backends
- [ ] `DBT_FABRIC_DRIVER_BACKEND` environment variable override works

### Should Have (P1)

- [ ] Deprecation warnings implemented for `driver` field
- [ ] CI runs on Windows, macOS, Ubuntu
- [ ] CI tests all Python versions 3.9-3.14
- [ ] Performance benchmarks captured and documented
- [ ] README updated with driver configuration docs
- [ ] CHANGELOG entry written
- [ ] CONTRIBUTING.md updated with backend development guide

### Nice to Have (P2)

- [ ] Benchmark script added to `scripts/`
- [ ] Performance comparison table in docs
- [ ] Migration guide for users switching from pyodbc
- [ ] FAQ section for common issues

### Definition of Done

1. All P0 criteria met
2. All P1 criteria met
3. No regressions in existing functionality
4. PR approved by at least 2 maintainers
5. Documentation reviewed and approved
6. Release notes drafted