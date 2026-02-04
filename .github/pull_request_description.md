## Summary

This PR introduces Microsoft's native `mssql-python` driver as the default database driver for dbt-fabric, replacing `pyodbc` as the primary option while maintaining full backward compatibility.

### Why this change?

| Aspect | mssql-python | pyodbc |
|--------|--------------|--------|
| **ODBC Driver Required** | ❌ No (bundled) | ✅ Yes |
| **Setup Complexity** | Simple `pip install` | Install ODBC driver + headers |
| **Authentication** | Native Azure AD support | Token byte conversion required |
| **Minimum Python** | 3.10+ | 3.9+ |

The mssql-python driver bundles the native SQL Server driver, eliminating the need for users to install and configure ODBC drivers separately.

---

## Changes

### New Files

| File | Description |
|------|-------------|
| `dbt/adapters/fabric/driver_backend.py` | Driver abstraction layer with `DriverBackend` ABC, `MssqlPythonBackend`, and `PyodbcBackend` implementations |
| `tests/unit/adapters/fabric/test_driver_backend.py` | 52 unit tests for driver backend |
| `tests/unit/adapters/fabric/test_fabric_credentials.py` | 16 unit tests for credentials |
| `tests/unit/adapters/fabric/test_fabric_connection_manager.py` | 29 unit tests for connection manager |
| `tests/unit/adapters/fabric/test_fabric_column.py` | 34 unit tests for column types |
| `tests/unit/adapters/fabric/test_fabric_relation.py` | 9 unit tests for relations |

### Modified Files

| File | Changes |
|------|---------|
| `fabric_credentials.py` | Added `driver_backend` field (auto/mssql-python/pyodbc) |
| `fabric_connection_manager.py` | Refactored to use `DriverBackend` interface |
| `setup.py` | Updated dependencies, Python 3.10-3.13 support |
| `README.md` | Comprehensive driver documentation and system dependencies |
| `CHANGELOG.md` | Release notes and migration guide |
| `unit-tests.yml` | Matrix for Python 3.10-3.13 × both drivers |
| `integration-tests-azure.yml` | Matrix for both drivers |
| `tests/conftest.py` | Added `driver_backend` to test profile |

---

## Driver Selection Behavior

```
┌─────────────────────────────────────────────────────────────┐
│                    Driver Selection Flow                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Check DBT_FABRIC_DRIVER_BACKEND env var                 │
│     └─> If set: use specified driver                        │
│                                                              │
│  2. Check driver_backend in profiles.yml                    │
│     └─> If set: use specified driver                        │
│                                                              │
│  3. Auto-detect (default):                                  │
│     └─> Try mssql-python first, fallback to pyodbc          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Configuration Examples

### Automatic (Recommended)
```yaml
# profiles.yml - no driver_backend needed
fabric:
  outputs:
    dev:
      type: fabric
      server: myserver.database.fabric.microsoft.com
      database: mydb
      schema: dbo
      authentication: ServicePrincipal
```

### Force Specific Driver
```yaml
# Force mssql-python
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: mssql-python
      # ...

# Force pyodbc (requires ODBC driver)
fabric:
  outputs:
    dev:
      type: fabric
      driver_backend: pyodbc
      driver: "ODBC Driver 18 for SQL Server"
      # ...
```

### Environment Variable Override
```bash
DBT_FABRIC_DRIVER_BACKEND=pyodbc dbt run
```

---

## Test Coverage

| Module | Before | After |
|--------|--------|-------|
| **Overall** | 41% | **51%** |
| `driver_backend.py` | N/A | **95%** |
| `fabric_credentials.py` | 76% | **100%** |
| `fabric_column.py` | 44% | **100%** |
| `fabric_relation.py` | 67% | **100%** |
| `fabric_connection_manager.py` | 31% | 41% |

**Test count: 30 → 159 tests** (+129 new tests)

---

## Breaking Changes

**None.** This is a backward-compatible change:

- Existing profiles continue to work without modification
- `pyodbc` users can continue using `driver_backend: pyodbc`
- The `driver` field is still supported for pyodbc backend

### Deprecation Notice

The `driver` field will emit a warning when using `mssql-python` backend (where it's ignored).

---

## CI/CD Updates

### Unit Tests Matrix
| Python | mssql-python | pyodbc |
|--------|--------------|--------|
| 3.9 | ❌ (not supported) | ✅ |
| 3.10 | ✅ | ✅ |
| 3.11 | ✅ | ✅ |
| 3.12 | ✅ | ✅ |
| 3.13 | ✅ | ✅ |

### Integration Tests
Both drivers tested against live Fabric DW with Azure AD authentication.

---

## System Dependencies

### mssql-python (bundled driver)

| Platform | Required Libraries |
|----------|-------------------|
| macOS | `brew install openssl` |
| Debian/Ubuntu | `apt install libltdl7 libkrb5-3 libgssapi-krb5-2` |
| RHEL/CentOS | `dnf install libtool-ltdl krb5-libs` |
| SUSE | `zypper install libltdl7 libkrb5-3 libgssapi-krb5-2` |
| Alpine | `apk add libtool krb5-libs krb5-dev` |

### pyodbc (external driver)
Requires Microsoft ODBC Driver 18 for SQL Server installation.

---

## Checklist

- [x] Code follows project style guidelines
- [x] Unit tests added/updated
- [x] Documentation updated (README, CHANGELOG)
- [x] CI workflows updated
- [x] Backward compatibility maintained
- [x] No breaking changes for existing users
