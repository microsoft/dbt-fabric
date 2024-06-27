# Changelog

### V1.8.7
* Improving table materialization to minimize downtime #189
* Handling temp tables in incremental models #188
* Add label support to filter queries #181
* Addressed bug - incremental models cannot full refresh #179 
* Addressed bug - #197, dbt test incorrect syntax with macro helpers.sql

### v1.8.0rc2

## Bug Fixes
* Remove dbt-adapters requirement in setup.py, and specify commit SHA of dbt-core and dbt-adapters in dev_requirements.txt, to fix `make dev`
* Fix failing test `tests/functional/adapter/test_query_comment.py::TestMacroArgsQueryComments::test_matches_comment` to use correct dbt_version, see [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/tests/functional/adapter/query_comment/test_query_comment.py)

### v1.8.0rc1

## Features

Supporting dbt-core 1.8.0

## Bug fixes

* Refactor relations to query from sys catalog instead of information schema causing concurrency issues when running multiple threads in parallel (https://github.com/microsoft/dbt-fabric/issues/52).

## Enhancements

[Decouple imports](https://github.com/dbt-labs/dbt-adapters/discussions/87) to common dbt core and dbt adapter interface packages for future maintainability and extensibility.

* Bump adapter packages
    - from pyodbc>=4.0.35,<5.1.0" to pyodbc>=4.0.35,<5.2.0

> From now on, Apple-silicon users don't have to locally build pyodbc, because M1, M2 binaries is included in pyodbc from 5.1.0 onwards!


* Bump dev requirements
    - from pytest~=7.4. to pytest~=8.0.1
    - from twine~=4.0.2 to twine~=5.0.0
    - from pre-commit~=3.5.0 to pre-commit~=3.6.2

### v1.7.3

## Enhancements

* Overwritten view adapter materialization and made improvements.
* Overwritten table adapter materizalization and made improvements in handling model level constraints
* Made Constraint name mandatory
* Added several macros to manage indexes, dropping table dependencies and managing model level constraints
* Bump dbt-tests-adapter requirement from ~=1.7.3 to ~=1.7.4
* Bump py-test adapter requirement from ~=pytest==7.4.3 to ~=pytest==7.4.4
* Bump precommit adapter requirement from ~=pre-commit==3.5.0 to ~=pre-commit==3.6.0

### v1.7.2

## Bug Fixes
* Addressed issue [#53](https://github.com/microsoft/dbt-fabric/issues/101) - "The server supports a maximum of 2100 parameters" by reducing the batch size by 1 if number of insert value parameters exceed 2100.
* Added bytearry data type code support along bytes for varbinary sql datatype.

## Enhancements
* Bump dbt-tests-adapter requirement from ~=1.7.2 to ~=1.7.3
* Bump actions/setup-python from 4 to 5

### v1.7.1

## Features

* Added capability support structure in fabric adapter
* Added metadata freshness checks
* Updated catalog fetch performance improvements to handle relations with many pre-existing objects
* Added dbt-show support to 1.7.1

## Enhancements

* improve connection manager logging
* Added metadata freshness checks tests
* Added capability support tests
* Added catalog fetch performance improvements
* Added dbt show's --limit flag tests
* Added storing test failures tests

### v1.6.1

## Features

* Fabric DW now supports sp_rename. Starting v1.6.1 sp_rename is metadata operation
* Enabled table clone feature

## Enhancements

* Addressed [Issue 53](https://github.com/microsoft/dbt-fabric/issues/53)
* Added explicit support for [Issue 76 - ActiveDirectoryServicePrincipal authentication](https://github.com/microsoft/dbt-fabric/issues/74)
* Removed port number support in connection string as it is no longer required in Microsoft Fabric DW
* Removed MSI authentication as it does not make sense for Microsoft Fabric.
* Table lock hints are not supported by Fabric DW
* Supported authentication modes are ActiveDirectory* and AZ CLI

### v1.7.0

## Features

* Supporting dbt-core 1.7.2

## Dependencies

* Bump from pytest==7.4.2 to pytest==7.4.3
* Bump from pre-commit==3.4.0 to 3.5.0
* Bump from dbt-tests-adapter~=1.6.2 to 1.7.2
* Bump from wheel==0.41.1 to 0.41.1

### v1.6.0

## Features

* Supporting dbt-core 1.6.2
* Adding limit - new args to adapter.execute() function
* Added tests related to dbt-debug to test --connection parameter
* Added adapter zone tests

## Dependencies

* Bump from pytest==7.4.0 to pytest==7.4.2
* Bump from pre-commit==3.3.3 to 3.4.0
* Bump from dbt-tests-adapter~=1.5.2 to 1.6.2
* Bump from actions@v3 to v4
* Bump from build-push-action@v4.0.0 to 4.2.1
### v1.5.0

Releasing 1.5 version for dbt-cloud integration.

### v1.5.0-rc1

* Upgraded dbt-fabric adapter to match dbt-core & dbt-tests-adapter version 1.5.2.
* Added constraint support to dbt-fabric adapter.
    * Check constraints are not supported.
    * Column & model constraints are not supported in CREATE TABLE command by Microsoft Fabric Data Warehouse. Column and model constraints are implemented by ALTER TABLE ADD Constraints command.
    * user-defined names for constraints are not currently supported. naming is handled by the adapter, until `SP_RENAME` is supported in Fabric
    * Added tests related to constraints.
* Bumped wheel, precommit, docker package versions.


### v1.4.0-rc3

Updated connection property to track dbt telemetry by Microsoft.

### v1.4.0-rc2

Fixed view rename relation macro.
Bumped required python packages versions.

### v1.4.0-rc1

Requires dbt 1.4.5 and previous versions are not supported by Fabric Data Warehouse. Microsoft is actively releasing/adding T-SQL support. Please raise issues in case of any bugs.

#### DBT Supported features
- All materializations and resource features such as Tables, Views, Seeds, sources, tests and dbt docs are supported.
- Advanced features such as incremental and snapshot features may work but are not planned to support in 1.4.5.

We recommend you to read Microsoft Fabric Data Warehouse [documentation](https://review.learn.microsoft.com/en-us/fabric/data-warehouse/?branch=main) before using the adapter.

#### Important things to consider when using dbt-fabric adapter
- SQL/Basic authentication is not supported by Fabric Data Warehouse. CLI and Service Principal authentication are currently supported.
- Please review the [T-SQL commands](https://review.learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area#limitations) not supported in Fabric Data Warehouse. Some of T-SQL commands such as ALTER TABLE ADD/ALTER/DROP COLUMN, MERGE, TRUNCATE, SP_RENAME are supported by dbt-fabric adapter using CTAS, DROP and CREATE commands.
- Many data types are supported and a few aren't. Please review [this](https://review.learn.microsoft.com/en-us/fabric/data-warehouse/data-types?branch=main) link for supported and unsupported data types.

#### Unsupported features
- nolock
- provisioning and granting access to basic user (sql server authentication)
- CTAS supports select on views/tables with underlying table definition. CREATE TABLE AS SELECT 1 AS Id - is not supported.
- datetime data type
- SP_RENAME
