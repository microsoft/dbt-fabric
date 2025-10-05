# Installation

## Prerequisites

### Python

Make sure you have [Python](https://www.python.org/) 3.10 or higher installed. You can check your Python version by running:

```bash
python --version
```

### ODBC Driver

This adapter requires the ODBC Driver for SQL Server.

=== "Windows"

    Download and install the ODBC Driver [from Microsoft Learn](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17&WT.mc_id=MVP_310840).

=== "macOS"

    1. Install Homebrew if you haven't already:

        ```bash
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        ```

    1. Add the MS ODBC Driver tap:

        ```bash
        brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
        ```

    1. Update Homebrew:

        ```bash
        brew update
        ```

    1. Install the ODBC Driver (this has a dependency on `unixodbc` which will be installed automatically):

        ```bash
        brew install msodbcsql18
        ```

=== "Linux"

    Follow the instructions on [Microsoft Learn](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver17&WT.mc_id=MVP_310840) to install the ODBC Driver and its dependencies for your specific Linux distribution.

## Install dbt-fabric-samdebruyn

Install dbt-fabric-samdebruyn using pip:

```bash
pip install dbt-fabric-samdebruyn dbt-core
```
