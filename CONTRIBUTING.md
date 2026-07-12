# Development of the adapter

Python 3.12 (or 3.13) is used for developing the adapter. Dependencies and environments are managed with
[`uv`](https://docs.astral.sh/uv/). Install `uv` first ([installation instructions](https://docs.astral.sh/uv/getting-started/installation/)).

## Setup (one-time)

```shell
make dev
```

This runs `uv sync` (creates `.venv`, installs the adapter itself in editable mode, and installs the `dev`
dependency group — pytest, ruff, mypy, pre-commit, etc. — pinned exactly as recorded in the checked-in
`uv.lock`) and then installs the git pre-commit hooks.

## Everyday commands

Everything below is a thin wrapper around `uv run <command>`; you can always call `uv run ...` directly instead.

| Command           | What it does                                          |
|--------------------|--------------------------------------------------------|
| `make unit`        | Runs the unit tests (`uv run pytest tests/unit`)       |
| `make functional`  | Runs the functional tests (`uv run pytest tests/functional`) — see [Testing](#testing) below for local setup |
| `make ruff`        | Runs ruff lint checks (`uv run ruff check .`)          |
| `make format`      | Runs ruff format checks (`uv run ruff format --check .`) |
| `make mypy`        | Runs static type checking (`uv run mypy`)              |
| `make lint`        | Runs `ruff` + `format` + `mypy` together               |
| `make test`        | Runs unit tests + `lint`                                |
| `make server`      | Spins up a local SQL Server via Docker Compose (for functional tests) |
| `make all`         | Runs every pre-commit hook against all files            |
| `make help`        | Lists all available `make` targets                     |

Changed a dependency in `pyproject.toml` (`dependencies` or `[dependency-groups]`)? Run `uv lock` to update
`uv.lock`, then commit both files together. `uv run ...` will auto-sync your `.venv` to match.

## Packaging

The project is packaged with [Hatchling](https://hatch.pypa.io/latest/) via a PEP 621 `pyproject.toml`
(no more `setup.py`). The package version is read from `dbt/adapters/fabric/__version__.py`
(`[tool.hatch.version]`). Runtime dependencies live under `[project.dependencies]`; dev-only dependencies
(pytest, ruff, mypy, etc.) live under `[dependency-groups].dev` and are pinned in the checked-in `uv.lock`.

## Code quality: what runs when

* **On every `git commit`** — pre-commit runs `ruff` (auto-fixes lint issues), `ruff format`, and `mypy`,
  plus a handful of hygiene checks (YAML/JSON validity, trailing whitespace, merge conflict markers, etc.).
  This is the fast, local feedback loop — most issues are caught and fixed before you even push.
* **On every push / pull request to `main`** — GitHub Actions re-runs the unit tests (see [CI/CD](#cicd) below)
  as the CI safety net. There is currently no separate CI lint job; formatting/lint enforcement happens via
  the local pre-commit hook.

## Testing

The functional tests require a running SQL Server instance. You can easily spin up a local instance with the following command:

```shell
make server
```

This will use Docker Compose to spin up a local instance of SQL Server. Docker Compose is now bundled with Docker, so make sure to [install the latest version of Docker](https://docs.docker.com/get-docker/).

Next, tell our tests how they should connect to the local instance by creating a file called `test.env` in the root of the project.
You can use the provided `test.env.sample` as a base and if you started the server with `make server`, then this matches the instance running on your local machine.

```shell
cp test.env.sample test.env
```

You can tweak the contents of this file to test against a different database.

Note that we need 3 users to be able to run tests related to the grants.
The 3 users are defined by the following environment variables containing their usernames.

* `DBT_TEST_USER_1`
* `DBT_TEST_USER_2`
* `DBT_TEST_USER_3`

Then run the tests:

```shell
make unit
make functional
```

## CI/CD

All pipelines run on GitHub Actions. Each one, in the order it typically fires:

1. **`publish-docker`** — builds and pushes the Docker image (one tag per supported Python version) used by
   the other pipelines below. Triggered when files under `devops/` or the workflow itself change.
   The `Dockerfile` lives in `devops/CI.Dockerfile` and includes `uv` pre-installed.
2. **`unit-tests`** — on push/PR to `main`/`v*`: `uv sync --locked` then `uv run pytest tests/unit`, once
   per supported Python version (3.12, 3.13).
3. **`integration-tests-azure`** — on PR to `main`: authenticates to Azure via OIDC, then `uv sync --locked`
   and `uv run pytest tests/functional --profile integration_tests` against a real Azure SQL-backed Fabric
   Warehouse. See [Azure integration tests](#azure-integration-tests) below for the required secrets.
4. **`release-version`** — on pushing a `v*` tag: `uv run scripts/verify_version.py` checks the tag matches
   `__version__.py`, then `uv build` + `uv publish` ship the package to PyPI. See
   [Releasing a new version](#releasing-a-new-version) below.

There is no dedicated lint/format CI job today — that check happens locally via the pre-commit hook
(see [Code quality](#code-quality-what-runs-when) above).

### Azure integration tests

The following environment variables are available:

* `DBT_AZURESQL_SERVER`: full hostname of the server hosting the Azure SQL database
* `DBT_AZURESQL_DB`: name of the Azure SQL database
* `DBT_AZURESQL_UID`: username of the SQL admin on the server hosting the Azure SQL database
* `DBT_AZURESQL_PWD`: password of the SQL admin on the server hosting the Azure SQL database
* `DBT_AZURE_TENANT`: Azure tenant ID
* `DBT_AZURE_SUBSCRIPTION_ID`: Azure subscription ID
* `DBT_AZURE_RESOURCE_GROUP_NAME`: Azure resource group name
* `DBT_AZURE_SP_NAME`: Client/application ID of the service principal used to connect to Azure AD
* `DBT_AZURE_SP_SECRET`: Password of the service principal used to connect to Azure AD

## Releasing a new version

1. Bump the version number in `dbt/adapters/fabric/__version__.py`.
2. If you're releasing support for a new version of `dbt-core`, also bump the `dbt-core`/`dbt-adapters`
   version constraints in `pyproject.toml`.
3. Create a git tag named `v<version>` (matching the version from step 1) and push it to GitHub.
4. The `release-version` GitHub Actions workflow triggers automatically: it verifies the tag matches
   `__version__.py` (`scripts/verify_version.py`), builds the package (`uv build`, using the `hatchling`
   backend), and publishes it to PyPI (`uv publish`).
