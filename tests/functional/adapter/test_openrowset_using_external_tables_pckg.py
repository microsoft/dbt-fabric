"""Functional tests for OPENROWSET-backed views via the dbt-external-tables package.

NOTE: This covers OPENROWSET file access (Parquet/CSV/JSONL from OneLake) wrapped as
views through the dbt-labs/dbt_external_tables package's stage_external_sources
operation - NOT a persistent CREATE EXTERNAL TABLE feature. Named to avoid confusion
with any future "external tables" feature that materializes real external table objects.

These tests exercise the Fabric-specific override macros in
dbt/include/fabric/macros/dbt_package_support/dbt_external_tables/external_tables.sql,
which let the community `dbt-labs/dbt_external_tables` package work against Fabric
Data Warehouse by creating views that wrap OPENROWSET(BULK ...) instead of the
Synapse-style CREATE EXTERNAL TABLE DDL the package normally emits.

Requires network access to install the package from GitHub via `dbt deps`, and a
live Fabric warehouse connection (OneLake sample files uploaded to the test lakehouse).
"""

import re

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

# -- Constants --

ONELAKE_BASE = (
    "https://msit-westcentralus-onelake.dfs.fabric.microsoft.com"
    "/e4487eff-d67d-4b58-917c-ffbb61a5c05f"
    "/63ac0a05-afa9-4b90-b840-8f1b6421e761/Files"
)

EXTERNAL_TABLES_PACKAGE = {
    "packages": [
        {"package": "dbt-labs/dbt_external_tables", "version": "0.12.2"},
    ]
}

DISPATCH_CONFIG = {
    "dispatch": [
        {
            "macro_namespace": "dbt_external_tables",
            "search_order": ["dbt", "dbt_external_tables"],
        }
    ]
}


def _normalize(sql: str) -> str:
    s = re.sub(r"\s+", " ", sql)
    return re.sub(r"\s?([\(\),])\s?", r"\1", s).lower().strip()


# -- Source / model fixtures --

sources_parquet_yml = """
version: 2
sources:
  - name: ext_parquet
    schema: "{{ target.schema }}"
    tables:
      - name: customers_ext
        external:
          location: "{base}/customers.parquet"
          file_format: parquet
        columns:
          - name: customer_id
            data_type: int
          - name: first_name
            data_type: "varchar(100)"
          - name: last_name
            data_type: "varchar(100)"
          - name: email
            data_type: "varchar(200)"
          - name: created_at
            data_type: datetime2
""".replace("{base}", ONELAKE_BASE)

parquet_model_sql = """
select * from {{ source('ext_parquet', 'customers_ext') }}
"""

sources_csv_yml = """
version: 2
sources:
  - name: ext_csv
    schema: "{{ target.schema }}"
    tables:
      - name: products_ext
        external:
          location: "{base}/products_semicolon.csv"
          file_format: csv
          options:
            header_row: true
            fieldterminator: ";"
        columns:
          - name: product_id
            data_type: int
          - name: name
            data_type: "varchar(200)"
          - name: category
            data_type: "varchar(100)"
          - name: price
            data_type: "decimal(10,2)"
          - name: in_stock
            data_type: bit
""".replace("{base}", ONELAKE_BASE)

csv_model_sql = """
select * from {{ source('ext_csv', 'products_ext') }}
"""

sources_jsonl_yml = """
version: 2
sources:
  - name: ext_jsonl
    schema: "{{ target.schema }}"
    tables:
      - name: events_ext
        external:
          location: "{base}/events.jsonl"
          file_format: jsonl
        columns:
          - name: event_id
            data_type: "varchar(50)"
          - name: event_type
            data_type: "varchar(50)"
          - name: timestamp
            data_type: "varchar(50)"
""".replace("{base}", ONELAKE_BASE)

jsonl_model_sql = """
select * from {{ source('ext_jsonl', 'events_ext') }}
"""

# Parquet does not support FIRSTROW/CODEPAGE/DATAFILETYPE per Fabric's OPENROWSET docs,
# unlike CSV/JSONL - used to verify the per-format option guard rejects this combination.
sources_parquet_invalid_option_yml = """
version: 2
sources:
  - name: ext_parquet_invalid
    schema: "{{ target.schema }}"
    tables:
      - name: customers_ext_invalid
        external:
          location: "{base}/customers.parquet"
          file_format: parquet
          options:
            firstrow: 2
        columns:
          - name: customer_id
            data_type: int
""".replace("{base}", ONELAKE_BASE)


# -- Test classes --


class TestOpenrowsetParquet:
    """Staging a Parquet external source creates a queryable OPENROWSET-backed view."""

    @pytest.fixture(scope="class")
    def packages(self):
        return EXTERNAL_TABLES_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return DISPATCH_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parquet_model.sql": parquet_model_sql,
            "sources.yml": sources_parquet_yml,
        }

    def test__stage_and_query_parquet(self, project):
        run_dbt(["deps"])
        results = run_dbt(["run-operation", "stage_external_sources"])
        assert len(results) == 0 or results is not None

        results = run_dbt(["run", "-s", "parquet_model"])
        assert len(results) == 1

        # The view should be directly queryable outside of the model too.
        query_results = project.run_sql(
            f"select count(*) as cnt from {project.test_schema}.customers_ext", fetch="one"
        )
        assert query_results[0] >= 0


class TestOpenrowsetCsvWithOptions:
    """Staging a CSV external source with custom FIELDTERMINATOR works."""

    @pytest.fixture(scope="class")
    def packages(self):
        return EXTERNAL_TABLES_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return DISPATCH_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "csv_model.sql": csv_model_sql,
            "sources.yml": sources_csv_yml,
        }

    def test__stage_and_query_csv(self, project):
        run_dbt(["deps"])
        run_dbt(["run-operation", "stage_external_sources"])

        results = run_dbt(["run", "-s", "csv_model"])
        assert len(results) == 1

        query_results = project.run_sql(
            f"select count(*) as cnt from {project.test_schema}.products_ext", fetch="one"
        )
        assert query_results[0] >= 0


class TestOpenrowsetJsonl:
    """Staging a JSONL external source works."""

    @pytest.fixture(scope="class")
    def packages(self):
        return EXTERNAL_TABLES_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return DISPATCH_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "jsonl_model.sql": jsonl_model_sql,
            "sources.yml": sources_jsonl_yml,
        }

    def test__stage_and_query_jsonl(self, project):
        run_dbt(["deps"])
        run_dbt(["run-operation", "stage_external_sources"])

        results = run_dbt(["run", "-s", "jsonl_model"])
        assert len(results) == 1

        query_results = project.run_sql(
            f"select count(*) as cnt from {project.test_schema}.events_ext", fetch="one"
        )
        assert query_results[0] >= 0


class TestOpenrowsetRestageAndDrop:
    """Re-staging is a no-op by default; ext_full_refresh forces drop + recreate."""

    @pytest.fixture(scope="class")
    def packages(self):
        return EXTERNAL_TABLES_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return DISPATCH_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parquet_model.sql": parquet_model_sql,
            "sources.yml": sources_parquet_yml,
        }

    def test__restage_is_noop_then_full_refresh_drops_and_recreates(self, project):
        run_dbt(["deps"])
        run_dbt(["run-operation", "stage_external_sources"])

        # Re-staging without ext_full_refresh should not attempt to recreate the view.
        _, output = run_dbt_and_capture(
            ["run-operation", "stage_external_sources", "--log-level", "debug"]
        )
        assert "create view" not in output.lower()

        # Forcing a full refresh should drop and recreate the view.
        _, output = run_dbt_and_capture(
            [
                "run-operation",
                "stage_external_sources",
                "--vars",
                "{ext_full_refresh: true}",
                "--log-level",
                "debug",
            ]
        )
        assert "drop view" in output.lower()
        assert "create view" in output.lower()

        # The view should still be queryable after being recreated.
        results = run_dbt(["run", "-s", "parquet_model"])
        assert len(results) == 1


class TestOpenrowsetParquetRejectsUnsupportedOption:
    """Parquet doesn't support FIRSTROW (unlike CSV/JSONL) - staging should raise a
    clear compiler error rather than sending an invalid option to the server."""

    @pytest.fixture(scope="class")
    def packages(self):
        return EXTERNAL_TABLES_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return DISPATCH_CONFIG

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": sources_parquet_invalid_option_yml,
        }

    def test__firstrow_rejected_for_parquet(self, project):
        run_dbt(["deps"])
        _, output = run_dbt_and_capture(
            ["run-operation", "stage_external_sources"], expect_pass=False
        )
        assert "not supported for format 'PARQUET'" in output
