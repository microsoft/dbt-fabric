import re

import pytest
from dbt.tests.util import read_file, run_dbt

# -- Constants --

ONELAKE_BASE = (
    "https://msit-onelake.dfs.fabric.microsoft.com"
    "/e4487eff-d67d-4b58-917c-ffbb61a5c05f"
    "/08f6bf3d-0819-4d4d-bfbb-b04a2fab4238/Files"
)
SAMPLE_DATA_PATH = ONELAKE_BASE

# -- Source definitions --

sources_parquet_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: customers
        meta:
          openrowset:
            file: "customers.parquet"
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
""".format(
    base=SAMPLE_DATA_PATH
)

sources_csv_orders_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: orders
        meta:
          openrowset:
            file: "orders.csv"
        columns:
          - name: order_id
            data_type: int
          - name: customer_id
            data_type: int
          - name: order_date
            data_type: date
          - name: amount
            data_type: "decimal(10,2)"
          - name: status
            data_type: "varchar(50)"
""".format(
    base=SAMPLE_DATA_PATH
)

sources_csv_semicolon_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: products
        meta:
          openrowset:
            file: "products_semicolon.csv"
            options:
              FIELDTERMINATOR: ";"
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
""".format(
    base=SAMPLE_DATA_PATH
)

sources_tsv_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: orders_tsv
        meta:
          openrowset:
            file: "orders.tsv"
            options:
              FIELDTERMINATOR: "\\t"
        columns:
          - name: order_id
            data_type: int
          - name: customer_id
            data_type: int
          - name: order_date
            data_type: date
          - name: amount
            data_type: "decimal(10,2)"
          - name: status
            data_type: "varchar(50)"
""".format(
    base=SAMPLE_DATA_PATH
)

sources_jsonl_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: events
        meta:
          openrowset:
            file: "events.jsonl"
        columns:
          - name: event_id
            data_type: "varchar(50)"
          - name: event_type
            data_type: "varchar(50)"
          - name: timestamp
            data_type: "varchar(50)"
          - name: user_id
            data_type: INT
            meta:
              openrowset:
                path: "$.user.id"
          - name: user_name
            data_type: "varchar(100)"
            meta:
              openrowset:
                path: "$.user.name"
          - name: page
            data_type: "varchar(200)"
            meta:
              openrowset:
                path: "$.properties.page"
""".format(
    base=SAMPLE_DATA_PATH
)

sources_parquet_no_schema_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: customers_auto
        meta:
          openrowset:
            file: "customers.parquet"
""".format(
    base=SAMPLE_DATA_PATH
)

sources_explicit_format_yml = """
version: 2
sources:
  - name: sample_data
    meta:
      openrowset:
        path: "{base}"
    tables:
      - name: orders_explicit
        meta:
          openrowset:
            file: "orders.csv"
            format: CSV
        columns:
          - name: order_id
            data_type: int
          - name: customer_id
            data_type: int
          - name: order_date
            data_type: date
          - name: amount
            data_type: "decimal(10,2)"
          - name: status
            data_type: "varchar(50)"
""".format(
    base=SAMPLE_DATA_PATH
)

sources_full_path_override_yml = """
version: 2
sources:
  - name: direct_access
    tables:
      - name: customers_direct
        meta:
          openrowset:
            path: "{base}/customers.parquet"
        columns:
          - name: customer_id
            data_type: int
          - name: first_name
            data_type: "varchar(100)"
""".format(
    base=SAMPLE_DATA_PATH
)

# -- Model SQL fixtures --

parquet_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'customers') }}
"""

csv_orders_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'orders') }}
"""

csv_semicolon_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'products') }}
"""

tsv_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'orders_tsv') }}
"""

jsonl_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'events') }}
"""

parquet_no_schema_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'customers_auto') }}
"""

explicit_format_model_sql = """
SELECT * FROM {{ openrowset_source('sample_data', 'orders_explicit') }}
"""

full_path_model_sql = """
SELECT * FROM {{ openrowset_source('direct_access', 'customers_direct') }}
"""


# -- Helpers --


def _normalize(sql: str) -> str:
    """Collapse whitespace and remove spaces around punctuation for easy assertion."""
    s = re.sub(r"\s+", " ", sql)
    return re.sub(r"\s?([\(\),])\s?", r"\1", s).lower().strip()


# -- Test classes --


class TestOpenrowsetParquet:
    """Parquet file with explicit column schema."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parquet_model.sql": parquet_model_sql,
            "sources.yml": sources_parquet_yml,
        }

    def test__parquet_compiles(self, project):
        results = run_dbt(["compile", "-s", "parquet_model"])
        assert len(results) == 1

        sql = _normalize(read_file("target", "compiled", "test", "models", "parquet_model.sql"))

        assert "openrowset(" in sql
        assert "bulk '{}/customers.parquet'".format(SAMPLE_DATA_PATH.lower()) in sql
        assert "format = 'parquet'" in sql
        assert "with(" in sql
        assert "customer_id int" in sql
        assert "first_name varchar(100)" in sql
        assert "last_name varchar(100)" in sql
        assert "email varchar(200)" in sql
        assert "created_at datetime2" in sql
        assert "as [customers]" in sql


class TestOpenrowsetCsvOrders:
    """CSV file with default options (HEADER_ROW = TRUE auto-applied)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "csv_orders_model.sql": csv_orders_model_sql,
            "sources.yml": sources_csv_orders_yml,
        }

    def test__csv_default_options(self, project):
        results = run_dbt(["compile", "-s", "csv_orders_model"])
        assert len(results) == 1

        sql = _normalize(read_file("target", "compiled", "test", "models", "csv_orders_model.sql"))

        assert "format = 'csv'" in sql
        assert "header_row = true" in sql
        # Engine defaults handle terminators — should not appear
        assert "fieldterminator" not in sql
        assert "rowterminator" not in sql
        # Schema
        assert "order_id int" in sql
        assert "amount decimal(10,2)" in sql
        assert "status varchar(50)" in sql
        assert "as [orders]" in sql


class TestOpenrowsetCsvSemicolon:
    """CSV file with semicolon delimiter override."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "csv_semicolon_model.sql": csv_semicolon_model_sql,
            "sources.yml": sources_csv_semicolon_yml,
        }

    def test__csv_semicolon_override(self, project):
        results = run_dbt(["compile", "-s", "csv_semicolon_model"])
        assert len(results) == 1

        sql = _normalize(
            read_file("target", "compiled", "test", "models", "csv_semicolon_model.sql")
        )

        assert "format = 'csv'" in sql
        assert "header_row = true" in sql
        assert "fieldterminator = ';'" in sql
        assert "product_id int" in sql
        assert "name varchar(200)" in sql
        assert "price decimal(10,2)" in sql
        assert "in_stock bit" in sql
        assert "as [products]" in sql


class TestOpenrowsetTsv:
    """TSV file — auto-detected as CSV format with tab delimiter override."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tsv_model.sql": tsv_model_sql,
            "sources.yml": sources_tsv_yml,
        }

    def test__tsv_compiles(self, project):
        results = run_dbt(["compile", "-s", "tsv_model"])
        assert len(results) == 1

        sql = _normalize(read_file("target", "compiled", "test", "models", "tsv_model.sql"))

        assert "format = 'csv'" in sql
        assert "header_row = true" in sql
        assert "fieldterminator" in sql
        assert "order_id int" in sql
        assert "as [orders_tsv]" in sql


class TestOpenrowsetJsonl:
    """JSONL file with nested column path mapping."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "jsonl_model.sql": jsonl_model_sql,
            "sources.yml": sources_jsonl_yml,
        }

    def test__jsonl_with_column_paths(self, project):
        results = run_dbt(["compile", "-s", "jsonl_model"])
        assert len(results) == 1

        sql = _normalize(read_file("target", "compiled", "test", "models", "jsonl_model.sql"))

        assert "format = 'jsonl'" in sql
        # Simple columns
        assert "event_id varchar(50)" in sql
        assert "event_type varchar(50)" in sql
        # Columns with JSON path mapping
        assert "user_id int '$.user.id'" in sql
        assert "user_name varchar(100)'$.user.name'" in sql
        assert "page varchar(200)'$.properties.page'" in sql
        assert "as [events]" in sql


class TestOpenrowsetParquetNoSchema:
    """Parquet file without column schema — engine auto-detects columns."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parquet_no_schema_model.sql": parquet_no_schema_model_sql,
            "sources.yml": sources_parquet_no_schema_yml,
        }

    def test__parquet_no_with_clause(self, project):
        results = run_dbt(["compile", "-s", "parquet_no_schema_model"])
        assert len(results) == 1

        sql = _normalize(
            read_file("target", "compiled", "test", "models", "parquet_no_schema_model.sql")
        )

        assert "openrowset(" in sql
        assert "format = 'parquet'" in sql
        assert "with(" not in sql
        assert "as [customers_auto]" in sql


class TestOpenrowsetExplicitFormat:
    """Explicit format override — uses format: CSV even though extension is .csv."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "explicit_format_model.sql": explicit_format_model_sql,
            "sources.yml": sources_explicit_format_yml,
        }

    def test__explicit_format(self, project):
        results = run_dbt(["compile", "-s", "explicit_format_model"])
        assert len(results) == 1

        sql = _normalize(
            read_file("target", "compiled", "test", "models", "explicit_format_model.sql")
        )

        assert "format = 'csv'" in sql
        assert "header_row = true" in sql
        assert "order_id int" in sql
        assert "as [orders_explicit]" in sql


class TestOpenrowsetFullPathOverride:
    """Full path at table level bypasses source base path."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "full_path_model.sql": full_path_model_sql,
            "sources.yml": sources_full_path_override_yml,
        }

    def test__full_path_override(self, project):
        results = run_dbt(["compile", "-s", "full_path_model"])
        assert len(results) == 1

        sql = _normalize(read_file("target", "compiled", "test", "models", "full_path_model.sql"))

        assert "bulk '{}/customers.parquet'".format(SAMPLE_DATA_PATH.lower()) in sql
        assert "format = 'parquet'" in sql
        assert "customer_id int" in sql
        assert "first_name varchar(100)" in sql
        assert "as [customers_direct]" in sql
