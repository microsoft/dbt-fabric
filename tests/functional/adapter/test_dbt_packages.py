from datetime import datetime

import pytest

from dbt.tests.util import run_dbt

EPOCH_CASES = [
    ("seconds", 0, "1970-01-01T00:00:00"),
    ("seconds", -1, "1969-12-31T23:59:59"),
    ("seconds", 2147483648, "2038-01-19T03:14:08"),
    ("seconds", 4102444800, "2100-01-01T00:00:00"),
    ("milliseconds", 4102444800123, "2100-01-01T00:00:00.123000"),
    ("microseconds", 4102444800123456, "2100-01-01T00:00:00.123456"),
    ("nanoseconds", 4102444800123456789, "2100-01-01T00:00:00.123456"),
    ("milliseconds", -1, "1969-12-31T23:59:59.999000"),
    ("microseconds", -1, "1969-12-31T23:59:59.999999"),
    ("nanoseconds", -1000, "1969-12-31T23:59:59.999999"),
    ("seconds", 253402300799, "9999-12-31T23:59:59"),
    ("microseconds", -62135596800000000, "0001-01-01T00:00:00"),
]


class TestDbtDateFabricSupport:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "godatadriven/dbt_date", "version": "0.21.0"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"dispatch": [{"macro_namespace": "dbt_date", "search_order": ["dbt", "dbt_date"]}]}

    @pytest.fixture(scope="class")
    def models(self):
        rows = []
        for index, (format, value, _) in enumerate(EPOCH_CASES):
            rows.append(
                f"select {index} as id, "
                "{{ dbt_date.from_unixtimestamp("
                f"'{value}', format='{format}'"
                ") }} as event_time"
            )
        return {
            "epochs.sql": (
                "{{ config(materialized='table') }}\n"
                "with converted as (\n"
                + "\nunion all\n".join(rows)
                + "\n)\nselect id, event_time, "
                "{{ dbt_date.to_unixtimestamp('event_time') }} as epoch_seconds from converted"
            ),
        }

    def test_epoch_conversion_precision_and_range(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["run"])) == 1
        rows = project.run_sql(
            f"select id, event_time, epoch_seconds from {project.test_schema}.epochs order by id",
            fetch="all",
        )
        assert len(rows) == len(EPOCH_CASES)
        epoch = datetime(1970, 1, 1)
        for row, (_, _, expected) in zip(rows, EPOCH_CASES, strict=True):
            expected_time = datetime.fromisoformat(expected)
            assert row[1] == expected_time
            assert row[2] == (expected_time - epoch).days * 86400 + (expected_time - epoch).seconds


EXPECTATIONS_SCHEMA = """
version: 2
models:
  - name: baseline
    data_tests:
      - dbt_expectations.expect_table_aggregation_to_equal_other_table:
          name: fractional_difference_passes
          arguments:
            expression: sum(amount)
            compare_model: ref('comparison')
            tolerance_percent: 0.2
      - dbt_expectations.expect_table_aggregation_to_equal_other_table:
          name: fractional_difference_fails
          arguments:
            expression: sum(amount)
            compare_model: ref('comparison')
            tolerance_percent: 0.05
          config:
            tags: [expected_failure]
  - name: unique_values
    data_tests:
      - dbt_expectations.expect_select_column_values_to_be_unique_within_record:
          name: ignore_any_missing
          arguments:
            column_list: ['first value', 'second value']
            quote_columns: true
            ignore_row_if: any_value_is_missing
      - dbt_expectations.expect_select_column_values_to_be_unique_within_record:
          name: row_condition_applies
          arguments:
            column_list: ['first value', 'second value']
            quote_columns: true
            ignore_row_if: all_values_are_missing
            row_condition: "[first value] is not null"
      - dbt_expectations.expect_select_column_values_to_be_unique_within_record:
          name: retain_partly_missing
          arguments:
            column_list: ['first value', 'second value']
            quote_columns: true
            ignore_row_if: all_values_are_missing
          config:
            tags: [expected_failure]
  - name: constant_values
    columns:
      - name: amount
        data_tests:
          - dbt_expectations.expect_column_values_to_be_within_n_stdevs:
              name: constant_values_pass
          - dbt_expectations.expect_column_values_to_be_within_n_stdevs:
              name: grouped_constant_values_pass
              arguments:
                group_by: [bucket, segment]
  - name: fractional_values
    columns:
      - name: amount
        data_tests:
          - dbt_expectations.expect_column_values_to_be_within_n_stdevs:
              name: fractional_average_passes
              arguments:
                sigma_threshold: 0.9
  - name: outliers
    columns:
      - name: amount
        data_tests:
          - dbt_expectations.expect_column_values_to_be_within_n_stdevs:
              name: outlier_fails
              arguments:
                sigma_threshold: 1
              config:
                tags: [expected_failure]
          - dbt_expectations.expect_column_values_to_be_within_n_stdevs:
              name: grouped_outlier_fails
              arguments:
                group_by: [bucket, segment]
                sigma_threshold: 1
              config:
                tags: [expected_failure]
  - name: recent_values
    data_tests:
      - dbt_expectations.expect_grouped_row_values_to_have_recent_data:
          name: ungrouped_recency_passes
          arguments:
            group_by: []
            timestamp_column: event_time
            datepart: day
            interval: 1
      - dbt_expectations.expect_grouped_row_values_to_have_recent_data:
          name: grouped_recency_passes
          arguments:
            group_by: [bucket, segment]
            timestamp_column: event_time
            datepart: day
            interval: 1
      - dbt_expectations.expect_grouped_row_values_to_have_recent_data:
          name: empty_recency_fails
          arguments:
            group_by: []
            timestamp_column: event_time
            datepart: day
            interval: 1
            row_condition: "1 = 0"
          config:
            tags: [expected_failure]
"""


class TestDbtExpectationsFabricSupport:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "metaplane/dbt_expectations", "version": "0.10.10"},
                {"package": "godatadriven/dbt_date", "version": "0.21.0"},
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {"dbt_date:time_zone": "UTC"},
            "dispatch": [
                {
                    "macro_namespace": "dbt_expectations",
                    "search_order": ["dbt", "dbt_expectations"],
                },
                {"macro_namespace": "dbt_date", "search_order": ["dbt", "dbt_date"]},
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "baseline.sql": "select 10 as amount",
            "comparison.sql": "select 9 as amount",
            "unique_values.sql": """
select 1 as [first value], cast('2' as varchar(10)) as [second value]
union all select null, ''
union all select null, null
""",
            "constant_values.sql": """
select 'a' as bucket, 'x' as segment, 10 as amount
union all select 'b', 'x', 10
union all select 'c', 'y', 10
""",
            "fractional_values.sql": "select 0 as amount union all select 1",
            "outliers.sql": """
select 1 as bucket, 'x' as segment, 0 as amount
union all select 2, 'x', 0
union all select 3, 'y', 0
union all select 4, 'y', 0
union all select 5, 'y', 100
""",
            "recent_values.sql": """
select 'a' as bucket, 'x' as segment, {{ dbt.current_timestamp() }} as event_time
""",
            "schema.yml": EXPECTATIONS_SCHEMA,
        }

    def test_expectations_return_correct_failure_counts(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["run"])) == 7
        passing = run_dbt(["test", "--exclude", "tag:expected_failure"])
        assert len(passing) == 8
        assert all(result.failures == 0 for result in passing)
        failing = run_dbt(["test", "--select", "tag:expected_failure"], expect_pass=False)
        assert len(failing) == 5
        assert all(result.status == "fail" and result.failures == 1 for result in failing)


class TestNativePackageFabricSupport:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.4.1"},
                {"package": "elementary-data/elementary", "version": "0.26.1"},
                {"package": "dbt-labs/dbt_project_evaluator", "version": "1.4.1"},
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"native_package_subject.sql": "select 1 as id"}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "verify_native_package_macros.sql": """
{% macro verify_native_package_macros() %}
    {% set sql %}
        select
            {{ elementary.edr_datediff("'2000-01-01'", "'2000-01-03'", "day") }} as elapsed,
            {{ dbt_project_evaluator.bool_literal(true) }} as enabled,
            {{ dbt_project_evaluator.get_dbtreplace_directory_pattern() }} as directory_path
        from (select 'models/marts/orders.sql' as file_path) paths
    {% endset %}
    {% set rows = run_query(sql) %}
    {% if rows | length != 1
        or rows[0][0] != 2
        or rows[0][1] != 1
        or rows[0][2] != 'models/marts/' %}
        {{ exceptions.raise_compiler_error("Unexpected native package macro results: " ~ rows) }}
    {% endif %}
{% endmacro %}
"""
        }

    def test_native_macro_dispatch_and_sql(self, project):
        run_dbt(["deps"])
        manifest = run_dbt(["parse"])
        assert "macro.elementary.fabric__edr_datediff" in manifest.macros
        assert "macro.dbt_project_evaluator.fabric__recursive_dag" in manifest.macros
        run_dbt(["run-operation", "verify_native_package_macros"])
