import pytest

from dbt.tests.util import run_dbt

AUDIT_HELPER_PACKAGE = {
    "packages": [
        {"package": "dbt-labs/audit_helper", "version": "0.14.0"},
    ]
}

# Without this, dbt resolves the package's own default__ macros instead of the
# adapter's fabric__ overrides.
AUDIT_HELPER_DISPATCH = {
    "dispatch": [
        {
            "macro_namespace": "audit_helper",
            "search_order": ["dbt", "audit_helper"],
        }
    ]
}

# order_id 1 is identical, 2 differs, 3 only exists in a, 4 only exists in b,
# 5 has a duplicated primary key in a, 6 is identical including a null status,
# and 7 differs on a null status. Blank status cells load as null.
A_CSV = """order_id,status,amount
1,new,100
2,new,200
3,new,300
5,new,500
5,new,500
6,,600
7,,700
"""

B_CSV = """order_id,status,amount
1,new,100
2,paid,200
4,new,400
5,new,500
6,,600
7,paid,700
"""

CLASSIFIED_SQL = """
{{ config(materialized="table") }}
{% set a_query %}select order_id, status, amount from {{ ref("audit_helper_a") }}{% endset %}
{% set b_query %}select order_id, status, amount from {{ ref("audit_helper_b") }}{% endset %}
{{ audit_helper.compare_and_classify_query_results(
    a_query=a_query,
    b_query=b_query,
    primary_key_columns=["order_id"],
    columns=["order_id", "status", "amount"]
) }}
"""


class TestAuditHelperClassification:
    @pytest.fixture(scope="class")
    def packages(self):
        return AUDIT_HELPER_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return AUDIT_HELPER_DISPATCH

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"audit_helper_a.csv": A_CSV, "audit_helper_b.csv": B_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"audit_helper_classified.sql": CLASSIFIED_SQL}

    def test_compare_and_classify_query_results(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 2
        assert len(run_dbt(["run"])) == 1

        per_order = project.run_sql(
            f"select distinct order_id, dbt_audit_row_status "
            f"from {project.test_schema}.audit_helper_classified order by order_id",
            fetch="all",
        )
        assert [tuple(row) for row in per_order] == [
            (1, "identical"),
            (2, "modified"),
            (3, "removed"),
            (4, "added"),
            (5, "nonunique_pk"),
            (6, "identical"),
            (7, "modified"),
        ]

        # dbt_audit_num_rows_in_status counts distinct (surrogate key, pk row number)
        # pairs, so the two sides of a modified row count once: 4 rows, 2 differences.
        per_status = project.run_sql(
            f"select dbt_audit_row_status, count(*), max(dbt_audit_num_rows_in_status) "
            f"from {project.test_schema}.audit_helper_classified "
            f"group by dbt_audit_row_status order by dbt_audit_row_status",
            fetch="all",
        )
        assert [tuple(row) for row in per_status] == [
            ("added", 1, 1),
            ("identical", 2, 2),
            ("modified", 4, 2),
            ("nonunique_pk", 2, 2),
            ("removed", 1, 1),
        ]


class TestAuditHelperComparisons:
    @pytest.fixture(scope="class")
    def packages(self):
        return AUDIT_HELPER_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return AUDIT_HELPER_DISPATCH

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "columns_a.sql": """
                select 1 as id, cast('abc' as varchar(10)) as label,
                    cast(1 as decimal(10,2)) as amount, 1 as only_a
            """,
            "columns_b.sql": """
                select 1 as id, cast('abc' as varchar(20)) as label,
                    cast(1 as decimal(12,3)) as amount, 1 as only_b
            """,
            "metadata_comparison.sql": """
                {{ config(materialized='table') }}
                {{ audit_helper.compare_relation_columns(ref('columns_a'), ref('columns_b')) }}
            """,
            "all_columns_comparison.sql": """
                {{ audit_helper.compare_all_columns(
                    ref('columns_a'), ref('columns_b'), primary_key='id', exclude_columns=['only_a']
                ) }}
            """,
            "which_columns_differ.sql": """
                {% set a %}
                    select 1 as [id key], 10 as [select], 1 as [customer's value],
                        cast('2024-01-01' as date) as updated_at
                    union all select 2, 20, 2, cast('2024-01-02' as date)
                    union all select 3, 30, 3, cast('2024-01-03' as date)
                {% endset %}
                {% set b %}
                    select 2 as [id key], 20 as [select], 2 as [customer's value],
                        cast('2024-01-02' as date) as updated_at
                    union all select 3, 31, 3, cast('2024-01-03' as date)
                    union all select 4, 40, 4, cast('2024-01-04' as date)
                {% endset %}
                {{ audit_helper.compare_which_query_columns_differ(
                    a, b, primary_key_columns=['id key'],
                    columns=['select', "customer's value"], event_time='updated_at'
                ) }}
            """,
            "query_comparison.sql": """
                {{ audit_helper.compare_queries(
                    'select 1 as id union all select 2', 'select 1 as id union all select 3',
                    summarize=false, limit=1
                ) }}
            """,
        }

    def test_metadata_event_bounds_and_comparison_contracts(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["run", "--select", "columns_a", "columns_b"])) == 2
        assert len(run_dbt(["run", "--exclude", "columns_a", "columns_b"])) == 4
        metadata = {
            row[0]: tuple(row[1:])
            for row in project.run_sql(
                f"select column_name, has_data_type_match, in_a_only, in_b_only, in_both "
                f"from {project.test_schema}.metadata_comparison",
                fetch="all",
            )
        }
        assert metadata == {
            "id": (1, 0, 0, 1),
            "label": (0, 0, 0, 1),
            "amount": (0, 0, 0, 1),
            "only_a": (0, 1, 0, 0),
            "only_b": (0, 0, 1, 0),
        }
        assert dict(
            project.run_sql(
                f"select * from {project.test_schema}.which_columns_differ", fetch="all"
            )
        ) == {"id key": 0, "select": 1, "customer's value": 0}
        assert [
            tuple(row)
            for row in project.run_sql(
                f"select column_name, perfect_match, conflicting_values "
                f"from {project.test_schema}.all_columns_comparison order by column_name",
                fetch="all",
            )
        ] == [("AMOUNT", 1, 0), ("ID", 1, 0), ("LABEL", 1, 0)]
        assert (
            project.run_sql(
                f"select count(*) from {project.test_schema}.query_comparison", fetch="one"
            )[0]
            == 1
        )
