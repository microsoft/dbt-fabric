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
