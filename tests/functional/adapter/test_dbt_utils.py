import pytest

from dbt.tests.util import run_dbt

DBT_UTILS_PACKAGE = {
    "packages": [
        {"package": "dbt-labs/dbt_utils", "version": "1.4.1"},
    ]
}

DBT_UTILS_DISPATCH = {
    "dispatch": [
        {
            "macro_namespace": "dbt_utils",
            "search_order": ["dbt", "dbt_utils"],
        }
    ]
}

SUBJECT_CSV = """id,status,amount,lower_bound,upper_bound,url
1,new,190000,0,10,https://example.com/orders/1?utm_source=email&utm_medium=campaign
2,new,290000,10,20,http://example.com/orders/2?utm_source=search
3,paid,400000,20,30,android-app://example.app/orders/3
"""

REFERENCE_CSV = """status
new
new
paid
"""

LARGER_CSV = """id,status
1,new
2,new
3,paid
4,paid
"""

SERIES_SQL = """
{{ config(materialized="table") }}
{{ dbt_utils.generate_series(upper_bound=20) }}
"""

DATE_SPINE_SQL = """
{{ config(materialized="table") }}
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2018-01-01' as date)",
    end_date="cast('2018-02-01' as date)"
) }}
"""

DEDUPLICATE_SQL = """
{{ config(materialized="table") }}
with duplicated as (
    select id, status, 1 as row_priority from {{ ref("dbt_utils_subject") }}
    union all
    select id, concat('duplicate_', status), 2 as row_priority
    from {{ ref("dbt_utils_subject") }}
)
{{ dbt_utils.deduplicate(
    relation="duplicated",
    partition_by="id",
    order_by="row_priority asc"
) }}
"""

WIDTH_BUCKET_SQL = """
{{ config(materialized="table") }}
select
    id,
    {{ dbt_utils.width_bucket("amount", 200000, 600000, 4) }} as amount_bucket
from {{ ref("dbt_utils_subject") }}
"""

URL_SQL = """
{{ config(materialized="table") }}
select
    id,
    {{ dbt_utils.get_url_host("url") }} as url_host,
    {{ dbt_utils.get_url_path("url") }} as url_path,
    {{ dbt_utils.get_url_parameter("url", "utm_source") }} as utm_source,
    {{ dbt_utils.get_url_parameter("url", "utm_medium") }} as utm_medium
from {{ ref("dbt_utils_subject") }}
"""

RELATIONS_SQL = """
{{ config(materialized="table") }}
{% set pattern_relations = dbt_utils.get_relations_by_pattern(
    schema_pattern=target.schema,
    table_pattern='dbt_utils_%'
) %}
{% set prefix_relations = dbt_utils.get_relations_by_prefix(
    schema=target.schema,
    prefix='dbt_utils_'
) %}
select
    {{ pattern_relations | length }} as pattern_count,
    {{ prefix_relations | length }} as prefix_count
"""

SCHEMA_YML = """
version: 2
seeds:
  - name: dbt_utils_subject
    data_tests:
      - dbt_utils.fewer_rows_than:
          arguments:
            compare_model: ref('dbt_utils_larger')
      - dbt_utils.cardinality_equality:
          arguments:
            column_name: status
            to: ref('dbt_utils_reference')
            field: status
      - dbt_utils.mutually_exclusive_ranges:
          arguments:
            lower_bound_column: lower_bound
            upper_bound_column: upper_bound
            gaps: not_allowed
"""


class TestDbtUtilsFabricSupport:
    @pytest.fixture(scope="class")
    def packages(self):
        return DBT_UTILS_PACKAGE

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return DBT_UTILS_DISPATCH

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "dbt_utils_subject.csv": SUBJECT_CSV,
            "dbt_utils_reference.csv": REFERENCE_CSV,
            "dbt_utils_larger.csv": LARGER_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dbt_utils_series.sql": SERIES_SQL,
            "dbt_utils_date_spine.sql": DATE_SPINE_SQL,
            "dbt_utils_deduplicate.sql": DEDUPLICATE_SQL,
            "dbt_utils_width_bucket.sql": WIDTH_BUCKET_SQL,
            "dbt_utils_urls.sql": URL_SQL,
            "dbt_utils_relations.sql": RELATIONS_SQL,
            "schema.yml": SCHEMA_YML,
        }

    def test_dbt_utils_macros_and_tests(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 3
        assert len(run_dbt(["run"])) == 6
        assert len(run_dbt(["test"])) == 3

        series = project.run_sql(
            f"select min(generated_number), max(generated_number), count(*) "
            f"from {project.test_schema}.dbt_utils_series",
            fetch="one",
        )
        assert tuple(series) == (1, 20, 20)

        date_spine = project.run_sql(
            f"select min(date_day), max(date_day), count(*) "
            f"from {project.test_schema}.dbt_utils_date_spine",
            fetch="one",
        )
        assert str(date_spine[0]).startswith("2018-01-01")
        assert str(date_spine[1]).startswith("2018-01-31")
        assert date_spine[2] == 31

        deduplicated = project.run_sql(
            f"select count(*), max(row_priority) from {project.test_schema}.dbt_utils_deduplicate",
            fetch="one",
        )
        assert tuple(deduplicated) == (3, 1)

        buckets = project.run_sql(
            f"select amount_bucket from {project.test_schema}.dbt_utils_width_bucket order by id",
            fetch="all",
        )
        assert [row[0] for row in buckets] == [0, 1, 3]

        urls = project.run_sql(
            f"select url_host, url_path, utm_source, utm_medium "
            f"from {project.test_schema}.dbt_utils_urls order by id",
            fetch="all",
        )
        assert [tuple(row) for row in urls] == [
            ("example.com", "/orders/1", "email", "campaign"),
            ("example.com", "/orders/2", "search", None),
            ("example.app", "/orders/3", None, None),
        ]

        relation_counts = project.run_sql(
            f"select pattern_count, prefix_count from {project.test_schema}.dbt_utils_relations",
            fetch="one",
        )
        assert relation_counts[0] >= 3
        assert relation_counts[1] >= 3
