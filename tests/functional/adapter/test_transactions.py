import pytest

from dbt.tests.util import patch_microbatch_end_time, run_dbt, write_file

FAIL_HOOK_CONFIG = """
{% if var('fail_hook', false) %}
  {{ config(post_hook=["select 1 / 0"]) }}
{% endif %}
"""

TABLE_MODEL = (
    FAIL_HOOK_CONFIG
    + """
{{ config(materialized='table') }}
select cast({{ var('value', 1) }} as int) as id,
       cast('{{ var("label", "original") }}' as varchar(20)) as label
"""
)

INCREMENTAL_MODEL = (
    FAIL_HOOK_CONFIG
    + """
{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}
select cast(1 as int) as id,
       cast('{{ var("label", "original") }}' as varchar(20)) as label
"""
)

MULTI_STATEMENT_INCREMENTAL_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='transaction_probe'
) }}
select cast(1 as int) as id, cast('original' as varchar(20)) as label
"""

MULTI_STATEMENT_STRATEGY = """
{% macro get_incremental_transaction_probe_sql(arg_dict) %}
  delete from {{ arg_dict['target_relation'] }};
  select 1 / 0;
{% endmacro %}
"""

VIEW_MODEL = (
    FAIL_HOOK_CONFIG
    + """
{{ config(materialized='view') }}
select cast({{ var('value', 1) }} as int) as id
"""
)

FAILING_PRE_HOOK_MODEL = """
{{ config(
    materialized='table',
    pre_hook=["insert into {{ this.schema }}.transaction_audit values ('inside')"]
) }}
select 1 / 0 as id
"""

FAILING_OUTSIDE_PRE_HOOK_MODEL = """
{{ config(
    materialized='table',
    pre_hook=[{
        "sql": "insert into {{ this.schema }}.transaction_audit values ('outside')",
        "transaction": false
    }]
) }}
select 1 / 0 as id
"""

FAILING_OUTSIDE_POST_HOOK_MODEL = """
{{ config(
    materialized='table',
    post_hook=[{"sql": "select 1 / 0", "transaction": false}]
) }}
select cast(1 as int) as id
"""

FAILING_STATISTICS_MODEL = """
{{ config(
    materialized='table',
    grants={'select': ['public']},
    statistics=['missing_column']
) }}
select cast(2 as int) as id, cast('replacement' as varchar(20)) as label
"""

METADATA_COMMIT_MODEL = """
{{ config(
    materialized='table',
    grants={'select': ['public']},
    statistics=['id']
) }}
select cast(1 as int) as id, cast('committed' as varchar(20)) as label
"""

SNAPSHOT = """
{% snapshot transaction_snapshot %}
{% set hooks = ["select 1 / 0"] if var('fail_hook', false) else [] %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    post_hook=hooks
) }}
select id, value, updated_at
{% if var('add_snapshot_column', false) %}
     , cast('new' as varchar(20)) as added_column
{% endif %}
from {{ ref('transaction_snapshot_source') }}
{% endsnapshot %}
"""


def _value(project, relation_name, column="label"):
    return project.run_sql(
        f"select {column} from {project.test_schema}.{relation_name} where id = 1",
        fetch="one",
    )[0]


def _object_id(project, relation_name):
    return project.run_sql(
        f"select object_id('{project.test_schema}.{relation_name}')",
        fetch="one",
    )[0]


def _leftover_relation_count(project, relation_name):
    return project.run_sql(
        f"""
        select count(*)
        from sys.objects o
        join sys.schemas s on s.schema_id = o.schema_id
        where s.name = '{project.test_schema}'
          and (
            o.name like '{relation_name}__dbt_%'
            or o.name = '{relation_name}_snapshot_staging_temp_view'
          )
        """,
        fetch="one",
    )[0]


def _column_names(project, relation_name):
    return {
        row[0]
        for row in project.run_sql(
            f"""
            select c.name
            from sys.columns c
            where c.object_id = object_id(
                '{project.test_schema}.{relation_name}'
            )
            """,
            fetch="all",
        )
    }


class TestMaterializationTransactions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "transaction_table.sql": TABLE_MODEL,
            "transaction_replace.sql": TABLE_MODEL,
            "transaction_recovery.sql": TABLE_MODEL,
            "transaction_incremental.sql": INCREMENTAL_MODEL,
            "transaction_schema_change.sql": (
                FAIL_HOOK_CONFIG
                + """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}
select cast(1 as int) as id,
       cast('{{ var("label", "original") }}' as varchar(20)) as label
{% if var('add_column', false) %}
     , cast('new' as varchar(20)) as added_column
{% endif %}
"""
            ),
            "multi_statement_incremental.sql": MULTI_STATEMENT_INCREMENTAL_MODEL,
            "transaction_view.sql": VIEW_MODEL,
            "transaction_type_swap.sql": TABLE_MODEL,
            "failing_inside_pre_hook.sql": FAILING_PRE_HOOK_MODEL,
            "failing_outside_pre_hook.sql": FAILING_OUTSIDE_PRE_HOOK_MODEL,
            "failing_outside_post_hook.sql": FAILING_OUTSIDE_POST_HOOK_MODEL,
            "statistics_rollback.sql": TABLE_MODEL,
            "metadata_commit.sql": METADATA_COMMIT_MODEL,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"transaction_probe.sql": MULTI_STATEMENT_STRATEGY}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "transaction_snapshot_source.csv": (
                "id,value,updated_at\n1,original,2024-01-01 00:00:00\n"
            )
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"transaction_snapshot.sql": SNAPSHOT}

    def test_failed_table_reload_restores_data_and_cleans_temporary_relations(self, project):
        run_dbt(["run", "-s", "transaction_table"])
        original_object_id = _object_id(project, "transaction_table")

        run_dbt(
            [
                "run",
                "-s",
                "transaction_table",
                "--vars",
                "{fail_hook: true, value: 2, label: replacement}",
            ],
            expect_pass=False,
        )

        assert _object_id(project, "transaction_table") == original_object_id
        assert _value(project, "transaction_table") == "original"
        assert _leftover_relation_count(project, "transaction_table") == 0

    def test_failed_table_replacement_restores_original_table(self, project):
        run_dbt(["run", "-s", "transaction_replace"])
        original_object_id = _object_id(project, "transaction_replace")

        write_file(
            FAIL_HOOK_CONFIG
            + """
            {{ config(materialized='table') }}
            select cast(2 as bigint) as id,
                   cast('replacement' as varchar(40)) as label,
                   cast('new' as varchar(10)) as added_column
            """,
            "models",
            "transaction_replace.sql",
        )

        run_dbt(
            ["run", "-s", "transaction_replace", "--vars", "{fail_hook: true}"],
            expect_pass=False,
        )

        assert _object_id(project, "transaction_replace") == original_object_id
        assert _value(project, "transaction_replace") == "original"
        assert _leftover_relation_count(project, "transaction_replace") == 0

    def test_failed_incremental_merge_restores_target(self, project):
        run_dbt(["run", "-s", "transaction_incremental"])

        run_dbt(
            [
                "run",
                "-s",
                "transaction_incremental",
                "--vars",
                "{fail_hook: true, label: replacement}",
            ],
            expect_pass=False,
        )

        assert _value(project, "transaction_incremental") == "original"
        assert _leftover_relation_count(project, "transaction_incremental") == 0

    def test_failed_incremental_schema_change_restores_schema_and_data(self, project):
        run_dbt(["run", "-s", "transaction_schema_change"])

        run_dbt(
            [
                "run",
                "-s",
                "transaction_schema_change",
                "--vars",
                "{fail_hook: true, add_column: true, label: replacement}",
            ],
            expect_pass=False,
        )

        assert _value(project, "transaction_schema_change") == "original"
        assert _column_names(project, "transaction_schema_change") == {"id", "label"}
        assert _leftover_relation_count(project, "transaction_schema_change") == 0

    def test_multi_statement_incremental_failure_rolls_back_earlier_delete(self, project):
        run_dbt(["run", "-s", "multi_statement_incremental"])

        run_dbt(["run", "-s", "multi_statement_incremental"], expect_pass=False)

        assert _value(project, "multi_statement_incremental") == "original"
        assert _leftover_relation_count(project, "multi_statement_incremental") == 0

    def test_failed_view_replacement_restores_original_view(self, project):
        run_dbt(["run", "-s", "transaction_view"])

        run_dbt(
            ["run", "-s", "transaction_view", "--vars", "{fail_hook: true, value: 2}"],
            expect_pass=False,
        )

        assert _value(project, "transaction_view", column="id") == 1
        assert _leftover_relation_count(project, "transaction_view") == 0

    def test_failed_table_to_view_swap_restores_original_table(self, project):
        run_dbt(["run", "-s", "transaction_type_swap"])
        original_object_id = _object_id(project, "transaction_type_swap")

        write_file(
            VIEW_MODEL,
            "models",
            "transaction_type_swap.sql",
        )
        run_dbt(
            [
                "run",
                "-s",
                "transaction_type_swap",
                "--vars",
                "{fail_hook: true, value: 2}",
            ],
            expect_pass=False,
        )

        assert _object_id(project, "transaction_type_swap") == original_object_id
        assert _value(project, "transaction_type_swap") == "original"
        assert _leftover_relation_count(project, "transaction_type_swap") == 0

    def test_transactional_pre_hook_rolls_back_with_model(self, project):
        project.run_sql(
            "drop table if exists {schema}.transaction_audit; "
            "create table {schema}.transaction_audit (location varchar(20));"
        )

        run_dbt(["run", "-s", "failing_inside_pre_hook"], expect_pass=False)

        assert (
            project.run_sql("select count(*) from {schema}.transaction_audit", fetch="one")[0] == 0
        )

    def test_outside_pre_hook_commits_before_model_transaction(self, project):
        project.run_sql(
            "drop table if exists {schema}.transaction_audit; "
            "create table {schema}.transaction_audit (location varchar(20));"
        )

        run_dbt(["run", "-s", "failing_outside_pre_hook"], expect_pass=False)

        assert (
            project.run_sql(
                "select count(*) from {schema}.transaction_audit where location = 'outside'",
                fetch="one",
            )[0]
            == 1
        )

    def test_outside_post_hook_failure_does_not_rollback_committed_model(self, project):
        run_dbt(["run", "-s", "failing_outside_post_hook"], expect_pass=False)

        assert _value(project, "failing_outside_post_hook", column="id") == 1

    def test_failed_statistics_rolls_back_data_grants_and_schema(self, project):
        run_dbt(["run", "-s", "statistics_rollback"])
        original_object_id = _object_id(project, "statistics_rollback")

        write_file(
            FAILING_STATISTICS_MODEL,
            "models",
            "statistics_rollback.sql",
        )
        run_dbt(["run", "-s", "statistics_rollback"], expect_pass=False)

        assert _object_id(project, "statistics_rollback") == original_object_id
        assert _value(project, "statistics_rollback") == "original"
        grant_count = project.run_sql(
            f"""
            select count(*)
            from sys.database_permissions p
            join sys.database_principals r
              on r.principal_id = p.grantee_principal_id
            where p.major_id = object_id(
                '{project.test_schema}.statistics_rollback'
            )
              and r.name = 'public'
              and p.permission_name = 'SELECT'
            """,
            fetch="one",
        )[0]
        assert grant_count == 0

    def test_grants_and_statistics_commit_with_model(self, project):
        run_dbt(["run", "-s", "metadata_commit"])

        grant_count = project.run_sql(
            f"""
            select count(*)
            from sys.database_permissions p
            join sys.database_principals r
              on r.principal_id = p.grantee_principal_id
            where p.major_id = object_id(
                '{project.test_schema}.metadata_commit'
            )
              and r.name = 'public'
              and p.permission_name = 'SELECT'
            """,
            fetch="one",
        )[0]
        statistics_count = project.run_sql(
            f"""
            select count(*)
            from sys.stats
            where object_id = object_id(
                '{project.test_schema}.metadata_commit'
            )
              and user_created = 1
            """,
            fetch="one",
        )[0]

        assert grant_count == 1
        assert statistics_count == 1

    def test_failed_snapshot_merge_restores_snapshot(self, project):
        run_dbt(["seed", "-s", "transaction_snapshot_source"])
        run_dbt(["snapshot", "-s", "transaction_snapshot"])

        project.run_sql(
            "update {schema}.transaction_snapshot_source "
            "set value = 'replacement', "
            "updated_at = cast('2024-02-01 00:00:00' as datetime2)"
        )
        run_dbt(
            ["snapshot", "-s", "transaction_snapshot", "--vars", "{fail_hook: true}"],
            expect_pass=False,
        )

        rows = project.run_sql(
            "select value, dbt_valid_to "
            "from {schema}.transaction_snapshot "
            "order by dbt_valid_from",
            fetch="all",
        )
        assert len(rows) == 1
        assert rows[0][0] == "original"
        assert rows[0][1] is None
        assert _leftover_relation_count(project, "transaction_snapshot") == 0

    def test_failed_snapshot_schema_change_restores_snapshot_schema(self, project):
        run_dbt(["seed", "-s", "transaction_snapshot_source"])
        run_dbt(["snapshot", "-s", "transaction_snapshot"])

        project.run_sql(
            "update {schema}.transaction_snapshot_source "
            "set value = 'replacement', "
            "updated_at = cast('2024-03-01 00:00:00' as datetime2)"
        )
        run_dbt(
            [
                "snapshot",
                "-s",
                "transaction_snapshot",
                "--vars",
                "{fail_hook: true, add_snapshot_column: true}",
            ],
            expect_pass=False,
        )

        assert "added_column" not in _column_names(project, "transaction_snapshot")
        rows = project.run_sql(
            "select value, dbt_valid_to "
            "from {schema}.transaction_snapshot "
            "order by dbt_valid_from",
            fetch="all",
        )
        assert len(rows) == 1
        assert rows[0][0] == "original"
        assert rows[0][1] is None
        assert _leftover_relation_count(project, "transaction_snapshot") == 0

    def test_failed_run_does_not_poison_next_connection(self, project):
        run_dbt(["run", "-s", "failing_inside_pre_hook"], expect_pass=False)

        results = run_dbt(["run", "-s", "transaction_recovery"])

        assert len(results) == 1
        assert _value(project, "transaction_recovery") == "original"


class TestSeedTransactionRollback:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"failing_transaction_seed.csv": "id,label\n1,original\n"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "failing_transaction_seed": {
                        "+post-hook": ["select 1 / {{ var('seed_divisor', 0) }}"],
                    }
                }
            }
        }

    def test_seed_post_hook_failure_rolls_back_table_creation(self, project):
        run_dbt(["seed", "-s", "failing_transaction_seed"], expect_pass=False)

        assert _object_id(project, "failing_transaction_seed") is None

    def test_seed_full_refresh_failure_restores_existing_table(self, project):
        run_dbt(
            [
                "seed",
                "-s",
                "failing_transaction_seed",
                "--vars",
                "{seed_divisor: 1}",
            ]
        )
        original_object_id = _object_id(project, "failing_transaction_seed")

        write_file(
            "id,label\n1,replacement\n2,new\n",
            "seeds",
            "failing_transaction_seed.csv",
        )
        run_dbt(
            ["seed", "-s", "failing_transaction_seed", "--full-refresh"],
            expect_pass=False,
        )

        assert _object_id(project, "failing_transaction_seed") == original_object_id
        assert _value(project, "failing_transaction_seed") == "original"

    def test_seed_truncate_failure_restores_existing_rows(self, project):
        write_file(
            "id,label\n1,original\n",
            "seeds",
            "failing_transaction_seed.csv",
        )
        run_dbt(
            [
                "seed",
                "-s",
                "failing_transaction_seed",
                "--vars",
                "{seed_divisor: 1}",
            ]
        )

        write_file(
            "id,label\n1,replacement\n2,new\n",
            "seeds",
            "failing_transaction_seed.csv",
        )
        run_dbt(
            ["seed", "-s", "failing_transaction_seed"],
            expect_pass=False,
        )

        assert _value(project, "failing_transaction_seed") == "original"
        assert (
            project.run_sql(
                "select count(*) from {schema}.failing_transaction_seed",
                fetch="one",
            )[0]
            == 1
        )


class TestMicrobatchTransactionRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "transaction_microbatch_input.sql": """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, cast('2020-01-01' as datetime2(6)) as event_time,
       cast('original' as varchar(20)) as label
union all
select 2 as id, cast('2020-01-02' as datetime2(6)) as event_time,
       cast('original' as varchar(20)) as label
union all
select 3 as id, cast('2020-01-03' as datetime2(6)) as event_time,
       cast('original' as varchar(20)) as label
""",
            "transaction_microbatch.sql": """
{% set hooks = ["select 1 / 0"] if var('fail_hook', false) else [] %}
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin='2020-01-01 00:00:00.000000',
    lookback=1,
    post_hook=hooks
) }}
select * from {{ ref('transaction_microbatch_input') }}
""",
        }

    def test_failed_batch_restores_rows_deleted_for_reprocessing(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])

        project.run_sql(
            "update {schema}.transaction_microbatch_input set label = 'replacement' where id = 3"
        )
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(
                [
                    "run",
                    "-s",
                    "transaction_microbatch",
                    "--vars",
                    "{fail_hook: true}",
                ],
                expect_pass=False,
            )

        assert (
            project.run_sql(
                "select label from {schema}.transaction_microbatch where id = 3",
                fetch="one",
            )[0]
            == "original"
        )


class TestFunctionTransactionRollback:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "failing_transaction_function.sql": "SELECT @price * 2",
            "failing_transaction_function.yml": """
functions:
  - name: failing_transaction_function
    config:
      post-hook:
        - "select 1 / {{ var('function_divisor', 0) }}"
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
""",
        }

    def test_function_post_hook_failure_rolls_back_creation(self, project):
        run_dbt(["build", "-s", "failing_transaction_function"], expect_pass=False)

        assert _object_id(project, "failing_transaction_function") is None

    def test_function_post_hook_failure_restores_previous_definition(self, project):
        run_dbt(
            [
                "build",
                "-s",
                "failing_transaction_function",
                "--vars",
                "{function_divisor: 1}",
            ]
        )

        write_file(
            "SELECT @price * 3",
            "functions",
            "failing_transaction_function.sql",
        )
        run_dbt(
            ["build", "-s", "failing_transaction_function"],
            expect_pass=False,
        )

        result = project.run_sql(
            f"select {project.test_schema}.failing_transaction_function(cast(5 as float))",
            fetch="one",
        )
        assert result[0] == 10


class TestParallelMaterializationTransactions:
    @pytest.fixture(scope="class")
    def dbt_profile_target_update(self):
        return {"threads": 4}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"parallel_transaction_{model_number}.sql": f"""
{{{{ config(materialized='table') }}}}
select cast({model_number} as int) as id
"""
            for model_number in range(1, 9)
        }

    def test_independent_materializations_commit_concurrently(self, project):
        results = run_dbt(["run"])

        assert len(results) == 8
        assert all(result.status == "success" for result in results)
        for model_number in range(1, 9):
            assert (
                project.run_sql(
                    f"select id from {project.test_schema}.parallel_transaction_{model_number}",
                    fetch="one",
                )[0]
                == model_number
            )
