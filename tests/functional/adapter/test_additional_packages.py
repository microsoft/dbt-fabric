import json
import subprocess
from pathlib import Path

import mssql_python
import pytest

from dbt.tests.util import run_dbt


def dispatch(package):
    return {"macro_namespace": package, "search_order": ["dbt", package]}


class TestDbtProfilerFabric:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "data-mie/dbt_profiler", "version": "1.0.0"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"dispatch": [dispatch("dbt_profiler")]}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "profile_input.sql": """
                {{ config(materialized='table') }}
                select 'a' as bucket, 1 as amount, cast(1 as bit) as flag
                union all select 'a', 2, cast(0 as bit)
                union all select 'a', null, cast(null as bit)
                union all select 'b', 10, cast(1 as bit)
                union all select 'b', 20, cast(1 as bit)
            """,
            "profile_all.sql": "{{ dbt_profiler.get_profile(ref('profile_input')) }}",
            "profile_grouped.sql": """
                {{ config(materialized='table') }}
                {{ dbt_profiler.get_profile(
                    ref('profile_input'), include_columns=['amount'], group_by=['bucket']
                ) }}
            """,
            "profile_empty.sql": """
                {{ dbt_profiler.get_profile(
                    ref('profile_input'), exclude_columns=['bucket', 'flag'], where_clause='1 = 0'
                ) }}
            """,
            "profile_filtered.sql": """
                {{ dbt_profiler.get_profile(
                    ref('profile_input'), include_columns=['amount'],
                    where_clause="bucket = 'a'", exclude_measures=['median', 'std_dev_sample']
                ) }}
            """,
            "profile_large_input.sql": """
                {{ config(materialized='table') }}
                select 2147483640 as amount,
                    cast(12345678901234567890123456789012345678 as decimal(38,0)) as wide_value
                union all select 2147483647, 12345678901234567890123456789012345678
            """,
            "profile_large.sql": "{{ dbt_profiler.get_profile(ref('profile_large_input')) }}",
        }

    def test_profiles_materialize_with_fractional_and_grouped_statistics(self, project):
        run_dbt(["deps"])
        assert len(run_dbt(["run"])) == 7
        rows = project.run_sql(
            f"select bucket, row_count, avg, median, std_dev_population, std_dev_sample "
            f"from {project.test_schema}.profile_grouped order by bucket",
            fetch="all",
        )
        assert len(rows) == 2
        assert tuple(rows[0][:4]) == ("a", 3, 1.5, 1.5)
        assert rows[0][4] == pytest.approx(0.5)
        assert rows[0][5] == pytest.approx(2**-0.5)
        assert tuple(rows[1][:4]) == ("b", 2, 15, 15)
        flag_average = project.run_sql(
            f"select avg from {project.test_schema}.profile_all where column_name = 'flag'",
            fetch="one",
        )[0]
        assert flag_average == pytest.approx(0.75)
        assert tuple(
            project.run_sql(
                f"select row_count, avg, median from {project.test_schema}.profile_empty",
                fetch="one",
            )
        ) == (0, None, None)
        assert tuple(
            project.run_sql(
                f"select row_count, avg from {project.test_schema}.profile_filtered", fetch="one"
            )
        ) == (3, 1.5)
        assert tuple(
            project.run_sql(
                f"select avg, std_dev_population from {project.test_schema}.profile_large "
                "where column_name = 'amount'",
                fetch="one",
            )
        ) == (2147483643.5, 3.5)
        assert (
            project.run_sql(
                f"select min from {project.test_schema}.profile_large "
                "where column_name = 'wide_value'",
                fetch="one",
            )[0]
            == "12345678901234567890123456789012345678"
        )


class TestDbtConstraintsFabric:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "Snowflake-Labs/dbt_constraints", "version": "1.0.10"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"dispatch": [dispatch("dbt_constraints")]}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent.sql": "{{ config(materialized='table') }} select 1 as id, 11 as code",
            "child.sql": (
                "{{ config(materialized='table') }} select id as parent_id from {{ ref('parent') }}"
            ),
            "schema.yml": """
version: 2
models:
  - name: parent
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
        data_tests:
          - dbt_constraints.primary_key
      - name: code
        data_type: int
        data_tests:
          - dbt_constraints.unique_key
  - name: child
    columns:
      - name: parent_id
        data_tests:
          - dbt_constraints.foreign_key:
              arguments:
                pk_table_name: ref('parent')
                pk_column_name: id
""",
        }

    def test_keys_are_created_after_passing_tests_and_are_idempotent(self, project):
        probe = f"[{project.test_schema}].[nullability_probe]"
        project.run_sql(f"create table {probe} (id int null)")
        with pytest.raises(
            mssql_python.ProgrammingError, match="requires data validation or rewrite"
        ):
            project.run_sql(f"alter table {probe} alter column id int not null")
        run_dbt(["deps"])
        run_dbt(["build"])
        for command in (["test"], ["build"], ["test"]):
            run_dbt(command)
            keys = project.run_sql(
                "select kc.type from sys.key_constraints kc "
                "join sys.tables t on t.object_id = kc.parent_object_id "
                f"where t.schema_id = schema_id('{project.test_schema}') order by kc.type",
                fetch="all",
            )
            assert [row[0] for row in keys] == ["PK", "UQ"]
            assert (
                project.run_sql(
                    "select count(*) from sys.foreign_keys fk "
                    "join sys.tables t on t.object_id = fk.parent_object_id "
                    f"where t.schema_id = schema_id('{project.test_schema}')",
                    fetch="one",
                )[0]
                == 1
            )


class TestDbtArtifactsFabric:
    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "brooklyn-data/dbt_artifacts", "version": "2.11.1"}]}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "dispatch": [dispatch("dbt_artifacts")],
            "on-run-end": ["{{ dbt_artifacts.upload_results(results) }}"],
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"artifact_seed.csv": "id\n1\n2\n"}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "artifact_model.sql": """
                {{ config(materialized='table', meta={'note': "O'Brien"}) }}
                select id, cast('O''Brien' as varchar(100)) as label
                from {{ ref('artifact_seed') }}
            """,
            "schema.yml": """
version: 2
models:
  - name: artifact_model
    columns:
      - name: id
        data_tests: [unique, not_null]
sources:
  - name: artifact_source
    schema: "{{ target.schema }}"
    tables:
      - name: artifact_seed
exposures:
  - name: artifact_dashboard
    type: dashboard
    owner:
      name: Test Owner
    depends_on: [ref('artifact_model')]
""",
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "artifact_snapshot.sql": """
                {% snapshot artifact_snapshot %}
                {{ config(
                    target_schema=target.schema, unique_key='id',
                    strategy='check', check_cols=['label']
                ) }}
                select * from {{ ref('artifact_model') }}
                {% endsnapshot %}
            """
        }

    def test_all_upload_datasets_and_core_models(self, project):
        run_dbt(["deps"])
        # These non-dispatched upstream model branches need a separate upstream release.
        patch = Path(__file__).parent / "fixtures/dbt_artifacts_fabric.patch"
        subprocess.run(
            ["git", "apply", str(patch)],
            cwd=Path(project.project_root) / "dbt_packages/dbt_artifacts",
            check=True,
        )
        run_dbt(["run", "--select", "package:dbt_artifacts"])
        run_dbt(["seed"])
        run_dbt(["run", "--select", "artifact_model"])
        run_dbt(["snapshot"])
        run_dbt(["test", "--select", "artifact_model"])
        run_dbt(["run", "--select", "package:dbt_artifacts"])
        for dataset in (
            "invocations",
            "models",
            "seeds",
            "snapshots",
            "sources",
            "tests",
            "exposures",
            "model_executions",
            "seed_executions",
            "snapshot_executions",
            "test_executions",
        ):
            assert (
                project.run_sql(
                    f"select count(*) from {project.test_schema}.[{dataset}]", fetch="one"
                )[0]
                > 0
            ), dataset
        metadata = project.run_sql(
            f"select top 1 meta from {project.test_schema}.models where name = 'artifact_model'",
            fetch="one",
        )[0]
        assert json.loads(metadata)["note"] == "O'Brien"
        assert (
            project.run_sql(
                f"select top 1 rows_affected from {project.test_schema}.seed_executions "
                "where name = 'artifact_seed'",
                fetch="one",
            )[0]
            == 2
        )
        test_rows = project.run_sql(
            f"select compile_started_at, query_completed_at, failures "
            f"from {project.test_schema}.test_executions",
            fetch="all",
        )
        assert len(test_rows) == 2
        assert all(row[0] is not None and row[1] is not None and row[2] == 0 for row in test_rows)
