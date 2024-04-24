import pytest
from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

# from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)


class BaseSimpleMaterializations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": files.base_view_sql,
            "table_model.sql": files.base_table_sql,
            "swappable.sql": files.base_materialized_var_sql,
            "schema.yml": files.schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": files.seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        if project.test_config.get("require_full_refresh", False):  # required for BigQuery
            results = run_dbt(
                ["run", "--full-refresh", "-m", "swappable", "--vars", "materialized_var: view"]
            )
        else:
            results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestSingularTestsFabric(BaseSingularTests):
    pass


@pytest.mark.skip(reason="ephemeral not supported")
class TestSingularTestsEphemeralFabric(BaseSingularTestsEphemeral):
    pass


class TestEmptyFabric(BaseEmpty):
    pass


class TestEphemeralFabric(BaseEphemeral):
    pass


class TestIncrementalFabric(BaseIncremental):
    pass


# Modified incremental_not_schema_change.sql file to handle DATETIME compatibility issues.
@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestIncrementalNotSchemaChangeFabric(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_not_schema_change.sql": """
{{ config(materialized="incremental",
unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
CAST(1 + '-' + current_timestamp AS DATETIME2(6)) as user_id_current_time,
{% if is_incremental() %}
'thisis18characters' as platform
{% else %}
'okthisis20characters' as platform
{% endif %}
"""
        }


class TestGenericTestsFabric(BaseGenericTests):
    pass


class TestSnapshotCheckColsFabric(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampFabric(BaseSnapshotTimestamp):
    pass


# Assertion Failed.
class TestBaseCachingFabric(BaseAdapterMethod):
    pass


class TestValidateConnectionFabric(BaseValidateConnection):
    pass
