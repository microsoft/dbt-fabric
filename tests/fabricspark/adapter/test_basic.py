import pytest

from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_get_catalog_for_single_relation import (
    BaseGetCatalogForSingleRelation,
)
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalBadStrategy,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)


class TestSimpleMaterializationsSpark(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": """
  {{ config(materialized="materialized_view") }}
"""
            + files.model_base,
            "table_model.sql": files.base_table_sql,
            "swappable.sql": files.base_materialized_var_sql,
            "schema.yml": files.schema_base_yml,
        }

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
            "view_model": "materialized_view",
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

        results = run_dbt(
            ["run", "-s", "swappable", "--vars", "materialized_var: materialized_view"]
        )
        assert len(results) == 1

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "materialized_view",
            "table_model": "table",
            "swappable": "materialized_view",
        }
        check_relation_types(project.adapter, expected)

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-s", "swappable", "--vars", "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "materialized_view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSingularTestsSpark(BaseSingularTests):
    pass


class TestSingularTestsEphemeralSpark(BaseSingularTestsEphemeral):
    pass


class TestEmptySpark(BaseEmpty):
    pass


class TestEphemeralSpark(BaseEphemeral):
    pass


class TestIncrementalSpark(BaseIncremental):
    pass


class TestIncrementalNotSchemaChangeFabric(BaseIncrementalNotSchemaChange):
    pass


class TestGenericTestsSpark(BaseGenericTests):
    pass


class TestSnapshotCheckColsSpark(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampSpark(BaseSnapshotTimestamp):
    pass


class TestBaseCachingSpark(BaseAdapterMethod):
    pass


class TestValidateConnectionSpark(BaseValidateConnection):
    pass


class TestDocsGenerateSpark(BaseDocsGenerate):
    pass


class TestDocsGenReferencesSpark(BaseDocsGenReferences):
    pass


class TestTableMaterializationSpark(BaseTableMaterialization):
    pass


@pytest.mark.skip(reason="Capability not implemented in FabricSpark.")
class TestGetCatalogForSingleRelationSpark(BaseGetCatalogForSingleRelation):
    pass


class TestIncrementalBadStrategySpark(BaseIncrementalBadStrategy):
    def test_incremental_invalid_strategy(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # try to run the incremental model, it should fail on the first attempt
        results = run_dbt(["run"], expect_pass=False)
        assert len(results.results) == 1
        assert "Invalid incremental strategy provided: bad_strategy" in results.results[0].message
