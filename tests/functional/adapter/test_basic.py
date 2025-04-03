import pytest

from dbt.tests.adapter.basic import expected_catalog
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    ref_models__docs_md,
    ref_models__ephemeral_copy_sql,
    ref_models__ephemeral_summary_sql,
    ref_models__schema_yml,
    ref_sources__schema_yml,
)
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
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestSingularTestsFabric(BaseSingularTests):
    pass


class TestSingularTestsEphemeralFabric(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "singular_tests_ephemeral",
            "models": {
                "+materialized": "table"  # no support for nested CTEs in Views yet
            },
        }


class TestEmptyFabric(BaseEmpty):
    pass


class TestEphemeralFabric(BaseEphemeral):
    pass


class TestIncrementalFabric(BaseIncremental):
    pass


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


class TestBaseCachingFabric(BaseAdapterMethod):
    pass


class TestValidateConnectionFabric(BaseValidateConnection):
    pass


class TestDocsGenerateFabric(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_catalog.base_expected_catalog(
            project,
            role=profile_user,
            id_type="int",
            text_type="varchar",
            time_type="datetime2",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=expected_catalog.no_stats(),
        )


class TestDocsGenReferencesFabric(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_catalog.expected_references_catalog(
            project,
            role=profile_user,
            id_type="int",
            text_type="varchar",
            time_type="datetime2",
            bigint_type="int",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=expected_catalog.no_stats(),
        )

    @pytest.fixture(scope="class")
    def models(self):
        ref_models__view_summary_sql = """
{{
  config(
    materialized = "view"
  )
}}

select first_name, ct from {{ref('ephemeral_summary')}}
"""

        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": ref_sources__schema_yml,
            "view_summary.sql": ref_models__view_summary_sql,
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }


class TestTableMaterializationFabric(BaseTableMaterialization):
    pass
