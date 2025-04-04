from pathlib import Path

import pytest

from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase
from dbt.tests.adapter.simple_copy.test_simple_copy import EmptyModelsArentRunBase, SimpleCopyBase
from dbt.tests.adapter.simple_seed import fixtures, seeds
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseBasicSeedTests,
    BaseSeedConfigFullRefreshOff,
    BaseSeedConfigFullRefreshOn,
    BaseSeedCustomSchema,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseSeedWithEmptyDelimiter,
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSimpleSeedWithBOM,
    BaseTestEmptySeed,
)
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride
from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSimpleSnapshotBase,
    BaseSnapshotCheck,
)
from dbt.tests.util import (
    copy_file,
)

fixed_seeds___expected_sql = (
    seeds.seeds__expected_sql.replace("TIMESTAMP WITHOUT TIME ZONE", "datetime2(6)")
    .replace("TEXT", "varchar(100)")
    .replace("INTEGER", "int")
)
fixed_properties__schema_yml = (
    fixtures.properties__schema_yml.replace("type: timestamp without time zone", "type: datetime2")
    .replace("type: text", "type: varchar")
    .replace("type: integer", "type: int")
    .replace("type: boolean", "type: bit")
)


class FixedSeedSetup:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_seeds___expected_sql)


class TestBasicSeedTestsFabric(FixedSeedSetup, BaseBasicSeedTests):
    def test_simple_seed_full_refresh_flag(self, project):
        pytest.skip(
            "This test assumes that if you drop a table, that it will cascade to all views"
        )


class TestEmptyModelsArentRunFabric(EmptyModelsArentRunBase):
    pass


class TestEmptySeedFabric(BaseTestEmptySeed):
    pass


class TestSeedConfigFullRefreshOffFabric(FixedSeedSetup, BaseSeedConfigFullRefreshOff):
    pass


class TestSeedConfigFullRefreshOnFabric(FixedSeedSetup, BaseSeedConfigFullRefreshOn):
    pass


class TestSeedCustomSchemaFabric(FixedSeedSetup, BaseSeedCustomSchema):
    pass


class TestSeedParsingFabric(FixedSeedSetup, BaseSeedParsing):
    pass


class TestSeedSpecificFormatsFabric(BaseSeedSpecificFormats):
    pass


class TestSeedWithEmptyDelimiterFabric(FixedSeedSetup, BaseSeedWithEmptyDelimiter):
    pass


class TestSeedWithUniqueDelimiterFabric(FixedSeedSetup, BaseSeedWithUniqueDelimiter):
    pass


class TestSeedWithWrongDelimiterFabric(FixedSeedSetup, BaseSeedWithWrongDelimiter):
    pass


class TestSimpleCopyBaseFabric(SimpleCopyBase):
    pass


class TestSimpleCopyUppercaseFabric(BaseSimpleCopyUppercase):
    pass


class TestSimpleSeedColumnOverrideFabric(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixed_properties__schema_yml,
        }


class TestSimpleSeedEnabledViaConfigFabric(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSimpleSeedWithBOMFabric(BaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixed_seeds___expected_sql)
        copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


class TestSimpleSnapshotBaseFabric(BaseSimpleSnapshotBase):
    pass


class TestSimpleSnapshotFabric(BaseSimpleSnapshot):
    pass


class TestSnapshotCheckFabric(BaseSnapshotCheck):
    pass
