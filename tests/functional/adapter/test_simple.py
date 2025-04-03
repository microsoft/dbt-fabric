from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase
from dbt.tests.adapter.simple_copy.test_simple_copy import EmptyModelsArentRunBase, SimpleCopyBase
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


class TestSimpleCopyUppercaseFabric(BaseSimpleCopyUppercase):
    pass


class TestSimpleCopyBaseFabric(SimpleCopyBase):
    pass


class TestEmptyModelsArentRunFabric(EmptyModelsArentRunBase):
    pass


# Adding seed-related test classes
class TestBasicSeedTestsFabric(BaseBasicSeedTests):
    pass


class TestSeedConfigFullRefreshOffFabric(BaseSeedConfigFullRefreshOff):
    pass


class TestSeedConfigFullRefreshOnFabric(BaseSeedConfigFullRefreshOn):
    pass


class TestSeedCustomSchemaFabric(BaseSeedCustomSchema):
    pass


class TestSeedParsingFabric(BaseSeedParsing):
    pass


class TestSeedSpecificFormatsFabric(BaseSeedSpecificFormats):
    pass


class TestSeedWithEmptyDelimiterFabric(BaseSeedWithEmptyDelimiter):
    pass


class TestSeedWithUniqueDelimiterFabric(BaseSeedWithUniqueDelimiter):
    pass


class TestSeedWithWrongDelimiterFabric(BaseSeedWithWrongDelimiter):
    pass


class TestSimpleSeedEnabledViaConfigFabric(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSimpleSeedWithBOMFabric(BaseSimpleSeedWithBOM):
    pass


class TestEmptySeedFabric(BaseTestEmptySeed):
    pass


class TestSimpleSeedColumnOverrideFabric(BaseSimpleSeedColumnOverride):
    pass


# Adding snapshot test classes
class TestSimpleSnapshotFabric(BaseSimpleSnapshot):
    pass


class TestSimpleSnapshotBaseFabric(BaseSimpleSnapshotBase):
    pass


class TestSnapshotCheckFabric(BaseSnapshotCheck):
    pass
