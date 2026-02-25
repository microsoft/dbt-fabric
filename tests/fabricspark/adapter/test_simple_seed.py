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


class TestBasicSeedTestsFabricSpark(BaseBasicSeedTests):
    pass


class TestEmptySeedFabricSpark(BaseTestEmptySeed):
    pass


class TestSeedConfigFullRefreshOffFabricSpark(BaseSeedConfigFullRefreshOff):
    pass


class TestSeedConfigFullRefreshOnFabricSpark(BaseSeedConfigFullRefreshOn):
    pass


class TestSeedCustomSchemaFabricSpark(BaseSeedCustomSchema):
    pass


class TestSeedParsingFabricSpark(BaseSeedParsing):
    pass


class TestSeedSpecificFormatsFabricSpark(BaseSeedSpecificFormats):
    pass


class TestSeedWithEmptyDelimiterFabricSpark(BaseSeedWithEmptyDelimiter):
    pass


class TestSeedWithUniqueDelimiterFabricSpark(BaseSeedWithUniqueDelimiter):
    pass


class TestSeedWithWrongDelimiterFabricSpark(BaseSeedWithWrongDelimiter):
    pass


class TestSimpleSeedColumnOverrideFabricSpark(BaseSimpleSeedColumnOverride):
    pass


class TestSimpleSeedEnabledViaConfigFabricSpark(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSimpleSeedWithBOMFabricSpark(BaseSimpleSeedWithBOM):
    pass
