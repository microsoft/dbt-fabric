from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliasErrors,
    BaseAliases,
    BaseSameAliasDifferentDatabases,
    BaseSameAliasDifferentSchemas,
)


class TestAliasesFabricSpark(BaseAliases):
    pass


class TestAliasErrorsFabricSpark(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemasFabricSpark(BaseSameAliasDifferentSchemas):
    pass


class TestSameAliasDifferentDatabasesFabricSpark(BaseSameAliasDifferentDatabases):
    pass
