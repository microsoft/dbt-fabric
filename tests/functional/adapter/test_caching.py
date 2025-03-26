import pytest

from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseCachingUppercaseModel,
    BaseNoPopulateCache,
)


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


@pytest.mark.skip(reason="Fabric DW does not support Case Insensivity.")
class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass


class TestNoPopulateCache(BaseNoPopulateCache):
    pass
