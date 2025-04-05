import pytest

from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct


@pytest.mark.skip(reason="Array concat is not supported in Fabric")
class TestArrayAppendFabric(BaseArrayAppend):
    pass


@pytest.mark.skip(reason="Array concat is not supported in Fabric")
class TestArrayConcatFabric(BaseArrayConcat):
    pass


class TestArrayConstructFabric(BaseArrayConstruct):
    pass
