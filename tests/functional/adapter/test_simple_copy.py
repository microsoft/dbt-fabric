from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase
from dbt.tests.adapter.simple_copy.test_simple_copy import EmptyModelsArentRunBase, SimpleCopyBase


class TestEmptyModelsArentRunFabric(EmptyModelsArentRunBase):
    pass


class TestSimpleCopyBaseFabric(SimpleCopyBase):
    pass


class TestSimpleCopyUppercaseFabric(BaseSimpleCopyUppercase):
    pass
