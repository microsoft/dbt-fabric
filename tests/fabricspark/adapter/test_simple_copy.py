from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase
from dbt.tests.adapter.simple_copy.test_simple_copy import EmptyModelsArentRunBase, SimpleCopyBase


class TestEmptyModelsArentRunFabricSpark(EmptyModelsArentRunBase):
    pass


class TestSimpleCopyBaseFabricSpark(SimpleCopyBase):
    pass


class TestSimpleCopyUppercaseFabricSpark(BaseSimpleCopyUppercase):
    pass
