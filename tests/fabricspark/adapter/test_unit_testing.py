from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_quoted_reserved_word_column_names import (
    BaseUnitTestQuotedReservedWordColumnNames,
)
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestFabricSparkUnitTestingTypes(BaseUnitTestingTypes):
    pass


class TestFabricSparkUnitTestCaseInsensivity(BaseUnitTestCaseInsensivity):
    pass


class TestFabricSparkUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass


class TestFabricSparkUnitTestQuotedReservedWordColumnNames(
    BaseUnitTestQuotedReservedWordColumnNames,
):
    pass
