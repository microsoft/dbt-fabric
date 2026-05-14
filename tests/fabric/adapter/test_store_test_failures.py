import time

import pytest
from dbt_common.exceptions import DbtDatabaseError

from dbt.tests.adapter.store_test_failures_tests import fixtures
from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsExceptions,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
)
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    BaseStoreTestFailures,
    BaseStoreTestFailuresLimit,
)
from dbt.tests.util import check_relation_types, run_dbt

tests__passing_test = """
select * from {{ ref('fine_model') }}
where 1=0
"""


class FabricStoreTestFailuresMixin:
    """Retry check_relation_types to handle Fabric snapshot isolation errors.

    When the full test suite runs in parallel, concurrent DDL from other test classes
    can cause transient snapshot isolation failures in the sys.tables/sys.views queries
    used by check_relation_types.
    """

    def run_and_assert(self, project, expected_results, expect_pass=False):
        results = run_dbt(["test"], expect_pass=expect_pass)

        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        relation_to_type = {result.name: result.type for result in expected_results}
        max_retries = 3
        for attempt in range(max_retries):
            try:
                check_relation_types(project.adapter, relation_to_type)
                break
            except DbtDatabaseError:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    raise


class TestFabricStoreTestFailures(FabricStoreTestFailuresMixin, BaseStoreTestFailures):
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing_test.sql": fixtures.tests__failing_test,
            "passing_test.sql": tests__passing_test,
        }


class TestFabricStoreTestFailuresAsGeneric(
    FabricStoreTestFailuresMixin, StoreTestFailuresAsGeneric
):
    pass


class TestFabricStoreTestFailuresAsExceptions(
    FabricStoreTestFailuresMixin, StoreTestFailuresAsExceptions
):
    pass


class TestFabricStoreTestFailuresAsInteractions(
    FabricStoreTestFailuresMixin, StoreTestFailuresAsInteractions
):
    pass


class TestFabricStoreTestFailuresAsProjectLevelEphemeral(
    FabricStoreTestFailuresMixin, StoreTestFailuresAsProjectLevelEphemeral
):
    pass


class TestFabricStoreTestFailuresAsProjectLevelOff(
    FabricStoreTestFailuresMixin, StoreTestFailuresAsProjectLevelOff
):
    pass


class TestFabricStoreTestFailuresAsProjectLevelView(
    FabricStoreTestFailuresMixin, StoreTestFailuresAsProjectLevelView
):
    pass


class TestFabricStoreTestFailuresLimit(BaseStoreTestFailuresLimit):
    pass
