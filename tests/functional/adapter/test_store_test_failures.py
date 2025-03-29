import pytest

from dbt.tests.adapter.store_test_failures_tests import basic, fixtures
from dbt.tests.util import check_relations_equal, run_dbt

# used to rename test audit schema to help test schema meet max char limit
# the default is _dbt_test__audit but this runs over the postgres 63 schema name char limit
# without which idempotency conditions will not hold (i.e. dbt can't drop the schema properly)
TEST_AUDIT_SCHEMA_SUFFIX = "dbt_test__aud"

tests__passing_test = """
select * from {{ ref('fine_model') }}
where 1=2
"""


class StoreTestFailuresBase:
    @pytest.fixture(scope="function", autouse=True)
    def setUp(self, project):
        self.test_audit_schema = f"{project.test_schema}_{TEST_AUDIT_SCHEMA_SUFFIX}"
        run_dbt(["seed"])
        run_dbt(["run"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": fixtures.seeds__people,
            "expected_accepted_values.csv": fixtures.seeds__expected_accepted_values,
            "expected_failing_test.csv": fixtures.seeds__expected_failing_test,
            "expected_not_null_problematic_model_id.csv": fixtures.seeds__expected_not_null_problematic_model_id,
            "expected_unique_problematic_model_id.csv": fixtures.seeds__expected_unique_problematic_model_id,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing_test.sql": fixtures.tests__failing_test,
            "passing_test.sql": tests__passing_test,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": fixtures.properties__schema_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fine_model.sql": fixtures.models__fine_model,
            "fine_model_but_with_a_no_good_very_long_name.sql": fixtures.models__file_model_but_with_a_no_good_very_long_name,
            "problematic_model.sql": fixtures.models__problematic_model,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "test": self.column_type_overrides(),
            },
            "data_tests": {"+schema": TEST_AUDIT_SCHEMA_SUFFIX},
        }

    def column_type_overrides(self):
        return {}

    def run_tests_store_one_failure(self, project):
        run_dbt(["test"], expect_pass=False)

        # one test is configured with store_failures: true, make sure it worked
        check_relations_equal(
            project.adapter,
            [
                f"{TEST_AUDIT_SCHEMA_SUFFIX}.unique_problematic_model_id",
                "expected_unique_problematic_model_id",
            ],
        )

    def run_tests_store_failures_and_assert(self, project):
        # make sure this works idempotently for all tests
        run_dbt(["test", "--store-failures"], expect_pass=False)
        results = run_dbt(["test", "--store-failures"], expect_pass=False)

        # compare test results
        actual = [(r.status, r.failures) for r in results]
        expected = [
            ("pass", 0),
            ("pass", 0),
            ("pass", 0),
            ("pass", 0),
            ("fail", 2),
            ("fail", 2),
            ("fail", 2),
            ("fail", 10),
        ]
        assert sorted(actual) == sorted(expected)

        # compare test results stored in database
        check_relations_equal(
            project.adapter, [f"{TEST_AUDIT_SCHEMA_SUFFIX}.failing_test", "expected_failing_test"]
        )
        check_relations_equal(
            project.adapter,
            [
                f"{TEST_AUDIT_SCHEMA_SUFFIX}.not_null_problematic_model_id",
                "expected_not_null_problematic_model_id",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{TEST_AUDIT_SCHEMA_SUFFIX}.unique_problematic_model_id",
                "expected_unique_problematic_model_id",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{TEST_AUDIT_SCHEMA_SUFFIX}.accepted_values_problemat"
                "ic_mo_c533ab4ca65c1a9dbf14f79ded49b628",
                "expected_accepted_values",
            ],
        )


class BaseStoreTestFailures(StoreTestFailuresBase):
    @pytest.fixture(scope="function")
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=self.test_audit_schema
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def column_type_overrides(self):
        return {
            "expected_unique_problematic_model_id": {
                "+column_types": {
                    "n_records": "bigint",
                },
            },
            "expected_accepted_values": {
                "+column_types": {
                    "n_records": "bigint",
                },
            },
        }

    def test__store_and_assert(self, project, clean_up):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)


class TestStoreTestFailures(BaseStoreTestFailures):
    pass


class TestStoreTestFailuresAsProjectLevelOff(basic.StoreTestFailuresAsProjectLevelOff):
    pass


class TestStoreTestFailuresAsProjectLevelView(basic.StoreTestFailuresAsProjectLevelView):
    pass


class TestStoreTestFailuresAsGeneric(basic.StoreTestFailuresAsGeneric):
    pass


class TestStoreTestFailuresAsProjectLevelEphemeral(basic.StoreTestFailuresAsProjectLevelEphemeral):
    pass


class TestStoreTestFailuresAsExceptions(basic.StoreTestFailuresAsExceptions):
    pass
