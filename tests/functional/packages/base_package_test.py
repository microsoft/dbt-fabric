import pytest

from dbt.tests.util import run_dbt


class BaseDbtPackageTests:
    @pytest.fixture(scope="class")
    def package_name(self) -> str:
        raise NotImplementedError("Subclasses must implement package_name")

    @pytest.fixture(scope="class")
    def package_repo(self) -> str:
        raise NotImplementedError("Subclasses must implement package_repo")

    @pytest.fixture(scope="class")
    def package_revision(self) -> str:
        raise NotImplementedError("Subclasses must implement package_version")

    @pytest.fixture(scope="class")
    def packages(self, package_repo: str, package_revision: str):
        return {
            "packages": [
                {"git": package_repo, "revision": package_revision},
                {
                    "git": package_repo,
                    "revision": package_revision,
                    "subdirectory": "integration_tests",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models_config(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds_config(self):
        return {}

    @pytest.fixture(scope="class")
    def tests_config(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(
        self, package_name: str, models_config: dict, seeds_config: dict, tests_config: dict
    ):
        return {
            "name": "test_dbt_package",
            "vars": {"dbt_date:time_zone": "UTC"},
            "dispatch": [
                {
                    "macro_namespace": package_name,
                    "search_order": [
                        f"test_dbt_package",
                        "dbt_fabric",
                        "dbt",
                        package_name,
                    ],
                }
            ],
            "seeds": seeds_config,
            "models": models_config,
            "tests": tests_config,
        }

    def test_packages(self, project, dbt_core_bug_workaround):
        run_dbt(["deps"])
        run_dbt(["seed"])
        run_dbt(["run"])
