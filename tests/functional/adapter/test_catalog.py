# TODO: repoint to dbt-artifacts when it is available
import pytest

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.tests.util import run_dbt

MY_SEED = """
id,value,record_valid_date
1,100,2023-01-01 00:00:00
2,200,2023-01-02 00:00:00
3,300,2023-01-02 00:00:00
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select *
from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select *
from {{ ref('my_seed') }}
"""


class TestCatalogRelationTypes:
    """
    Many adapters can use this test as-is. However, if your adapter contains different
    relation types or uses different strings to describe the node (e.g. 'table' instead of 'BASE TABLE'),
    then you'll need to configure this test.

    To configure this test, you'll most likely need to update either `models`
    and/or `test_relation_types_populate_correctly`. For example, `dbt-snowflake`
    supports dynamic tables and does not support materialized views. It's implementation
    might look like this:

    class TestCatalogRelationTypes:
        @pytest.fixture(scope="class", autouse=True)
        def models(self):
            yield {
                "my_table.sql": files.MY_TABLE,
                "my_view.sql": files.MY_VIEW,
                "my_dynamic_table.sql": files.MY_DYNAMIC_TABLE,
            }

        @pytest.mark.parametrize(
            "node_name,relation_type",
            [
                ("seed.test.my_seed", "BASE TABLE"),
                ("model.test.my_table", "BASE TABLE"),
                ("model.test.my_view", "VIEW"),
                ("model.test.my_dynamic_table", "DYNAMIC TABLE"),
            ],
        )
        def test_relation_types_populate_correctly(
            self, docs: CatalogArtifact, node_name: str, relation_type: str
        ):
            super().test_relation_types_populate_correctly(
                docs, node_name, relation_type
            )

    Note that we're able to configure the test case using pytest parameterization
    and call back to the original test. That way any updates to the test are incorporated
    into your adapter.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
            ("seed.test.my_seed", "BASE TABLE"),
            ("model.test.my_table", "BASE TABLE"),
            ("model.test.my_view", "VIEW"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        """
        This test addresses: https://github.com/dbt-labs/dbt-core/issues/8864
        """
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type
