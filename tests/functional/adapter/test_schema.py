import os
from time import sleep

import pytest

from dbt.tests.util import run_dbt


class TestSchemaCreation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dummy.sql": """
{{ config(schema='with_custom_auth') }}
select 1 as id
""",
        }

    @staticmethod
    @pytest.fixture(scope="class")
    def dbt_profile_target_update():
        return {"schema_authorization": "{{ env_var('DBT_TEST_USER_1', 'dbo') }}"}

    @staticmethod
    def _verify_schema_owner(schema_name, owner, project):
        get_schema_owner = f"""
select p.name
from sys.schemas s
join sys.database_principals p
on s.principal_id = p.principal_id
where s.name = '{schema_name}'
        """
        result = project.run_sql(get_schema_owner, fetch="one")
        first_record = result[0]
        assert first_record == owner, f"Schema owner for {schema_name} is not {owner}"

    def test_schema_creation(self, project, unique_schema):
        test_user = os.getenv("DBT_TEST_USER_1", "dbo")
        if test_user != "dbo":
            # Fabric only seems to recognize users as soon as you have granted them something
            # You cannot grant something to dbo
            project.run_sql(f"grant select on schema :: dbo to [{test_user}]", fetch=None)

        res = run_dbt(["run"])
        assert len(res) == 1

        self._verify_schema_owner(unique_schema, test_user, project)
        self._verify_schema_owner(unique_schema + "_with_custom_auth", test_user, project)
