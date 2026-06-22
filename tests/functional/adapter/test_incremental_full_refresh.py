"""Full-refresh behavior for the `incremental` materialization.

The full-refresh path builds the replacement under a `__dbt_temp` name and swaps
it into place with a rename (instead of dropping the target and rebuilding it in
situ). These tests assert that a full refresh still produces correct data and
leaves no orphaned temp/backup relations behind.
"""

import pytest
from dbt.tests.util import run_dbt

_MODEL_INCREMENTAL = """
{{ config(materialized='incremental', unique_key='id') }}

select 1 as id, 'a' as letter
union all select 2 as id, 'b' as letter
{% if is_incremental() %}
union all select 3 as id, 'c' as letter
{% endif %}
"""

_COUNT_LEFTOVERS = """
select count(*) as n
from information_schema.tables
where table_schema = '{schema}'
  and (table_name like '%__dbt_temp%' or table_name like '%__dbt_backup%')
"""


class TestIncrementalFullRefreshSwapFabric:
    @pytest.fixture(scope="class")
    def models(self):
        return {"swap_model.sql": _MODEL_INCREMENTAL}

    def test_full_refresh_swaps_and_cleans_up(self, project):
        # Run 1 — first build (no existing relation): 2 rows.
        run_dbt(["run", "--select", "swap_model"])
        rows = project.run_sql("select count(*) from {schema}.swap_model", fetch="one")
        assert rows[0] == 2

        # Run 2 — incremental run: adds the third row via merge.
        run_dbt(["run", "--select", "swap_model"])
        rows = project.run_sql("select count(*) from {schema}.swap_model", fetch="one")
        assert rows[0] == 3

        # Run 3 — full refresh: rebuilds from scratch (is_incremental() false -> 2 rows).
        run_dbt(["run", "--select", "swap_model", "--full-refresh"])
        rows = project.run_sql("select count(*) from {schema}.swap_model", fetch="one")
        assert rows[0] == 2

        # The target survived the rebuild and no temp/backup relations were left behind.
        leftovers = project.run_sql(
            _COUNT_LEFTOVERS.format(schema=project.test_schema), fetch="one"
        )
        assert leftovers[0] == 0
