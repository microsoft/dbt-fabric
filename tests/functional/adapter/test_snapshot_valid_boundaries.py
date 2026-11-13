import pytest

from dbt.tests.util import run_dbt

_seed_sql = """
create table {database}.{schema}.seed (
    id INTEGER,
    val VARCHAR(50)
);

insert into {database}.{schema}.seed (id, val) values
(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');
"""

_change_sql = """
update {database}.{schema}.seed set val = val + '_changed' where id <= 5;
delete from {database}.{schema}.seed where id = 6;
"""

_snapshot_sql = """
{% snapshot {name} %}
    {{
        config(
            unique_key='id',
            strategy='check',
            check_cols=['val'],
            hard_deletes='invalidate',
        )
    }}
    select * from {{ target.database }}.{{ target.schema }}.seed
{% endsnapshot %}
"""


def snapshot_sql(name):
    return _snapshot_sql.replace("{name}", name)


_mismatched_boundaries_sql = """
select closed.id
from {database}.{schema}.snapshot_check as closed
join {database}.{schema}.snapshot_check as successor
    on successor.id = closed.id
    and successor.dbt_valid_from > closed.dbt_valid_from
where closed.dbt_valid_to is not null
    and closed.dbt_valid_to <> successor.dbt_valid_from
"""

_second_run_times_sql = """
select distinct second_run_time
from (
    select dbt_valid_to as second_run_time
    from {database}.{schema}.snapshot_check
    where dbt_valid_to is not null
    union all
    select dbt_valid_from
    from {database}.{schema}.snapshot_check
    where dbt_valid_from > (select min(dbt_valid_from) from {database}.{schema}.snapshot_check)
    union all
    select dbt_valid_to
    from {database}.{schema}.snapshot_check_other
    where dbt_valid_to is not null
) as times
"""

_closed_ids_sql = """
select id from {database}.{schema}.snapshot_check where dbt_valid_to is not null order by id
"""

_new_versions_sql = """
select count(*)
from {database}.{schema}.snapshot_check
where dbt_valid_to is null
    and dbt_valid_from = (select max(dbt_valid_from) from {database}.{schema}.snapshot_check)
"""


# Fabric Warehouse does not reliably reproduce SYSDATETIME() drift within one statement, so this
# test also compares two snapshots: they run as separate statements and share one time only when
# the snapshot time is constant for the invocation.
class TestSnapshotValidBoundaries:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_check.sql": snapshot_sql("snapshot_check"),
            "snapshot_check_other.sql": snapshot_sql("snapshot_check_other"),
        }

    def test_closed_and_new_versions_share_one_boundary(self, project):
        project.run_sql(_seed_sql)
        assert len(run_dbt(["snapshot"])) == 2

        project.run_sql(_change_sql)
        assert len(run_dbt(["snapshot"])) == 2

        assert project.run_sql(_mismatched_boundaries_sql, fetch="all") == []
        assert len(project.run_sql(_second_run_times_sql, fetch="all")) == 1
        closed_ids = [row[0] for row in project.run_sql(_closed_ids_sql, fetch="all")]
        assert closed_ids == [1, 2, 3, 4, 5, 6]
        assert project.run_sql(_new_versions_sql, fetch="one")[0] == 5
