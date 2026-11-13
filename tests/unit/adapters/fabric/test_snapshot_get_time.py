from datetime import UTC, datetime
from pathlib import Path

from dbt_common.clients.jinja import get_template
from dbt_common.exceptions.macros import MacroReturn
from dbt_common.utils.jinja import get_dbt_macro_name

TIMESTAMPS = Path(__file__).parents[4] / "dbt/include/fabric/macros/utils/timestamps.sql"
CURRENT_TIMESTAMP = "CAST(SYSDATETIME() AS DATETIME2(6))"


def macro_return(value):
    raise MacroReturn(value)


def render_snapshot_get_time(run_started_at):
    module = {}

    def dispatch(name):
        def invoke(*args, **kwargs):
            try:
                return module[get_dbt_macro_name(f"fabric__{name}")](*args, **kwargs)
            except MacroReturn as result:
                return result.value

        return invoke

    context = {
        "return": macro_return,
        "run_started_at": run_started_at,
        "current_timestamp": dispatch("current_timestamp"),
        "snapshot_string_as_time": dispatch("snapshot_string_as_time"),
    }
    module.update(get_template(TIMESTAMPS.read_text(), context).make_module(context).__dict__)
    return str(dispatch("snapshot_get_time")()).strip()


def test_snapshot_get_time_is_a_literal_of_run_started_at():
    run_started_at = datetime(2026, 11, 13, 3, 4, 41, 571669, tzinfo=UTC)

    assert (
        render_snapshot_get_time(run_started_at)
        == "CONVERT(DATETIME2(6), '2026-11-13 03:04:41.571669')"
    )


def test_snapshot_get_time_keeps_microseconds_when_zero():
    run_started_at = datetime(2026, 11, 13, 3, 4, 41, tzinfo=UTC)

    assert (
        render_snapshot_get_time(run_started_at)
        == "CONVERT(DATETIME2(6), '2026-11-13 03:04:41.000000')"
    )


def test_snapshot_get_time_is_identical_across_references():
    run_started_at = datetime(2026, 11, 13, 3, 4, 41, 571669, tzinfo=UTC)

    first = render_snapshot_get_time(run_started_at)
    second = render_snapshot_get_time(run_started_at)

    assert first == second
    assert "SYSDATETIME" not in first


def test_snapshot_get_time_falls_back_to_current_timestamp_without_run_started_at():
    assert render_snapshot_get_time(None) == CURRENT_TIMESTAMP
