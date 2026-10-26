import hashlib
import sqlite3
import statistics
from pathlib import Path
from types import SimpleNamespace

import pytest
from dbt_common.clients.jinja import get_template
from dbt_common.exceptions import CompilationError
from dbt_common.utils.jinja import get_dbt_macro_name

MACROS = Path(__file__).parents[4] / "dbt/include/fabric/macros/dbt_package_support"
EQUAL = "dbt_expectations/schema_tests/_generalized/equal_expression.sql"
UNIQUE = (
    "dbt_expectations/schema_tests/multi-column/"
    "expect_select_column_values_to_be_unique_within_record.sql"
)
STDEVS = (
    "dbt_expectations/schema_tests/distributional/expect_column_values_to_be_within_n_stdevs.sql"
)
RECENCY = (
    "dbt_expectations/schema_tests/table_shape/expect_grouped_row_values_to_have_recent_data.sql"
)
FROM_EPOCH = "dbt_date/calendar_date/from_unixtimestamp.sql"
TO_EPOCH = "dbt_date/calendar_date/to_unixtimestamp.sql"


def raise_compiler_error(message):
    raise CompilationError(message)


def render(path, name, *args, **kwargs):
    def get_select(*select_args):
        return render(EQUAL, "get_select", *select_args)

    def ignore_row_if_expression(mode, columns):
        if mode not in ("all_values_are_missing", "any_value_is_missing"):
            raise CompilationError("Invalid ignore_row_if")
        operator = " and " if mode == "all_values_are_missing" else " or "
        return "not (" + operator.join(f"{column} is null" for column in columns) + ")"

    context = {
        "adapter": SimpleNamespace(quote=lambda column: f"[{column}]"),
        "dbt": SimpleNamespace(
            type_float=lambda: "float",
            type_timestamp=lambda: "datetime2(6)",
            dateadd=lambda part, interval, value: f"dateadd({part}, {interval}, {value})",
            hash=lambda column: f"md5({column})",
        ),
        "dbt_date": SimpleNamespace(now=lambda: "current_timestamp"),
        "dbt_expectations": SimpleNamespace(
            get_select=get_select,
            ignore_row_if_expression=ignore_row_if_expression,
        ),
        "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
    }
    module = get_template((MACROS / path).read_text(), context).make_module(context)
    return getattr(module, get_dbt_macro_name(f"fabric__{name}"))(*args, **kwargs)


class Stdev:
    def __init__(self):
        self.values = []

    def step(self, value):
        if value is not None:
            self.values.append(value)

    def value(self):
        return statistics.stdev(self.values) if len(self.values) > 1 else None

    def finalize(self):
        return self.value()


@pytest.fixture
def database():
    with sqlite3.connect(":memory:") as connection:
        connection.create_window_function("stdev", 1, Stdev)
        connection.create_function(
            "md5",
            1,
            lambda value: hashlib.md5(("" if value is None else str(value)).encode()).hexdigest(),
        )
        yield connection
    connection.close()


@pytest.mark.parametrize("tolerance, failures", [(0.005, 1), (0.02, 0)])
def test_percentage_tolerance_uses_fractional_division(database, tolerance, failures):
    database.executescript(
        "create table baseline(amount int); insert into baseline values (100);"
        "create table comparison(amount int); insert into comparison values (99);"
    )
    sql = render(
        EQUAL,
        "test_equal_expression",
        "baseline",
        "sum(amount)",
        "comparison",
        None,
        None,
        None,
        None,
        None,
        0,
        tolerance,
    )
    rows = database.execute(sql).fetchall()
    assert len(rows) == failures
    if rows:
        assert rows[0][-1] == pytest.approx(0.01)


@pytest.mark.parametrize("quote_columns", [True, False])
@pytest.mark.parametrize(
    "ignore_row_if, row_condition, failures",
    [
        ("all_values_are_missing", None, 1),
        ("any_value_is_missing", None, 0),
        ("all_values_are_missing", "first_value is not null", 0),
    ],
)
def test_within_record_uniqueness_handles_types_and_missing_values(
    database, quote_columns, ignore_row_if, row_condition, failures
):
    database.executescript(
        "create table subject(first_value int, second_value text);"
        "insert into subject values (1, '2'), (null, ''), (null, null);"
    )
    sql = render(
        UNIQUE,
        "test_expect_select_column_values_to_be_unique_within_record",
        "subject",
        ["first_value", "second_value"],
        quote_columns,
        ignore_row_if,
        row_condition,
    )
    assert len(database.execute(sql).fetchall()) == failures
    assert sql.count("md5(") == 2
    if quote_columns:
        assert "md5([first_value])" in sql
        assert "row_number() over(order by [first_value], [second_value])" in sql
    else:
        assert "row_number() over(order by first_value, second_value)" in sql


def test_within_record_uniqueness_delegates_invalid_ignore_mode():
    with pytest.raises(CompilationError, match="Invalid ignore_row_if"):
        render(
            UNIQUE,
            "test_expect_select_column_values_to_be_unique_within_record",
            "subject",
            ["first_value", "second_value"],
            True,
            "invalid",
            None,
        )


@pytest.mark.parametrize("group_by", [None, [], ["bucket"], ["bucket", "segment"], "bucket"])
def test_stdev_constant_values_return_no_failures(database, group_by):
    database.executescript(
        "create table subject(bucket text, segment text, amount int);"
        "insert into subject values ('a', 'x', 10), ('b', 'x', 10), ('c', 'y', 10);"
    )
    sql = render(
        STDEVS, "test_expect_column_values_to_be_within_n_stdevs", "subject", "amount", group_by, 3
    )
    assert database.execute(sql).fetchall() == []
    assert "nullif(amount_stddev, 0)" in sql


@pytest.mark.parametrize(
    "values, threshold, failures",
    [([0, 1], 0.9, 0), ([0, 0, 0, 0, 100], 1, 1), ([10], 3, 0)],
)
def test_stdev_returns_actual_failing_rows(database, values, threshold, failures):
    database.execute("create table subject(amount int)")
    database.executemany("insert into subject values (?)", [(value,) for value in values])
    sql = render(
        STDEVS,
        "test_expect_column_values_to_be_within_n_stdevs",
        "subject",
        "amount",
        None,
        threshold,
    )
    rows = database.execute(sql).fetchall()
    assert len(rows) == failures
    if rows:
        assert rows[0][0] == 100


@pytest.mark.parametrize("group_by", [None, [], ["bucket"], ["bucket", "segment"]])
def test_recency_supports_optional_grouping_and_empty_input(group_by):
    sql = render(
        RECENCY,
        "test_expect_grouped_row_values_to_have_recent_data",
        "subject",
        group_by,
        "event_time",
        "day",
        1,
        "amount > 0",
    )
    assert ("group by" in sql) == bool(group_by)
    assert "having count(*) > 0" in sql
    assert "max(cast(event_time as datetime2(6)))" in sql
    assert "and amount > 0" in sql
    assert "dateadd(day, -1, current_timestamp)" in sql


@pytest.mark.parametrize(
    "format, units",
    [
        ("seconds", 1),
        ("milliseconds", 1000),
        ("microseconds", 1000000),
        ("nanoseconds", 1000000000),
    ],
)
def test_epoch_conversion_splits_into_int_sized_dateadd_arguments(format, units):
    sql = render(FROM_EPOCH, "from_unixtimestamp", "epoch_value", format)
    assert sql.count("dateadd(") == 3
    assert f"cast(epoch_value as bigint) / {units}" in sql
    assert f"cast(epoch_value as bigint) % {units}" in sql
    assert "86400" in sql
    assert "datetime2(6)" in sql
    assert "datetime2(7)" not in sql


def test_epoch_conversion_defaults_to_seconds():
    assert render(FROM_EPOCH, "from_unixtimestamp", "epoch_value") == render(
        FROM_EPOCH, "from_unixtimestamp", "epoch_value", "seconds"
    )


def test_epoch_conversion_rejects_unknown_units():
    with pytest.raises(CompilationError, match="not supported"):
        render(FROM_EPOCH, "from_unixtimestamp", "epoch_value", "minutes")


def test_to_epoch_uses_bigint_difference():
    sql = render(TO_EPOCH, "to_unixtimestamp", "event_time")
    assert "datediff_big(second, cast('1970-01-01' as datetime2(6)), event_time)" in sql
