import agate
import dbt_common.exceptions
import pytest
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)

from dbt.adapters.fabric.fabric_adapter import FabricAdapter
from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.adapters.fabric.table_refresh import FabricTableConstraint


class TestConvertBooleanType:
    def test_returns_bit(self):
        assert FabricAdapter.convert_boolean_type(None, None) == "bit"


class TestConvertDatetimeType:
    def test_returns_datetime2(self):
        assert FabricAdapter.convert_datetime_type(None, None) == "datetime2(6)"


class TestConvertTimeType:
    def test_returns_time6(self):
        assert FabricAdapter.convert_time_type(None, None) == "time(6)"


class TestConvertNumberType:
    def test_returns_float_when_decimals_present(self):
        table = agate.Table([[1.5], [2.3]], column_names=["val"], column_types=[agate.Number()])
        assert FabricAdapter.convert_number_type(table, 0) == "float"

    def test_returns_int_when_no_decimals(self):
        table = agate.Table([[1], [2]], column_names=["val"], column_types=[agate.Number()])
        assert FabricAdapter.convert_number_type(table, 0) == "int"


class TestConvertTextType:
    def test_short_strings_get_minimum_length_16(self):
        table = agate.Table([["hi"], ["ok"]], column_names=["val"], column_types=[agate.Text()])
        assert FabricAdapter.convert_text_type(table, 0) == "varchar(16)"

    def test_long_strings_use_max_utf8_byte_length(self):
        long_val = "a" * 100
        table = agate.Table(
            [[long_val], ["short"]], column_names=["val"], column_types=[agate.Text()]
        )
        assert FabricAdapter.convert_text_type(table, 0) == "varchar(100)"

    def test_multibyte_characters_use_byte_length(self):
        val = "ünïcödé tëxt dätä"
        table = agate.Table([[val]], column_names=["val"], column_types=[agate.Text()])
        expected_len = len(val.encode("utf-8"))
        assert expected_len > 16
        assert FabricAdapter.convert_text_type(table, 0) == f"varchar({expected_len})"

    def test_empty_column_defaults_to_64(self):
        table = agate.Table([[None]], column_names=["val"], column_types=[agate.Text()])
        assert FabricAdapter.convert_text_type(table, 0) == "varchar(64)"


class TestQuote:
    def test_wraps_in_brackets(self):
        assert FabricAdapter.quote("my_table") == "[my_table]"

    def test_escapes_closing_bracket(self):
        assert FabricAdapter.quote("tricky]name") == "[tricky]]name]"

    def test_escapes_multiple_closing_brackets(self):
        assert FabricAdapter.quote("a]b]c") == "[a]]b]]c]"


class TestDateFunction:
    def test_returns_getdate(self):
        assert FabricAdapter.date_function() == "getdate()"


def _make_adapter_instance():
    return object.__new__(FabricAdapter)


class TestTableRefreshConstraintPlanning:
    @pytest.fixture
    def adapter(self):
        adapter = _make_adapter_instance()
        columns = [
            FabricColumn(
                "id",
                "int",
                char_size=4,
                numeric_precision=10,
                numeric_scale=0,
                is_nullable=False,
            )
        ]
        adapter._describe_query_columns = lambda sql: columns
        adapter.get_columns_in_relation = lambda relation: columns
        adapter._get_table_cluster_by = lambda relation: []
        adapter.get_constraints_in_relation = lambda relation: []
        return adapter

    @pytest.fixture
    def relation(self):
        return FabricRelation.create(
            database="warehouse",
            schema="dbo",
            identifier="refresh_table",
            type="table",
        )

    def test_plan_contains_rendered_constraint_addition(self, adapter, relation):
        plan = adapter.get_table_refresh_plan(
            relation,
            "select cast(1 as int) as id",
            constraints=[
                {
                    "name": "uq_refresh_table",
                    "type": "unique",
                    "columns": ["id"],
                }
            ],
        )

        assert plan["action"] == "reload"
        assert plan["constraints_to_add"] == ["uq_refresh_table"]
        assert plan["constraint_add_sql"] == [
            "add constraint uq_refresh_table unique nonclustered(id) not enforced"
        ]

    def test_custom_constraint_forces_replacement(self, adapter, relation):
        plan = adapter.get_table_refresh_plan(
            relation,
            "select cast(1 as int) as id",
            constraints=[
                {
                    "name": "custom_refresh_table",
                    "type": "custom",
                    "expression": "constraint custom_refresh_table unique (id)",
                }
            ],
        )

        assert plan["action"] == "replace"
        assert plan["reason"] == "constraint definition requires table replacement"

    def test_changed_qualified_foreign_key_is_reconciled(self, adapter, relation):
        adapter.get_constraints_in_relation = lambda relation: [
            FabricTableConstraint(
                "fk_refresh_table",
                "foreign_key",
                ("id",),
                referenced_database="warehouse",
                referenced_schema="archive",
                referenced_table="parent",
                referenced_columns=("id",),
            )
        ]

        plan = adapter.get_table_refresh_plan(
            relation,
            "select cast(1 as int) as id",
            constraints=[
                {
                    "name": "fk_refresh_table",
                    "type": "foreign_key",
                    "columns": ["id"],
                    "expression": "[warehouse].[dbo].[parent] ([id])",
                }
            ],
        )

        assert plan["constraints_to_drop"] == ["fk_refresh_table"]
        assert plan["constraints_to_add"] == ["fk_refresh_table"]

    def test_unchanged_constraint_is_not_rendered(self, adapter, relation):
        adapter.get_constraints_in_relation = lambda relation: [
            FabricTableConstraint("uq_refresh_table", "unique", ("id",))
        ]

        plan = adapter.get_table_refresh_plan(
            relation,
            "select cast(1 as int) as id",
            constraints=[
                {
                    "name": "uq_refresh_table",
                    "type": "unique",
                    "columns": ["id"],
                }
            ],
        )

        assert plan["action"] == "reload"
        assert plan["constraints_to_drop"] == []
        assert plan["constraints_to_add"] == []
        assert plan["constraint_add_sql"] == []

    def test_query_referencing_target_forces_replacement(self, adapter, relation):
        plan = adapter.get_table_refresh_plan(
            relation,
            "select id from [warehouse].[dbo].[refresh_table]",
        )

        assert plan["action"] == "replace"
        assert plan["reason"] == "query references the target relation"


class TestTableRefreshMetadata:
    @pytest.mark.parametrize(
        ("data_type", "max_length", "expected"),
        [
            ("varchar", 100, 100),
            ("nvarchar", 100, 50),
            ("NCHAR", "20", 10),
            ("nvarchar", -1, -1),
            ("date", None, None),
        ],
    )
    def test_column_character_size(self, data_type, max_length, expected):
        assert FabricAdapter._column_character_size(data_type, max_length) == expected

    def test_column_character_size_rejects_unexpected_value(self):
        with pytest.raises(TypeError, match="Unexpected max_length"):
            FabricAdapter._column_character_size("varchar", object())

    def test_describe_query_columns_maps_metadata_and_skips_hidden_columns(self):
        adapter = _make_adapter_instance()
        captured_sql = []
        adapter._fetch_dicts = lambda sql: (
            captured_sql.append(sql)
            or [
                {
                    "name": "name",
                    "system_type_name": "nvarchar(50)",
                    "max_length": 100,
                    "precision": 0,
                    "scale": 0,
                    "is_nullable": 1,
                    "collation_name": "Latin1_General_100_CI_AS",
                    "is_identity_column": 0,
                    "is_hidden": 0,
                },
                {
                    "name": "hidden",
                    "system_type_name": "int",
                    "max_length": 4,
                    "precision": 10,
                    "scale": 0,
                    "is_nullable": 0,
                    "collation_name": None,
                    "is_identity_column": 0,
                    "is_hidden": 1,
                },
            ]
        )

        columns = adapter._describe_query_columns("select 'value' as name")

        assert "select ''value'' as name" in captured_sql[0]
        assert columns == [
            FabricColumn(
                "name",
                "nvarchar",
                char_size=50,
                numeric_precision=0,
                numeric_scale=0,
                is_nullable=True,
                collation_name="Latin1_General_100_CI_AS",
            )
        ]

    def test_get_constraints_groups_composite_key_and_foreign_key_columns(self):
        adapter = _make_adapter_instance()
        adapter._fetch_dicts = lambda sql: [
            {
                "name": "pk_model",
                "constraint_type": "primary_key",
                "column_name": "tenant_id",
                "referenced_database": None,
                "referenced_schema": None,
                "referenced_table": None,
                "referenced_column": None,
            },
            {
                "name": "pk_model",
                "constraint_type": "primary_key",
                "column_name": "id",
                "referenced_database": None,
                "referenced_schema": None,
                "referenced_table": None,
                "referenced_column": None,
            },
            {
                "name": "fk_parent",
                "constraint_type": "foreign_key",
                "column_name": "parent_tenant_id",
                "referenced_database": "warehouse",
                "referenced_schema": "dbo",
                "referenced_table": "parent",
                "referenced_column": "tenant_id",
            },
            {
                "name": "fk_parent",
                "constraint_type": "foreign_key",
                "column_name": "parent_id",
                "referenced_database": "warehouse",
                "referenced_schema": "dbo",
                "referenced_table": "parent",
                "referenced_column": "id",
            },
        ]
        relation = FabricRelation.create(
            database="warehouse",
            schema="dbo",
            identifier="model",
            type="table",
        )

        assert adapter.get_constraints_in_relation(relation) == [
            FabricTableConstraint("pk_model", "primary_key", ("tenant_id", "id")),
            FabricTableConstraint(
                "fk_parent",
                "foreign_key",
                ("parent_tenant_id", "parent_id"),
                referenced_database="warehouse",
                referenced_schema="dbo",
                referenced_table="parent",
                referenced_columns=("tenant_id", "id"),
            ),
        ]


class TestColumnSchemaFromQuery:
    def test_preserves_connection_type_normalization(self):
        adapter = _make_adapter_instance()
        cursor = type(
            "Cursor",
            (),
            {
                "description": [
                    ("amount", 3, None, None, None, None, None),
                    ("id", 4, None, None, None, None, None),
                ]
            },
        )()
        adapter.connections = type(
            "Connections",
            (),
            {
                "add_select_query": lambda self, sql: (None, cursor),
                "data_type_code_to_name": lambda self, code: {
                    3: "decimal",
                    4: "int",
                }[code],
            },
        )()

        assert adapter.get_column_schema_from_query("select 1.0 as amount, 1 as id") == [
            FabricColumn("amount", "decimal"),
            FabricColumn("id", "int"),
        ]


class TestTimestampAddSql:
    def test_default_parameters(self):
        adapter = _make_adapter_instance()
        result = adapter.timestamp_add_sql("my_col")
        assert result == "DATEADD(hour,1,my_col)"

    def test_custom_parameters(self):
        adapter = _make_adapter_instance()
        result = adapter.timestamp_add_sql("ts_col", number=5, interval="day")
        assert result == "DATEADD(day,5,ts_col)"


class TestStringAddSql:
    def test_append(self):
        adapter = _make_adapter_instance()
        result = adapter.string_add_sql("col", "suffix")
        assert result == "col + 'suffix'"

    def test_prepend(self):
        adapter = _make_adapter_instance()
        result = adapter.string_add_sql("col", "prefix", location="prepend")
        assert result == "'prefix' + col"

    def test_invalid_location_raises(self):
        adapter = _make_adapter_instance()
        with pytest.raises(ValueError, match="unexpected location"):
            adapter.string_add_sql("col", "val", location="middle")


class TestValidIncrementalStrategies:
    def test_returns_expected_strategies(self):
        adapter = _make_adapter_instance()
        assert adapter.valid_incremental_strategies() == [
            "append",
            "delete+insert",
            "microbatch",
            "merge",
        ]


class TestRenderColumnConstraint:
    def test_not_null(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.not_null)
        assert FabricAdapter.render_column_constraint(constraint) == "not null"

    def test_unique_returns_empty_string(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.unique)
        assert FabricAdapter.render_column_constraint(constraint) == ""

    def test_primary_key_returns_empty_string(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.primary_key)
        assert FabricAdapter.render_column_constraint(constraint) == ""

    def test_check_returns_empty_string(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.check)
        assert FabricAdapter.render_column_constraint(constraint) == ""


class TestRenderModelConstraint:
    def test_unique(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.unique, name="uq_col1", columns=["col1"]
        )
        result = FabricAdapter.render_model_constraint(constraint)
        assert result == "add constraint uq_col1 unique nonclustered(col1) not enforced"

    def test_unique_multiple_columns(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.unique, name="uq_multi", columns=["col1", "col2"]
        )
        result = FabricAdapter.render_model_constraint(constraint)
        assert result == "add constraint uq_multi unique nonclustered(col1, col2) not enforced"

    def test_primary_key(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.primary_key, name="pk_id", columns=["id"]
        )
        result = FabricAdapter.render_model_constraint(constraint)
        assert result == "add constraint pk_id primary key nonclustered(id) not enforced"

    def test_foreign_key_with_expression(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.foreign_key,
            name="fk_order",
            columns=["order_id"],
            expression="orders(id)",
        )
        result = FabricAdapter.render_model_constraint(constraint)
        assert (
            result
            == "add constraint fk_order foreign key(order_id) references orders(id) not enforced"
        )

    def test_foreign_key_without_expression_returns_none(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.foreign_key, name="fk_orphan", columns=["col1"]
        )
        assert FabricAdapter.render_model_constraint(constraint) is None

    def test_custom_with_expression(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.custom,
            name="chk_positive",
            columns=["amount"],
            expression="check (amount > 0)",
        )
        result = FabricAdapter.render_model_constraint(constraint)
        assert result == "add constraint check (amount > 0)"

    def test_custom_without_expression_returns_none(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.custom, name="chk_empty", columns=["col1"]
        )
        assert FabricAdapter.render_model_constraint(constraint) is None

    def test_missing_name_raises(self):
        constraint = ModelLevelConstraint(type=ConstraintType.unique, name=None, columns=["col1"])
        with pytest.raises(dbt_common.exceptions.DbtDatabaseError, match="Constraint name"):
            FabricAdapter.render_model_constraint(constraint)

    def test_check_type_returns_none(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.check, name="chk_test", columns=["col1"]
        )
        assert FabricAdapter.render_model_constraint(constraint) is None
