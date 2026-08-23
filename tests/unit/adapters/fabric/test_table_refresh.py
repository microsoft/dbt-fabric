import pytest
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.table_refresh import (
    FabricTableColumn,
    FabricTableConstraint,
    build_refresh_plan,
    desired_constraint,
    diff_constraints,
    query_references_relation,
    requires_constraint_replacement,
)


def _column(
    name="id",
    data_type="int",
    max_length=4,
    precision=10,
    scale=0,
    nullable=False,
    collation=None,
    identity=False,
):
    return FabricTableColumn(
        name=name,
        data_type=data_type,
        max_length=max_length,
        precision=precision,
        scale=scale,
        nullable=nullable,
        collation=collation,
        identity=identity,
    )


class TestBuildRefreshPlan:
    def test_reload_when_schema_and_layout_match(self):
        columns = [_column(), _column(name="name", data_type="varchar", max_length=100)]

        plan = build_refresh_plan(columns, columns, ["id"], ["ID"])

        assert plan == {
            "action": "reload",
            "reason": "schema and physical layout unchanged",
            "column_names": ["id", "name"],
        }

    @pytest.mark.parametrize(
        ("query_columns", "target_columns"),
        [
            ([_column(name="new_id")], [_column()]),
            ([_column(data_type="bigint")], [_column()]),
            ([_column(max_length=8)], [_column()]),
            ([_column(precision=12)], [_column()]),
            ([_column(scale=2)], [_column()]),
            ([_column(nullable=False)], [_column(nullable=True)]),
            ([_column(collation="Latin1_General_100_CI_AS")], [_column()]),
            ([_column(), _column(name="other")], [_column()]),
        ],
    )
    def test_replace_when_schema_changes(self, query_columns, target_columns):
        plan = build_refresh_plan(query_columns, target_columns, None, [])

        assert plan["action"] == "replace"

    def test_reload_keeps_stricter_target_nullability(self):
        plan = build_refresh_plan(
            [_column(nullable=True)],
            [_column(nullable=False)],
            None,
            [],
        )

        assert plan["action"] == "reload"

    def test_replace_when_identity_is_present(self):
        column = _column(data_type="bigint", max_length=8, precision=19, identity=True)

        plan = build_refresh_plan([column], [column], None, [])

        assert plan["action"] == "replace"
        assert plan["reason"] == "identity column present"

    def test_replace_when_cluster_by_changes(self):
        columns = [_column(), _column(name="created_at", data_type="datetime2")]

        plan = build_refresh_plan(columns, columns, ["created_at"], ["id"])

        assert plan["action"] == "replace"
        assert plan["reason"] == "physical layout changed"

    def test_cluster_by_order_change_forces_replacement(self):
        columns = [_column(), _column(name="created_at", data_type="datetime2")]

        plan = build_refresh_plan(columns, columns, ["created_at", "id"], ["id", "created_at"])

        assert plan["action"] == "replace"


class TestFabricTableColumn:
    def test_from_mapping_normalizes_values(self):
        column = FabricTableColumn.from_mapping(
            {
                "name": "Name",
                "data_type": "VARCHAR",
                "max_length": "100",
                "precision": None,
                "scale": None,
                "is_nullable": 1,
                "collation_name": "Latin1_General_100_CI_AS",
                "is_identity": 0,
            }
        )

        assert column == FabricTableColumn(
            name="Name",
            data_type="varchar",
            max_length=100,
            precision=None,
            scale=None,
            nullable=True,
            collation="latin1_general_100_ci_as",
            identity=False,
        )

    def test_from_fabric_column_normalizes_values(self):
        column = FabricTableColumn.from_column(
            FabricColumn(
                column="Name",
                dtype="VARCHAR",
                char_size=100,
                is_nullable=True,
                collation_name="Latin1_General_100_CI_AS",
            )
        )

        assert column == FabricTableColumn(
            name="Name",
            data_type="varchar",
            max_length=100,
            precision=None,
            scale=None,
            nullable=True,
            collation="latin1_general_100_ci_as",
            identity=False,
        )


class TestQueryReferencesRelation:
    def test_detects_quoted_relation(self):
        assert query_references_relation(
            "select * from [warehouse].[dbo].[target]",
            ["[warehouse].[dbo].[target]", "[dbo].[target]"],
        )

    def test_is_case_insensitive(self):
        assert query_references_relation(
            "select * from [DBO].[TARGET]",
            ["[dbo].[target]"],
        )

    def test_ignores_unrelated_queries(self):
        assert not query_references_relation(
            "select * from [dbo].[source]",
            ["[dbo].[target]"],
        )


class TestConstraintDiff:
    def test_unchanged_constraint_is_preserved(self):
        constraint = FabricTableConstraint("pk_model", "primary_key", ("id",))

        assert diff_constraints([constraint], [constraint]) == ([], [])

    def test_changed_constraint_is_replaced(self):
        desired = FabricTableConstraint("pk_model", "primary_key", ("new_id",))
        existing = FabricTableConstraint("pk_model", "primary_key", ("id",))

        assert diff_constraints([desired], [existing]) == (["pk_model"], ["pk_model"])

    def test_added_and_removed_constraints(self):
        desired = FabricTableConstraint("uq_model", "unique", ("code",))
        existing = FabricTableConstraint("pk_model", "primary_key", ("id",))

        assert diff_constraints([desired], [existing]) == (["pk_model"], ["uq_model"])

    def test_diff_is_case_insensitive_and_deterministic(self):
        desired = [
            FabricTableConstraint("z_constraint", "unique", ("code",)),
            FabricTableConstraint("A_constraint", "primary_key", ("id",)),
        ]
        existing = [
            FabricTableConstraint("old_z", "unique", ("legacy",)),
            FabricTableConstraint("OLD_A", "primary_key", ("legacy_id",)),
        ]

        assert diff_constraints(desired, existing) == (
            ["OLD_A", "old_z"],
            ["A_constraint", "z_constraint"],
        )

    def test_duplicate_desired_names_are_rejected_case_insensitively(self):
        constraints = [
            FabricTableConstraint("PK_Model", "primary_key", ("id",)),
            FabricTableConstraint("pk_model", "primary_key", ("id",)),
        ]

        with pytest.raises(DbtDatabaseError, match="Duplicate desired"):
            diff_constraints(constraints, [])

    def test_desired_foreign_key_parses_expression(self):
        constraint = desired_constraint(
            {
                "name": "fk_order",
                "type": "foreign_key",
                "columns": ["order_id"],
                "expression": "[sales].[orders] ([id])",
            }
        )

        assert constraint == FabricTableConstraint(
            name="fk_order",
            constraint_type="foreign_key",
            columns=("order_id",),
            referenced_schema="sales",
            referenced_table="orders",
            referenced_columns=("id",),
        )

    def test_foreign_key_database_schema_and_quoted_identifiers_are_preserved(self):
        constraint = desired_constraint(
            {
                "name": "fk_order",
                "type": "foreign_key",
                "columns": ["order id"],
                "expression": "[warehouse].[sales.data].[orders]]archive] ([order id])",
            }
        )

        assert constraint == FabricTableConstraint(
            name="fk_order",
            constraint_type="foreign_key",
            columns=("order id",),
            referenced_database="warehouse",
            referenced_schema="sales.data",
            referenced_table="orders]archive",
            referenced_columns=("order id",),
        )

    def test_qualified_foreign_key_detects_schema_change(self):
        desired = FabricTableConstraint(
            "fk_order",
            "foreign_key",
            ("order_id",),
            referenced_schema="sales",
            referenced_table="orders",
            referenced_columns=("id",),
        )
        existing = FabricTableConstraint(
            "fk_order",
            "foreign_key",
            ("order_id",),
            referenced_schema="archive",
            referenced_table="orders",
            referenced_columns=("id",),
        )

        assert diff_constraints([desired], [existing]) == (["fk_order"], ["fk_order"])

    def test_unqualified_foreign_key_matches_resolved_schema(self):
        desired = FabricTableConstraint(
            "fk_order",
            "foreign_key",
            ("order_id",),
            referenced_table="orders",
            referenced_columns=("id",),
        )
        existing = FabricTableConstraint(
            "fk_order",
            "foreign_key",
            ("order_id",),
            referenced_database="warehouse",
            referenced_schema="dbo",
            referenced_table="orders",
            referenced_columns=("id",),
        )

        assert diff_constraints([desired], [existing]) == ([], [])

    @pytest.mark.parametrize(
        "expression",
        [
            "orders",
            "orders ()",
            "sales..orders (id)",
            "sales orders (id)",
            "[sales].[orders (id)",
            "sales.orders (id; drop table users)",
        ],
    )
    def test_invalid_foreign_key_expression_is_rejected(self, expression):
        with pytest.raises(DbtDatabaseError):
            desired_constraint(
                {
                    "name": "fk_order",
                    "type": "foreign_key",
                    "columns": ["order_id"],
                    "expression": expression,
                }
            )

    def test_foreign_key_column_count_must_match(self):
        with pytest.raises(DbtDatabaseError, match="source columns"):
            desired_constraint(
                {
                    "name": "fk_order",
                    "type": "foreign_key",
                    "columns": ["order_id", "tenant_id"],
                    "expression": "sales.orders (id)",
                }
            )

    @pytest.mark.parametrize("constraint_type", ["primary_key", "unique"])
    def test_supported_constraint_requires_name(self, constraint_type):
        with pytest.raises(DbtDatabaseError, match="non-empty name"):
            desired_constraint(
                {
                    "type": constraint_type,
                    "columns": ["id"],
                }
            )

    def test_supported_constraint_requires_columns(self):
        with pytest.raises(DbtDatabaseError, match="at least one"):
            desired_constraint(
                {
                    "name": "pk_model",
                    "type": "primary_key",
                    "columns": [],
                }
            )

    def test_renderable_custom_constraint_requires_replacement(self):
        assert requires_constraint_replacement(
            {
                "name": "custom_model",
                "type": "custom",
                "expression": "constraint custom_model unique (id)",
            }
        )

    def test_non_renderable_constraint_does_not_require_replacement(self):
        assert not requires_constraint_replacement(
            {
                "name": "check_model",
                "type": "check",
                "expression": "id > 0",
            }
        )
