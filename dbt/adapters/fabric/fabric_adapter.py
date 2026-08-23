import agate
import dbt_common.exceptions
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.events.functions import fire_event
from dbt_common.utils.dict import filter_null_values

from dbt.adapters.base.column import Column as BaseColumn
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.events.types import SchemaCreation
from dbt.adapters.fabric.base_fabric_adapter import BaseFabricAdapter
from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.fabric_configs import FabricConfigs
from dbt.adapters.fabric.fabric_connection_manager import FabricConnectionManager
from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.adapters.fabric.table_refresh import (
    FabricTableColumn,
    FabricTableConstraint,
    build_refresh_plan,
    desired_constraint,
    diff_constraints,
    query_references_relation,
    requires_constraint_replacement,
)
from dbt.adapters.reference_keys import _make_ref_key_dict
from dbt.adapters.sql.impl import CREATE_SCHEMA_MACRO_NAME, SQLAdapter


class FabricAdapter(BaseFabricAdapter, SQLAdapter):
    ConnectionManager = FabricConnectionManager
    connections: FabricConnectionManager
    Column = FabricColumn
    AdapterSpecificConfigs = FabricConfigs
    Relation = FabricRelation

    @classmethod
    def quote(cls, identifier):
        return "[{}]".format(identifier.replace("]", "]]"))

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
        }
    )
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> list[BaseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        _, cursor = self.connections.add_select_query(sql)
        return [
            self.Column.create(
                column_name,
                self.connections.data_type_code_to_name(column_type_code),
            )
            for column_name, column_type_code, *_ in cursor.description
        ]

    @available.parse(
        lambda *a, **k: {
            "action": "replace",
            "reason": "parse-time default",
            "column_names": [],
            "constraints_to_drop": [],
            "constraint_add_sql": [],
        }
    )
    def get_table_refresh_plan(
        self,
        relation: BaseRelation,
        sql: str,
        cluster_by: str | list[str] | None = None,
        constraints: list[object] | None = None,
    ) -> dict[str, object]:
        """Choose whether a full refresh can preserve the existing table object."""
        query_columns = [
            FabricTableColumn.from_column(column) for column in self._describe_query_columns(sql)
        ]
        target_columns = [
            FabricTableColumn.from_column(column)
            for column in self.get_columns_in_relation(relation)
        ]
        target_cluster_by = self._get_table_cluster_by(relation)
        target_constraints = self.get_constraints_in_relation(relation)
        raw_constraints = constraints or []
        desired_constraints: list[FabricTableConstraint] = []
        raw_constraints_by_name: dict[str, object] = {}
        replacement_required = False
        for constraint in raw_constraints:
            replacement_required = replacement_required or requires_constraint_replacement(
                constraint
            )
            parsed_constraint = desired_constraint(constraint)
            if parsed_constraint is None:
                continue
            normalized_name = parsed_constraint.name.casefold()
            if normalized_name in raw_constraints_by_name:
                raise dbt_common.exceptions.DbtDatabaseError(
                    f"Duplicate desired Fabric constraint name: {parsed_constraint.name}"
                )
            desired_constraints.append(parsed_constraint)
            raw_constraints_by_name[normalized_name] = constraint

        refresh_plan = build_refresh_plan(
            query_columns,
            target_columns,
            cluster_by,
            target_cluster_by,
        )
        constraints_to_drop, constraints_to_add = diff_constraints(
            desired_constraints,
            target_constraints,
        )
        refresh_plan["constraints_to_drop"] = constraints_to_drop
        refresh_plan["constraints_to_add"] = constraints_to_add
        refresh_plan["constraint_add_sql"] = [
            rendered_constraint
            for constraint_name in constraints_to_add
            for rendered_constraint in self.render_raw_model_constraints(
                raw_constraints=[raw_constraints_by_name[constraint_name.casefold()]]
            )
        ]
        if refresh_plan["action"] == "reload" and replacement_required:
            refresh_plan["action"] = "replace"
            refresh_plan["reason"] = "constraint definition requires table replacement"
        if refresh_plan["action"] == "reload" and query_references_relation(
            sql,
            [
                str(relation),
                str(relation.include(database=False)),
            ],
        ):
            return {
                "action": "replace",
                "reason": "query references the target relation",
                "column_names": [column.name for column in query_columns],
                "constraints_to_drop": constraints_to_drop,
                "constraints_to_add": constraints_to_add,
                "constraint_add_sql": refresh_plan["constraint_add_sql"],
            }
        return refresh_plan

    def _describe_query_columns(self, sql: str) -> list[FabricColumn]:
        escaped_sql = sql.replace("'", "''")
        rows = self._fetch_dicts(f"EXEC sys.sp_describe_first_result_set @tsql = N'{escaped_sql}'")
        return [
            FabricColumn(
                column=str(row["name"]),
                dtype=str(row["system_type_name"]).split("(", 1)[0],
                char_size=self._column_character_size(
                    str(row["system_type_name"]).split("(", 1)[0],
                    row["max_length"],
                ),
                numeric_precision=row["precision"],
                numeric_scale=row["scale"],
                is_nullable=bool(row["is_nullable"]),
                collation_name=(
                    None if row["collation_name"] is None else str(row["collation_name"])
                ),
                is_identity=bool(row["is_identity_column"]),
            )
            for row in rows
            if not row["is_hidden"]
        ]

    @staticmethod
    def _column_character_size(data_type: str, max_length: object) -> int | None:
        if max_length is None:
            return None
        if not isinstance(max_length, (int, str)):
            raise TypeError(f"Unexpected max_length value: {max_length!r}")
        size = int(max_length)
        if data_type.casefold() in {"nchar", "nvarchar", "sysname"} and size != -1:
            return size // 2
        return size

    def _get_table_cluster_by(self, relation: BaseRelation) -> list[str]:
        relation_name = str(relation).replace("'", "''")
        rows = self._fetch_dicts(
            f"""
            {self._use_database_sql(relation.database)}
            SELECT c.name
            FROM sys.columns AS c
            INNER JOIN sys.index_columns AS ic
                ON c.object_id = ic.object_id
                AND c.column_id = ic.column_id
            WHERE c.object_id = OBJECT_ID(N'{relation_name}')
                AND ic.data_clustering_ordinal > 0
            ORDER BY ic.data_clustering_ordinal
            """
        )
        return [str(row["name"]) for row in rows]

    @available.parse(lambda *a, **k: [])
    def get_constraints_in_relation(self, relation: BaseRelation) -> list[FabricTableConstraint]:
        """Return the named constraints defined on a Fabric table."""
        relation_name = str(relation).replace("'", "''")
        rows = self._fetch_dicts(
            f"""
            {self._use_database_sql(relation.database)}
            SELECT
                kc.name,
                CASE kc.type WHEN 'PK' THEN 'primary_key' ELSE 'unique' END
                    AS constraint_type,
                c.name AS column_name,
                ic.key_ordinal AS column_ordinal,
                CAST(NULL AS varchar(128)) AS referenced_table,
                CAST(NULL AS varchar(128)) AS referenced_schema,
                CAST(NULL AS varchar(128)) AS referenced_database,
                CAST(NULL AS varchar(128)) AS referenced_column
            FROM sys.key_constraints AS kc
            INNER JOIN sys.index_columns AS ic
                ON kc.parent_object_id = ic.object_id
                AND kc.unique_index_id = ic.index_id
            INNER JOIN sys.columns AS c
                ON ic.object_id = c.object_id
                AND ic.column_id = c.column_id
            WHERE kc.parent_object_id = OBJECT_ID(N'{relation_name}')
            UNION ALL
            SELECT
                fk.name,
                'foreign_key',
                parent_column.name,
                fkc.constraint_column_id,
                referenced_table.name,
                referenced_schema.name,
                DB_NAME(),
                referenced_column.name
            FROM sys.foreign_keys AS fk
            INNER JOIN sys.foreign_key_columns AS fkc
                ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.columns AS parent_column
                ON fkc.parent_object_id = parent_column.object_id
                AND fkc.parent_column_id = parent_column.column_id
            INNER JOIN sys.tables AS referenced_table
                ON fkc.referenced_object_id = referenced_table.object_id
            INNER JOIN sys.schemas AS referenced_schema
                ON referenced_table.schema_id = referenced_schema.schema_id
            INNER JOIN sys.columns AS referenced_column
                ON fkc.referenced_object_id = referenced_column.object_id
                AND fkc.referenced_column_id = referenced_column.column_id
            WHERE fk.parent_object_id = OBJECT_ID(N'{relation_name}')
            ORDER BY name, column_ordinal
            """
        )
        grouped: dict[
            str,
            tuple[
                str,
                list[str],
                str | None,
                str | None,
                str | None,
                list[str],
            ],
        ] = {}
        for row in rows:
            name = str(row["name"])
            _, columns, _, _, _, referenced_columns = grouped.setdefault(
                name,
                (
                    str(row["constraint_type"]),
                    [],
                    (
                        None
                        if row["referenced_database"] is None
                        else str(row["referenced_database"])
                    ),
                    (None if row["referenced_schema"] is None else str(row["referenced_schema"])),
                    (None if row["referenced_table"] is None else str(row["referenced_table"])),
                    [],
                ),
            )
            columns.append(str(row["column_name"]))
            if row["referenced_column"] is not None:
                referenced_columns.append(str(row["referenced_column"]))
        return [
            FabricTableConstraint(
                name=name,
                constraint_type=constraint_type,
                columns=tuple(columns),
                referenced_database=referenced_database,
                referenced_schema=referenced_schema,
                referenced_table=referenced_table,
                referenced_columns=tuple(referenced_columns),
            )
            for name, (
                constraint_type,
                columns,
                referenced_database,
                referenced_schema,
                referenced_table,
                referenced_columns,
            ) in grouped.items()
        ]

    def _fetch_dicts(self, sql: str) -> list[dict[str, object]]:
        _, cursor = self.connections.add_select_query(sql)
        try:
            while cursor.description is None and cursor.nextset():
                pass
            if cursor.description is None:
                return []
            fields = [str(description[0]).casefold() for description in cursor.description]
            return [dict(zip(fields, row, strict=True)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    @classmethod
    def _use_database_sql(cls, database: str | None) -> str:
        if database is None:
            return ""
        return f"USE {cls.quote(database)};"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bit"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime2(6)"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    def create_schema(self, relation: BaseRelation) -> None:
        relation = relation.without_identifier()
        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
        macro_name = CREATE_SCHEMA_MACRO_NAME
        kwargs = {
            "relation": relation,
        }

        if self.config.credentials.schema_authorization:
            kwargs["schema_authorization"] = self.config.credentials.schema_authorization
            macro_name = "fabric__create_schema_with_authorization"

        self.execute_macro(macro_name, kwargs=kwargs)
        self.commit_if_has_connection()

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        # see https://github.com/fishtown-analytics/dbt/pull/2255
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        length = max_len if max_len > 16 else 16
        return f"varchar({length})"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time(6)"

    @classmethod
    def date_function(cls):
        return "getdate()"

    # Methods used in adapter tests
    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # note: 'interval' is not supported for T-SQL
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"DATEADD({interval},{number},{add_to})"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location="append",
    ) -> str:
        """
        `+` is T-SQL's string concatenation operator
        """
        if location == "append":
            return f"{add_to} + '{value}'"
        elif location == "prepend":
            return f"'{value}' + {add_to}"
        else:
            raise ValueError(f'Got an unexpected location value of "{location}"')

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: list[str] | None = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """
        note: using is not supported on Synapse so COLUMNS_EQUAL_SQL is adjsuted
        Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: list[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted(self.quote(c.name) for c in columns)
        else:
            names = sorted(self.quote(n) for n in column_names)
        columns_csv = ", ".join(names)

        if columns_csv == "":
            columns_csv = "*"

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert", "microbatch", "merge"]

    # This is for use in the test suite
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if not fetch:
                conn.handle.commit()
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            raise
        finally:
            conn.transaction_open = False

    @available
    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> str | None:
        rendered_column_constraint = None
        if constraint.type == ConstraintType.not_null:
            rendered_column_constraint = "not null "
        else:
            rendered_column_constraint = ""

        if rendered_column_constraint:
            rendered_column_constraint = rendered_column_constraint.strip()

        return rendered_column_constraint

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> str | None:
        constraint_prefix = "add constraint "
        column_list = ", ".join(constraint.columns)

        if constraint.name is None:
            raise dbt_common.exceptions.DbtDatabaseError(
                "Constraint name cannot be empty. Provide constraint name  - column "
                + column_list
                + " and run the project again."
            )

        if constraint.type == ConstraintType.unique:
            return (
                constraint_prefix
                + f"{constraint.name} unique nonclustered({column_list}) not enforced"
            )
        elif constraint.type == ConstraintType.primary_key:
            return (
                constraint_prefix
                + f"{constraint.name} primary key nonclustered({column_list}) not enforced"
            )
        elif constraint.type == ConstraintType.foreign_key and constraint.expression:
            return (
                constraint_prefix
                + f"{constraint.name} foreign key({column_list}) references "
                + f"{constraint.expression} not enforced"
            )
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix}{constraint.expression}"
        else:
            return None

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> dict[str, str]:
        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
                "schema": schema,
            }
        )

    @available
    def create_or_update_warehouse_snapshot(
        self, snapshot_name: str, description: str | None = None
    ) -> str:
        """Create a new warehouse snapshot or update an existing one with the same name.

        Exposed as a Jinja macro via ``@available``, so it can be called from
        ``on-run-start``, ``on-run-end``, ``post-hook``, or any other Jinja context.

        Args:
            snapshot_name: Display name for the snapshot.
            description: Optional description for the snapshot.
        """
        api = self.connections.get_fabric_api_client(self.config.credentials)
        api.create_or_update_warehouse_snapshot(snapshot_name, description)
        return ""


COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count on row_count_diff.id = diff_count.id
""".strip()
