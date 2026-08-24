import re
from dataclasses import dataclass
from typing import Any

from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.fabric.fabric_column import FabricColumn


@dataclass(frozen=True)
class FabricTableColumn:
    name: str
    data_type: str
    max_length: int | None
    precision: int | None
    scale: int | None
    nullable: bool
    collation: str | None
    identity: bool

    @classmethod
    def from_column(cls, column: FabricColumn) -> "FabricTableColumn":
        return cls(
            name=column.name,
            data_type=column.dtype.casefold(),
            max_length=_optional_int(column.char_size),
            precision=_optional_int(column.numeric_precision),
            scale=_optional_int(column.numeric_scale),
            nullable=bool(column.is_nullable),
            collation=_optional_casefold(column.collation_name),
            identity=column.is_identity,
        )

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "FabricTableColumn":
        return cls(
            name=str(value["name"]),
            data_type=str(value["data_type"]).casefold(),
            max_length=_optional_int(value.get("max_length")),
            precision=_optional_int(value.get("precision")),
            scale=_optional_int(value.get("scale")),
            nullable=bool(value.get("is_nullable")),
            collation=_optional_casefold(value.get("collation_name")),
            identity=bool(value.get("is_identity")),
        )

    @property
    def comparison_key(self) -> tuple[Any, ...]:
        return (
            self.name.casefold(),
            self.data_type,
            self.max_length,
            self.precision,
            self.scale,
            self.nullable,
            self.collation,
            self.identity,
        )

    def can_reload_into(self, target: "FabricTableColumn") -> bool:
        query_definition = self.comparison_key[:5] + self.comparison_key[6:]
        target_definition = target.comparison_key[:5] + target.comparison_key[6:]
        if query_definition != target_definition:
            return False

        # Query metadata often marks expressions nullable even when a model contract
        # creates a NOT NULL target. Keeping the stricter target is safe; the insert
        # still fails and rolls back if the query actually produces NULL.
        return not (target.nullable and not self.nullable)


@dataclass(frozen=True)
class FabricTableConstraint:
    name: str
    constraint_type: str
    columns: tuple[str, ...]
    referenced_database: str | None = None
    referenced_schema: str | None = None
    referenced_table: str | None = None
    referenced_columns: tuple[str, ...] = ()

    @property
    def comparison_key(self) -> tuple[Any, ...]:
        return (
            self.name.casefold(),
            self.constraint_type.casefold(),
            tuple(column.casefold() for column in self.columns),
            _optional_casefold(self.referenced_database),
            _optional_casefold(self.referenced_schema),
            _optional_casefold(self.referenced_table),
            tuple(column.casefold() for column in self.referenced_columns),
        )


def build_refresh_plan(
    query_columns: list[FabricTableColumn],
    target_columns: list[FabricTableColumn],
    requested_cluster_by: str | list[str] | None,
    target_cluster_by: list[str],
) -> dict[str, Any]:
    if any(column.identity for column in query_columns + target_columns):
        return _replace_plan("identity column present", query_columns)

    if len(query_columns) != len(target_columns):
        return _replace_plan("column count changed", query_columns)

    for query_column, target_column in zip(query_columns, target_columns, strict=True):
        if not query_column.can_reload_into(target_column):
            return _replace_plan(
                f"column definition changed for {query_column.name}",
                query_columns,
            )

    requested_cluster_columns = [
        column.casefold() for column in _normalize_cluster_by(requested_cluster_by)
    ]
    target_cluster_columns = [column.casefold() for column in target_cluster_by]
    if requested_cluster_columns != target_cluster_columns:
        return _replace_plan("physical layout changed", query_columns)

    return {
        "action": "reload",
        "reason": "schema and physical layout unchanged",
        "column_names": [column.name for column in query_columns],
    }


def diff_constraints(
    desired: list[FabricTableConstraint],
    existing: list[FabricTableConstraint],
) -> tuple[list[str], list[str]]:
    desired_by_name = _constraints_by_name(desired, "desired")
    existing_by_name = _constraints_by_name(existing, "existing")

    to_drop = sorted(
        (
            constraint.name
            for name, constraint in existing_by_name.items()
            if name not in desired_by_name
            or not _constraints_equal(desired_by_name[name], constraint)
        ),
        key=str.casefold,
    )
    to_add = sorted(
        (
            constraint.name
            for name, constraint in desired_by_name.items()
            if name not in existing_by_name
            or not _constraints_equal(constraint, existing_by_name[name])
        ),
        key=str.casefold,
    )
    return to_drop, to_add


def desired_constraint(value: Any) -> FabricTableConstraint | None:
    constraint_type = _constraint_value(value, "type")
    if constraint_type is None:
        return None

    normalized_type = str(getattr(constraint_type, "value", constraint_type)).casefold()
    if normalized_type not in {"primary_key", "unique", "foreign_key"}:
        return None

    name = _constraint_value(value, "name")
    if name is None or not str(name).strip():
        raise DbtDatabaseError(f"Fabric {normalized_type} constraints must have a non-empty name.")

    columns = tuple(str(column) for column in (_constraint_value(value, "columns") or []))
    if not columns or any(not column.strip() for column in columns):
        raise DbtDatabaseError(
            f"Fabric constraint {name!s} must define at least one non-empty column."
        )

    referenced_database = None
    referenced_schema = None
    referenced_table = None
    referenced_columns: tuple[str, ...] = ()
    if normalized_type == "foreign_key":
        expression = _constraint_value(value, "expression")
        if expression is None:
            return None
        (
            referenced_database,
            referenced_schema,
            referenced_table,
            referenced_columns,
        ) = _parse_reference(expression)
        if len(referenced_columns) != len(columns):
            raise DbtDatabaseError(
                f"Fabric foreign key constraint {name!s} has {len(columns)} source "
                f"columns but {len(referenced_columns)} referenced columns."
            )

    return FabricTableConstraint(
        name=str(name),
        constraint_type=normalized_type,
        columns=columns,
        referenced_database=referenced_database,
        referenced_schema=referenced_schema,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
    )


def requires_constraint_replacement(value: Any) -> bool:
    constraint_type = _constraint_value(value, "type")
    normalized_type = str(getattr(constraint_type, "value", constraint_type)).casefold()
    return normalized_type == "custom" and bool(_constraint_value(value, "expression"))


def _constraints_by_name(
    constraints: list[FabricTableConstraint],
    source: str,
) -> dict[str, FabricTableConstraint]:
    by_name: dict[str, FabricTableConstraint] = {}
    for constraint in constraints:
        normalized_name = constraint.name.casefold()
        if normalized_name in by_name:
            raise DbtDatabaseError(f"Duplicate {source} Fabric constraint name: {constraint.name}")
        by_name[normalized_name] = constraint
    return by_name


def _constraints_equal(
    desired: FabricTableConstraint,
    existing: FabricTableConstraint,
) -> bool:
    if (
        desired.name.casefold() != existing.name.casefold()
        or desired.constraint_type.casefold() != existing.constraint_type.casefold()
        or _normalized_identifiers(desired.columns) != _normalized_identifiers(existing.columns)
        or _optional_casefold(desired.referenced_table)
        != _optional_casefold(existing.referenced_table)
        or _normalized_identifiers(desired.referenced_columns)
        != _normalized_identifiers(existing.referenced_columns)
    ):
        return False

    if desired.referenced_schema is not None and _optional_casefold(
        desired.referenced_schema
    ) != _optional_casefold(existing.referenced_schema):
        return False
    return not (
        desired.referenced_database is not None
        and _optional_casefold(desired.referenced_database)
        != _optional_casefold(existing.referenced_database)
    )


def _normalized_identifiers(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(value.casefold() for value in values)


def _parse_reference(
    expression: Any,
) -> tuple[str | None, str | None, str, tuple[str, ...]]:
    normalized = str(expression).strip()
    opening_parenthesis = normalized.find("(")
    if opening_parenthesis <= 0 or not normalized.endswith(")"):
        raise DbtDatabaseError(
            "Fabric foreign key expressions must use '[database.][schema.]table (column[, ...])'."
        )

    relation_text = normalized[:opening_parenthesis].strip()
    columns_text = normalized[opening_parenthesis + 1 : -1].strip()
    relation_parts = _split_sql_identifiers(relation_text, ".")
    if not 1 <= len(relation_parts) <= 3:
        raise DbtDatabaseError(
            "Fabric foreign key references must contain a table and optional schema and database."
        )
    referenced_columns = tuple(_split_sql_identifiers(columns_text, ","))
    if not referenced_columns:
        raise DbtDatabaseError("Fabric foreign key expressions must define referenced columns.")

    database = relation_parts[-3] if len(relation_parts) == 3 else None
    schema = relation_parts[-2] if len(relation_parts) >= 2 else None
    return database, schema, relation_parts[-1], referenced_columns


def _split_sql_identifiers(value: str, separator: str) -> list[str]:
    identifiers: list[str] = []
    token: list[str] = []
    in_brackets = False
    quoted = False
    bracket_closed = False
    index = 0
    while index < len(value):
        character = value[index]
        if character == "[":
            if in_brackets or quoted or "".join(token).strip():
                raise _invalid_identifier_list(value)
            in_brackets = True
            quoted = True
        elif character == "]" and in_brackets:
            if index + 1 < len(value) and value[index + 1] == "]":
                token.append("]")
                index += 1
            else:
                in_brackets = False
                bracket_closed = True
        elif character == separator and not in_brackets:
            identifiers.append(_validate_identifier("".join(token), value, quoted))
            token = []
            quoted = False
            bracket_closed = False
        elif bracket_closed and not character.isspace():
            raise _invalid_identifier_list(value)
        else:
            token.append(character)
        index += 1

    if in_brackets:
        raise _invalid_identifier_list(value)
    identifiers.append(_validate_identifier("".join(token), value, quoted))
    return identifiers


def _validate_identifier(identifier: str, source: str, quoted: bool) -> str:
    normalized = identifier.strip()
    if not normalized or (not quoted and not re.fullmatch(r"[\w@$#]+", normalized)):
        raise _invalid_identifier_list(source)
    return normalized


def _invalid_identifier_list(value: str) -> DbtDatabaseError:
    return DbtDatabaseError(
        f"Invalid Fabric constraint identifier list: {value!r}. "
        "Use bare or bracket-quoted identifiers."
    )


def _replace_plan(reason: str, columns: list[FabricTableColumn]) -> dict[str, Any]:
    return {
        "action": "replace",
        "reason": reason,
        "column_names": [column.name for column in columns],
    }


def _normalize_cluster_by(cluster_by: str | list[str] | None) -> list[str]:
    if cluster_by is None:
        return []
    if isinstance(cluster_by, str):
        return [cluster_by]
    return list(cluster_by)


def query_references_relation(sql: str, relation_names: list[str]) -> bool:
    normalized_sql = sql.casefold()
    return any(name.casefold() in normalized_sql for name in relation_names)


def _constraint_value(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_casefold(value: Any) -> str | None:
    return None if value is None else str(value).casefold()
