from typing import Any, ClassVar, Dict

from dbt.adapters.base import Column
from dbt_common.exceptions import DbtRuntimeError


class FabricColumn(Column):
    @property
    def quoted(self) -> str:
        return "[{}]".format(self.column)

    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(MAX)",
        "VARCHAR": "VARCHAR(MAX)",
        "CHAR": "CHAR(1)",
        "NCHAR": "CHAR(1)",
        "NVARCHAR": "VARCHAR(MAX)",
        "TIMESTAMP": "DATETIME2(6)",
        "DATETIME2": "DATETIME2(6)",
        "DATETIME2(6)": "DATETIME2(6)",
        "DATE": "DATE",
        "TIME": "TIME(6)",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",
        "BIT": "BIT",
        "BOOLEAN": "BIT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "MONEY": "DECIMAL",
        "SMALLMONEY": "DECIMAL",
        "UNIQUEIDENTIFIER": "UNIQUEIDENTIFIER",
        "VARBINARY": "VARBINARY(MAX)",
        "BINARY": "BINARY(1)",
    }

    @classmethod
    def string_type(cls, size: int) -> str:
        if size is None or size <= 0:
            return "varchar(max)"
        return f"varchar({size})"

    def literal(self, value: Any) -> str:
        return "cast('{}' as {})".format(value, self.data_type)

    @property
    def data_type(self) -> str:
        # Always enforce datetime2 precision
        if self.dtype.lower() == "datetime2":
            return "datetime2(6)"
        if self.is_string():
            return self.string_type(self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype

    def is_string(self) -> bool:
        return self.dtype.lower() in ["varchar", "char"]

    def is_number(self):
        return any([self.is_integer(), self.is_numeric(), self.is_float()])

    def is_float(self):
        return self.dtype.lower() in ["float", "real"]

    def is_integer(self) -> bool:
        return self.dtype.lower() in ["int", "integer", "bigint", "smallint", "tinyint"]

    def is_numeric(self) -> bool:
        return self.dtype.lower() in ["numeric", "decimal", "money", "smallmoney"]

    def string_size(self) -> int:
        if not self.is_string():
            raise DbtRuntimeError("Called string_size() on non-string field!")
        if self.char_size is None:
            return -1
        return int(self.char_size)

    def can_expand_to(self, other_column: "FabricColumn") -> bool:
        if not self.is_string() or not other_column.is_string():
            return False
        self_size = self.string_size()
        other_size = other_column.string_size()
        if other_size == -1:
            return self_size != -1
        if self_size == -1:
            return False
        return other_size > self_size
