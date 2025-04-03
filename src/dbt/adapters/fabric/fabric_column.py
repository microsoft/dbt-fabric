from typing import Any, ClassVar, Dict

from dbt.adapters.base import Column


class FabricColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR(8000)",
        "TIMESTAMP": "DATETIME2(6)",
        "FLOAT": "FLOAT",
        "INTEGER": "INT",
        "BOOLEAN": "BIT",
    }

    @classmethod
    def string_type(cls, size: int) -> str:
        return f"varchar({size if size > 0 else '8000'})"

    def literal(self, value: Any) -> str:
        return "cast('{}' as {})".format(value, self.data_type)

    def is_integer(self):
        return super().is_integer() or self.dtype.lower() == "int"

    @property
    def data_type(self) -> str:
        if self.dtype == "datetime2":
            return f"datetime2({self.numeric_scale})"
        return super().data_type
