"""Unit tests for fabric_column module."""

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabric.fabric_column import FabricColumn


class TestFabricColumnTypeLabels:
    """Tests for TYPE_LABELS class variable."""

    def test_type_labels_exist(self):
        """Verify TYPE_LABELS has expected entries."""
        assert FabricColumn.TYPE_LABELS["STRING"] == "VARCHAR(8000)"
        assert FabricColumn.TYPE_LABELS["VARCHAR"] == "VARCHAR(8000)"
        assert FabricColumn.TYPE_LABELS["TIMESTAMP"] == "DATETIME2(6)"
        assert FabricColumn.TYPE_LABELS["INT"] == "INT"
        assert FabricColumn.TYPE_LABELS["BIGINT"] == "BIGINT"
        assert FabricColumn.TYPE_LABELS["BOOLEAN"] == "BIT"
        assert FabricColumn.TYPE_LABELS["UNIQUEIDENTIFIER"] == "UNIQUEIDENTIFIER"

    def test_all_common_types_mapped(self):
        """Verify all common SQL types are mapped."""
        expected_types = [
            "STRING", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR",
            "TIMESTAMP", "DATETIME2", "DATE", "TIME",
            "FLOAT", "REAL", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
            "BIT", "BOOLEAN", "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY",
            "UNIQUEIDENTIFIER", "VARBINARY", "BINARY",
        ]
        for type_name in expected_types:
            assert type_name in FabricColumn.TYPE_LABELS


class TestFabricColumnStringType:
    """Tests for string_type class method."""

    def test_string_type_with_size(self):
        """Verify string_type returns correct format with size."""
        result = FabricColumn.string_type(100)
        assert result == "varchar(100)"

    def test_string_type_with_zero_size(self):
        """Verify string_type defaults to 8000 for zero size."""
        result = FabricColumn.string_type(0)
        assert result == "varchar(8000)"

    def test_string_type_with_negative_size(self):
        """Verify string_type defaults to 8000 for negative size."""
        result = FabricColumn.string_type(-1)
        assert result == "varchar(8000)"


class TestFabricColumnDataType:
    """Tests for data_type property."""

    def test_data_type_datetime2(self):
        """Verify datetime2 enforces precision."""
        column = FabricColumn(
            column="test_col",
            dtype="datetime2",
        )
        assert column.data_type == "datetime2(6)"

    def test_data_type_datetime2_uppercase(self):
        """Verify DATETIME2 (uppercase) enforces precision."""
        column = FabricColumn(
            column="test_col",
            dtype="DATETIME2",
        )
        assert column.data_type == "datetime2(6)"

    def test_data_type_varchar(self):
        """Verify varchar returns correct string type."""
        column = FabricColumn(
            column="test_col",
            dtype="varchar",
            char_size=255,
        )
        assert column.data_type == "varchar(255)"

    def test_data_type_char(self):
        """Verify char returns correct string type."""
        column = FabricColumn(
            column="test_col",
            dtype="char",
            char_size=10,
        )
        assert column.data_type == "varchar(10)"

    def test_data_type_numeric(self):
        """Verify numeric types include precision and scale."""
        column = FabricColumn(
            column="test_col",
            dtype="decimal",
            numeric_precision=18,
            numeric_scale=2,
        )
        # numeric_type is inherited from base class
        assert "decimal" in column.data_type.lower(
        ) or "numeric" in column.data_type.lower()

    def test_data_type_passthrough(self):
        """Verify non-special types are passed through."""
        column = FabricColumn(
            column="test_col",
            dtype="uniqueidentifier",
        )
        assert column.data_type == "uniqueidentifier"


class TestFabricColumnIsString:
    """Tests for is_string method."""

    def test_is_string_varchar(self):
        """Verify varchar is detected as string."""
        column = FabricColumn(column="test_col", dtype="varchar")
        assert column.is_string() is True

    def test_is_string_char(self):
        """Verify char is detected as string."""
        column = FabricColumn(column="test_col", dtype="char")
        assert column.is_string() is True

    def test_is_string_int(self):
        """Verify int is not detected as string."""
        column = FabricColumn(column="test_col", dtype="int")
        assert column.is_string() is False

    def test_is_string_nvarchar(self):
        """Verify nvarchar is NOT detected as string (not in list)."""
        column = FabricColumn(column="test_col", dtype="nvarchar")
        assert column.is_string() is False


class TestFabricColumnIsNumber:
    """Tests for is_number method."""

    def test_is_number_int(self):
        """Verify int is detected as number."""
        column = FabricColumn(column="test_col", dtype="int")
        assert column.is_number() is True

    def test_is_number_float(self):
        """Verify float is detected as number."""
        column = FabricColumn(column="test_col", dtype="float")
        assert column.is_number() is True

    def test_is_number_decimal(self):
        """Verify decimal is detected as number."""
        column = FabricColumn(column="test_col", dtype="decimal")
        assert column.is_number() is True

    def test_is_number_varchar(self):
        """Verify varchar is not detected as number."""
        column = FabricColumn(column="test_col", dtype="varchar")
        assert column.is_number() is False


class TestFabricColumnIsFloat:
    """Tests for is_float method."""

    def test_is_float_float(self):
        """Verify float is detected as float."""
        column = FabricColumn(column="test_col", dtype="float")
        assert column.is_float() is True

    def test_is_float_real(self):
        """Verify real is detected as float."""
        column = FabricColumn(column="test_col", dtype="real")
        assert column.is_float() is True

    def test_is_float_int(self):
        """Verify int is not detected as float."""
        column = FabricColumn(column="test_col", dtype="int")
        assert column.is_float() is False


class TestFabricColumnIsInteger:
    """Tests for is_integer method."""

    def test_is_integer_int(self):
        """Verify int is detected as integer."""
        column = FabricColumn(column="test_col", dtype="int")
        assert column.is_integer() is True

    def test_is_integer_integer(self):
        """Verify integer is detected as integer."""
        column = FabricColumn(column="test_col", dtype="integer")
        assert column.is_integer() is True

    def test_is_integer_bigint(self):
        """Verify bigint is detected as integer."""
        column = FabricColumn(column="test_col", dtype="bigint")
        assert column.is_integer() is True

    def test_is_integer_smallint(self):
        """Verify smallint is detected as integer."""
        column = FabricColumn(column="test_col", dtype="smallint")
        assert column.is_integer() is True

    def test_is_integer_tinyint(self):
        """Verify tinyint is detected as integer."""
        column = FabricColumn(column="test_col", dtype="tinyint")
        assert column.is_integer() is True

    def test_is_integer_float(self):
        """Verify float is not detected as integer."""
        column = FabricColumn(column="test_col", dtype="float")
        assert column.is_integer() is False


class TestFabricColumnIsNumeric:
    """Tests for is_numeric method."""

    def test_is_numeric_decimal(self):
        """Verify decimal is detected as numeric."""
        column = FabricColumn(column="test_col", dtype="decimal")
        assert column.is_numeric() is True

    def test_is_numeric_numeric(self):
        """Verify numeric is detected as numeric."""
        column = FabricColumn(column="test_col", dtype="numeric")
        assert column.is_numeric() is True

    def test_is_numeric_money(self):
        """Verify money is detected as numeric."""
        column = FabricColumn(column="test_col", dtype="money")
        assert column.is_numeric() is True

    def test_is_numeric_smallmoney(self):
        """Verify smallmoney is detected as numeric."""
        column = FabricColumn(column="test_col", dtype="smallmoney")
        assert column.is_numeric() is True

    def test_is_numeric_int(self):
        """Verify int is not detected as numeric."""
        column = FabricColumn(column="test_col", dtype="int")
        assert column.is_numeric() is False


class TestFabricColumnStringSize:
    """Tests for string_size method."""

    def test_string_size_with_char_size(self):
        """Verify string_size returns char_size."""
        column = FabricColumn(
            column="test_col", dtype="varchar", char_size=100)
        assert column.string_size() == 100

    def test_string_size_without_char_size(self):
        """Verify string_size returns 8000 when char_size is None."""
        column = FabricColumn(
            column="test_col", dtype="varchar", char_size=None)
        assert column.string_size() == 8000

    def test_string_size_on_non_string_raises(self):
        """Verify string_size raises on non-string column."""
        column = FabricColumn(column="test_col", dtype="int")
        with pytest.raises(DbtRuntimeError, match="non-string"):
            column.string_size()


class TestFabricColumnCanExpandTo:
    """Tests for can_expand_to method."""

    def test_can_expand_to_larger_varchar(self):
        """Verify can expand to larger varchar."""
        column1 = FabricColumn(column="col1", dtype="varchar", char_size=100)
        column2 = FabricColumn(column="col2", dtype="varchar", char_size=200)

        assert column1.can_expand_to(column2) is True

    def test_cannot_expand_to_smaller_varchar(self):
        """Verify cannot expand to smaller varchar."""
        column1 = FabricColumn(column="col1", dtype="varchar", char_size=200)
        column2 = FabricColumn(column="col2", dtype="varchar", char_size=100)

        assert column1.can_expand_to(column2) is False

    def test_cannot_expand_to_same_size(self):
        """Verify cannot expand to same size."""
        column1 = FabricColumn(column="col1", dtype="varchar", char_size=100)
        column2 = FabricColumn(column="col2", dtype="varchar", char_size=100)

        assert column1.can_expand_to(column2) is False

    def test_cannot_expand_non_string_to_string(self):
        """Verify non-string cannot expand to string."""
        column1 = FabricColumn(column="col1", dtype="int")
        column2 = FabricColumn(column="col2", dtype="varchar", char_size=100)

        assert column1.can_expand_to(column2) is False

    def test_cannot_expand_string_to_non_string(self):
        """Verify string cannot expand to non-string."""
        column1 = FabricColumn(column="col1", dtype="varchar", char_size=100)
        column2 = FabricColumn(column="col2", dtype="int")

        assert column1.can_expand_to(column2) is False


class TestFabricColumnLiteral:
    """Tests for literal method."""

    def test_literal_varchar(self):
        """Verify literal wraps value in cast for varchar."""
        column = FabricColumn(
            column="test_col", dtype="varchar", char_size=100)
        result = column.literal("test_value")
        assert result == "cast('test_value' as varchar(100))"

    def test_literal_int(self):
        """Verify literal wraps value in cast for int."""
        column = FabricColumn(column="test_col", dtype="int")
        result = column.literal("42")
        assert result == "cast('42' as int)"

    def test_literal_datetime2(self):
        """Verify literal uses enforced datetime2(6) precision."""
        column = FabricColumn(column="test_col", dtype="datetime2")
        result = column.literal("2024-01-01")
        assert result == "cast('2024-01-01' as datetime2(6))"
