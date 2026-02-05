"""Unit tests for fabric_relation module."""

import pytest

from dbt.adapters.fabric.fabric_relation import FabricRelation
from dbt.adapters.fabric.relation_configs import FabricQuotePolicy, FabricRelationType


class TestFabricRelationType:
    """Tests for FabricRelationType enum."""

    def test_get_relation_type(self):
        """Verify get_relation_type returns FabricRelationType."""
        assert FabricRelation.get_relation_type is FabricRelationType


class TestFabricRelationDefaults:
    """Tests for default values."""

    def test_default_quote_policy(self):
        """Verify default quote policy is FabricQuotePolicy."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
        )
        assert isinstance(relation.quote_policy, FabricQuotePolicy)

    def test_default_require_alias(self):
        """Verify default require_alias is False."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
        )
        assert relation.require_alias is False


class TestFabricRelationRenderLimited:
    """Tests for render_limited method."""

    def test_render_limited_no_limit(self):
        """Verify render_limited with no limit returns rendered relation."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
        )
        # limit is None by default
        result = relation.render_limited()

        # Should just render the relation normally
        assert "test_table" in result

    def test_render_limited_with_zero_limit(self):
        """Verify render_limited with limit=0 returns WHERE 1=0."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
            limit=0,
        )
        result = relation.render_limited()

        assert "where 1=0" in result.lower()
        assert "_dbt_limit_subq" in result

    def test_render_limited_with_positive_limit(self):
        """Verify render_limited with positive limit uses TOP."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
            limit=10,
        )
        result = relation.render_limited()

        assert "TOP 10" in result
        assert "_dbt_limit_subq" in result

    def test_render_limited_with_large_limit(self):
        """Verify render_limited with large limit uses TOP."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
            limit=1000,
        )
        result = relation.render_limited()

        assert "TOP 1000" in result


class TestFabricRelationRenderLimitedAlias:
    """Tests for _render_limited_alias method."""

    def test_render_limited_alias_returns_standard_alias(self):
        """Verify _render_limited_alias returns expected alias."""
        relation = FabricRelation.create(
            database="testdb",
            schema="dbo",
            identifier="test_table",
        )
        result = relation._render_limited_alias()

        assert result == "_dbt_limit_subq"


class TestFabricRelationCreate:
    """Tests for create factory method."""

    def test_create_with_all_parts(self):
        """Verify create works with database, schema, and identifier."""
        relation = FabricRelation.create(
            database="mydb",
            schema="myschema",
            identifier="mytable",
        )

        assert relation.database == "mydb"
        assert relation.schema == "myschema"
        assert relation.identifier == "mytable"

    def test_create_with_type(self):
        """Verify create works with relation type."""
        relation = FabricRelation.create(
            database="mydb",
            schema="myschema",
            identifier="mytable",
            type=FabricRelationType.Table,
        )

        assert relation.type == FabricRelationType.Table

    def test_create_view(self):
        """Verify create works for views."""
        relation = FabricRelation.create(
            database="mydb",
            schema="myschema",
            identifier="myview",
            type=FabricRelationType.View,
        )

        assert relation.type == FabricRelationType.View


class TestFabricRelationRender:
    """Tests for render method (inherited but important to verify)."""

    def test_render_full_path(self):
        """Verify render produces full path."""
        relation = FabricRelation.create(
            database="mydb",
            schema="myschema",
            identifier="mytable",
        )
        result = relation.render()

        # Should contain all parts (quoting may vary)
        assert "mydb" in result
        assert "myschema" in result
        assert "mytable" in result
