from typing import Callable, Tuple, TypeAlias

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.fabric.base_fabric_adapter import BaseFabricAdapter
from dbt.adapters.fabricspark.fabricspark_connection_manager import FabricSparkConnectionManager
from dbt.adapters.fabricspark.fabricspark_relation import (
    FabricSparkRelation,
    FabricSparkRelationType,
)
from dbt.adapters.spark.impl import SparkAdapter


class FabricSparkAdapter(BaseFabricAdapter, SparkAdapter):
    ConnectionManager = FabricSparkConnectionManager  # type: ignore
    connections: FabricSparkConnectionManager  # type: ignore
    Relation: TypeAlias = FabricSparkRelation  # type: ignore
    RelationInfo = Tuple[str, str, str]

    def _namespace_to_parts(self, namespace: str) -> Tuple[str, str, str]:
        """Convert a namespace string into its components."""
        # Example namespace: `adapter-dev`.`dbtdevlh`.`test17722693981743727771_test_basic`
        parts = tuple(map(lambda x: x.strip("`"), namespace.split(".")))
        if len(parts) != 3:
            raise DbtRuntimeError(
                f"Unexpected namespace format: '{namespace}'. Expected format: 'workspace.database.schema'"
            )
        return parts

    def _build_spark_relation_list(
        self,
        row_list: "agate.Table",
        relation_info_func: Callable[["agate.Row"], RelationInfo],
    ) -> list[FabricSparkRelation]:
        relations = []
        for row in row_list:
            _namespace, name, information = relation_info_func(row)
            _workspace, _database, _schema = self._namespace_to_parts(_namespace)

            # Example information string:
            # Catalog: spark_catalog  # noqa: ERA001
            # Database: `adapter-dev`.`dbtdevlh`.`test17722693981743727771_test_basic`
            # Table: table_model  # noqa: ERA001
            # Created Time: Wed Jan 21 12:17:49 UTC 1970
            # Last Access: UNKNOWN
            # Created By: Spark
            # Type: MANAGED  # noqa: ERA001
            # Provider: delta  # noqa: ERA001
            # Comment: ...  # noqa: ERA001
            # Table Properties: [key=value, ...]
            # Location: abfss://...

            # 2 possible types: MATERIALIZED_LAKE_VIEW or MANAGED (regular table)

            rel_type: FabricSparkRelationType = (
                FabricSparkRelationType.MaterializedView
                if "Type: MATERIALIZED_LAKE_VIEW" in information
                else FabricSparkRelationType.Table
            )

            relation: FabricSparkRelation = self.Relation.create(
                database=_database,
                schema=_schema,
                identifier=name,
                type=rel_type,
                information=information,
                workspace=_workspace,
            )
            relations.append(relation)

        return relations
