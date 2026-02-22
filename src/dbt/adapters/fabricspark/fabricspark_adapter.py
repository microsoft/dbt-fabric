from typing import TypeAlias

from dbt.adapters.fabric.base_fabric_adapter import BaseFabricAdapter
from dbt.adapters.fabricspark.fabricspark_connection_manager import FabricSparkConnectionManager
from dbt.adapters.fabricspark.fabricspark_relation import FabricSparkRelation
from dbt.adapters.spark.impl import SparkAdapter


class FabricSparkAdapter(BaseFabricAdapter, SparkAdapter):
    ConnectionManager = FabricSparkConnectionManager
    connections: FabricSparkConnectionManager
    Relation: TypeAlias = FabricSparkRelation
