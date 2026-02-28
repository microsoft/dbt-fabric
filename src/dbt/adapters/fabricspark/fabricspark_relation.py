from dataclasses import dataclass, field
from typing import Type

from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.contracts.relation import Policy
from dbt.adapters.spark.relation import SparkIncludePolicy, SparkQuotePolicy
from dbt.adapters.utils import classproperty


class FabricSparkRelationType(StrEnum):
    Table = "table"
    CTE = "cte"
    MaterializedView = "materialized_view"
    Ephemeral = "ephemeral"
    # this is a "catch all" that is better than `None` == external to anything dbt is aware of
    External = "external"
    PointerTable = "pointer_table"
    Function = "function"


@dataclass(frozen=True, eq=False, repr=False)
class FabricSparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = "`"
    require_alias: bool = False
    information: str | None = None
    type: FabricSparkRelationType | None = None  # type: ignore

    @classproperty
    def get_relation_type(cls) -> Type[FabricSparkRelationType]:
        return FabricSparkRelationType

    def information_schema(self, view_name=None) -> InformationSchema:
        # some of our data comes from jinja, where things can be `Undefined`.
        if not isinstance(view_name, str):
            view_name = None

        return InformationSchema.from_relation(self, view_name)
