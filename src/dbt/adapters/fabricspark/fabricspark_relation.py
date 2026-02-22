from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import Policy
from dbt.adapters.spark.relation import SparkIncludePolicy, SparkQuotePolicy


@dataclass(frozen=True, eq=False, repr=False)
class FabricSparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = "`"
    require_alias: bool = False
