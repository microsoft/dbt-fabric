from dataclasses import dataclass, field
from typing import Optional, Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.utils import classproperty

from dbt.adapters.fabric.relation_configs import FabricQuotePolicy, FabricRelationType


@dataclass(frozen=True, eq=False, repr=False)
class FabricRelation(BaseRelation):
    type: Optional[FabricRelationType] = None  # type: ignore
    quote_policy: FabricQuotePolicy = field(default_factory=lambda: FabricQuotePolicy())
    require_alias: bool = False

    @classproperty
    def get_relation_type(cls) -> Type[FabricRelationType]:
        return FabricRelationType

    @classmethod
    def render_limited(self) -> str:
        rendered = self.render()
        if self.limit is None:
            return rendered
        elif self.limit == 0:
            return f"(select * from {rendered} where 1=0) {self._render_limited_alias()}"
        else:
            return f"(select TOP {self.limit} * from {rendered}) {self._render_limited_alias()}"
