from dataclasses import dataclass
from typing import List, Optional

from dbt.adapters.protocol import AdapterConfig


@dataclass
class FabricConfigs(AdapterConfig):
    auto_provision_aad_principals: Optional[bool] = False
    cluster_by: Optional[List[str]] = None
