from dataclasses import dataclass

from dbt.adapters.protocol import AdapterConfig


@dataclass
class FabricConfigs(AdapterConfig):
    auto_provision_aad_principals: bool | None = False
