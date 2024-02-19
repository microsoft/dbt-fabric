from dbt.adapters.base import AdapterPlugin
from dbt.adapters.include import fabric

from dbt.adapters.fabric.fabric_adapter import FabricAdapter
from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.fabric_configs import FabricConfigs
from dbt.adapters.fabric.fabric_connection_manager import FabricConnectionManager
from dbt.adapters.fabric.fabric_credentials import FabricCredentials

Plugin = AdapterPlugin(
    adapter=FabricAdapter,
    credentials=FabricCredentials,
    include_path=fabric.PACKAGE_PATH,
)

__all__ = [
    "Plugin",
    "FabricConnectionManager",
    "FabricColumn",
    "FabricAdapter",
    "FabricCredentials",
    "FabricConfigs",
]
