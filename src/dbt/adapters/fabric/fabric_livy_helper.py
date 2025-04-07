from typing import Any, Dict

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.fabric.fabric_credentials import FabricCredentials


class FabricLivyHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credential: FabricCredentials) -> None:
        super().__init__(parsed_model, credential)

    def submit(self, compiled_code: str) -> Any:
        super().submit(compiled_code)
