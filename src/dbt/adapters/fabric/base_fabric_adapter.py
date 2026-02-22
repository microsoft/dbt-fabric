import abc
from typing import Any, Type

from dbt.adapters.base.impl import PythonJobHelper
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.fabric.fabric_livy_helper import FabricLivyHelper
from dbt.adapters.sql.impl import SQLAdapter


class BaseFabricAdapter(SQLAdapter, metaclass=abc.ABCMeta):
    @property
    def default_python_submission_method(self) -> str:
        return "livy"

    @property
    def python_submission_helpers(self) -> dict[str, Type[PythonJobHelper]]:
        return {
            "livy": FabricLivyHelper,
        }

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        if not submission_result or not submission_result.success:
            return AdapterResponse(_message="ERROR")
        return AdapterResponse(_message="OK")
