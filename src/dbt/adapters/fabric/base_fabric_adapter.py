import abc
from typing import Type

from dbt.adapters.base.impl import PythonJobHelper
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.fabric.fabric_livy_helper import FabricLivyHelper
from dbt.adapters.fabric.fabric_livy_session import LivySubmissionResult
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

    def generate_python_submission_response(
        self, submission_result: LivySubmissionResult | None
    ) -> AdapterResponse:
        if not submission_result:
            return AdapterResponse(_message="ERROR")
        elif not submission_result.success:
            assert submission_result.error_message is not None
            return AdapterResponse(
                _message=submission_result.error_message, query_id=submission_result.run_id
            )
        return AdapterResponse(_message="OK", query_id=submission_result.run_id)
