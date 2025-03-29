import pytest

from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeralErrorHandling,
    BaseEphemeralMulti,
    BaseEphemeralNested,
)


@pytest.mark.skip(reason="Epemeral models are not supported in Fabric DW")
class TestEphemeral(BaseEphemeralMulti):
    pass


@pytest.mark.skip(reason="Epemeral models are not supported in Fabric DW")
class TestEphemeralNested(BaseEphemeralNested):
    pass


@pytest.mark.skip(reason="Epemeral models are not supported in Fabric DW")
class TestEphemeralErrorHandling(BaseEphemeralErrorHandling):
    pass
