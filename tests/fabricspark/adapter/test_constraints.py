from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsColumnsEqual,
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
)


class TestViewConstraintsColumnsEqualFabricSpark(BaseConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqualFabricSpark(BaseConstraintsColumnsEqual):
    pass


class TestTableConstraintsColumnsEqualFabricSpark(BaseConstraintsColumnsEqual):
    pass


class TestTableConstraintsRuntimeDdlEnforcementFabricSpark(BaseConstraintsRuntimeDdlEnforcement):
    pass


class TestIncrementalConstraintsRuntimeDdlEnforcementFabricSpark(
    BaseConstraintsRuntimeDdlEnforcement
):
    pass


class TestModelConstraintsRuntimeEnforcementFabricSpark(BaseModelConstraintsRuntimeEnforcement):
    pass


class TestTableConstraintsRollbackFabricSpark(BaseConstraintsRollback):
    pass


class TestIncrementalConstraintsRollbackFabricSpark(BaseConstraintsRollback):
    pass
