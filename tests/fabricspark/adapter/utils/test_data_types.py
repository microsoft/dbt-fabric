from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


class TestTypeBigIntFabricSpark(BaseTypeBigInt):
    pass


class TestTypeFloatFabricSpark(BaseTypeFloat):
    pass


class TestTypeIntFabricSpark(BaseTypeInt):
    pass


class TestTypeNumericFabricSpark(BaseTypeNumeric):
    pass


class TestTypeStringFabricSpark(BaseTypeString):
    pass


class TestTypeTimestampFabricSpark(BaseTypeTimestamp):
    pass


class TestTypeBooleanFabricSpark(BaseTypeBoolean):
    pass
