from dbt.tests.adapter.utils.base_array_utils import BaseArrayUtils
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast import BaseCast
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import (
    BaseCurrentTimestamp,
    BaseCurrentTimestampAware,
    BaseCurrentTimestampNaive,
)
from dbt.tests.adapter.utils.test_date import BaseDate
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_source_freshness_custom_info import BaseCalculateFreshnessMethod
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod


class TestArrayUtilsFabric(BaseArrayUtils):
    pass


class TestUtilsFabric(BaseUtils):
    pass


class TestAnyValueFabric(BaseAnyValue):
    pass


class TestArrayAppendFabric(BaseArrayAppend):
    pass


class TestArrayConcatFabric(BaseArrayConcat):
    pass


class TestArrayConstructFabric(BaseArrayConstruct):
    pass


class TestBoolOrFabric(BaseBoolOr):
    pass


class TestCastFabric(BaseCast):
    pass


class TestCastBoolToTextFabric(BaseCastBoolToText):
    pass


class TestConcatFabric(BaseConcat):
    pass


class TestCurrentTimestampFabric(BaseCurrentTimestamp):
    pass


class TestCurrentTimestampAwareFabric(BaseCurrentTimestampAware):
    pass


class TestCurrentTimestampNaiveFabric(BaseCurrentTimestampNaive):
    pass


class TestDateFabric(BaseDate):
    pass


class TestDateSpineFabric(BaseDateSpine):
    pass


class TestDateTruncFabric(BaseDateTrunc):
    pass


class TestDateAddFabric(BaseDateAdd):
    pass


class TestDateDiffFabric(BaseDateDiff):
    pass


class TestEscapeSingleQuotesBackslashFabric(BaseEscapeSingleQuotesBackslash):
    pass


class TestEscapeSingleQuotesQuoteFabric(BaseEscapeSingleQuotesQuote):
    pass


class TestExceptFabric(BaseExcept):
    pass


class TestGenerateSeriesFabric(BaseGenerateSeries):
    pass


class TestGetIntervalsBetweenFabric(BaseGetIntervalsBetween):
    pass


class TestGetPowersOfTwoFabric(BaseGetPowersOfTwo):
    pass


class TestHashFabric(BaseHash):
    pass


class TestIntersectFabric(BaseIntersect):
    pass


class TestLastDayFabric(BaseLastDay):
    pass


class TestLengthFabric(BaseLength):
    pass


class TestListaggFabric(BaseListagg):
    pass


class TestPositionFabric(BasePosition):
    pass


class TestReplaceFabric(BaseReplace):
    pass


class TestRightFabric(BaseRight):
    pass


class TestSafeCastFabric(BaseSafeCast):
    pass


class TestCalculateFreshnessMethodFabric(BaseCalculateFreshnessMethod):
    pass


class TestSplitPartFabric(BaseSplitPart):
    pass


class TestStringLiteralFabric(BaseStringLiteral):
    pass


class TestValidateSqlMethodFabric(BaseValidateSqlMethod):
    pass
