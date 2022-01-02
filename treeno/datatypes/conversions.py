"""Coercions between data types, which is important for inferring the ouput type in a typical binary operation like
Add, Subtract, etc in treeno.expressions

The way Trino does it is by finding the correct argument template for a given function and then attempting to coerce
all arguments to the template using TypeCoercion.java. We decide to do it more flexibly during
__attrs_post_init__ to determine the output type, and we perform verifications separately. The conversions here are
not for coercing from a to b, but rather finding the common supertime for a and b.

Note that this module is very much in flux, as I operate under an assumption:
The supertype of the input types under typical binary operators will be the output type (with exceptions, such as
timestamp - timestamp = interval)
"""
from treeno.datatypes.builder import (
    array,
    char,
    decimal,
    hll,
    map_,
    row,
    time,
    timestamp,
    unknown,
    varchar,
)
from treeno.datatypes.types import (
    ARRAY,
    BIGINT,
    CHAR,
    DATE,
    DECIMAL,
    DOUBLE,
    HLL,
    INTEGER,
    INTERVAL,
    MAP,
    P4HLL,
    REAL,
    ROW,
    SMALLINT,
    TIME,
    TIMESTAMP,
    TINYINT,
    VARCHAR,
    DataType,
)

INTEGRAL_TYPES = {INTEGER, BIGINT, SMALLINT, TINYINT}
INTEGRAL_DECIMAL_PRECISION = {TINYINT: 3, SMALLINT: 5, INTEGER: 10, BIGINT: 19}
FLOAT_TYPES = {REAL, DOUBLE}
# Decimal can be either a float or an integral, and we treat it fairly differently so it's a special case
NUMERIC_TYPES = INTEGRAL_TYPES | FLOAT_TYPES | {DECIMAL}

STRING_TYPES = {CHAR, VARCHAR}
# Refer to: https://github.com/trinodb/trino/blob/f382a176760975fec51e89f3dc62060e20420f82/core/trino-spi/src/main/java/io/trino/spi/type/CharType.java#L48
MAX_CHAR_LENGTH = 65536

HLL_TYPES = {HLL, P4HLL}

DATETIME_TYPES = {DATE, TIME, TIMESTAMP}


def get_arithmetic_type(dtype1: DataType, dtype2: DataType) -> DataType:
    if dtype1 == dtype2:
        return dtype1
    name1, name2 = dtype1.type_name, dtype2.type_name
    if name1 == name2:
        return common_same_type(dtype1, dtype2)
    elif {name1, name2}.issubset(NUMERIC_TYPES):
        return common_numeric(dtype1, dtype2)
    return unknown()


def common_supertype(dtype1: DataType, dtype2: DataType) -> DataType:
    """Returns a common supertype for the given dtypes.
    """
    if dtype1.type_name == dtype2.type_name:
        if dtype1 == dtype2:
            return dtype1
        if dtype1.type_name in COMMON_CONVERSION_MAP:
            common_same_type(dtype1, dtype2)
        return unknown()
    elif dtype1.type_name in NUMERIC_TYPES:
        return common_numeric(dtype1, dtype2)
    elif dtype1.type_name in STRING_TYPES:
        return common_string(dtype1, dtype2)
    elif dtype1.type_name in HLL_TYPES:
        return common_hll(dtype1, dtype2)
    elif dtype1.type_name in DATETIME_TYPES:
        return common_datetime(dtype1, dtype2)
    raise TypeError(f"No common supertypes between {dtype1} and {dtype2}")


def common_same_type(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name
        and dtype1.type_name in COMMON_CONVERSION_MAP
    )
    return COMMON_CONVERSION_MAP[dtype1.type_name](dtype1, dtype2)


def common_datetime(dtype1: DataType, dtype2: DataType) -> DataType:
    # NOTE: It is assumed here that these dtypes don't have the same base type
    assert (
        dtype1.type_name in DATETIME_TYPES
        and dtype2.type_name in DATETIME_TYPES
    ), f"common_numeric must be called with types in {DATETIME_TYPES}"
    # date and time are not convertible to each other
    if {dtype1.type_name, dtype2.type_name} == {DATE, TIME}:
        return unknown()
    # Currently there's either TIMESTAMP, TIME or TIMESTAMP, DATE, both of which get promoted to timestamp
    return dtype1 if dtype1.type_name == TIMESTAMP else dtype2


def common_hll(dtype1: DataType, dtype2: DataType) -> DataType:
    # NOTE: It is assumed here that these dtypes don't have the same base type
    assert (
        dtype1.type_name in HLL_TYPES and dtype2.type_name in HLL_TYPES
    ), f"common_numeric must be called with types in {HLL_TYPES}"
    # No matter what we cast to hll
    return hll()


def common_string(dtype1: DataType, dtype2: DataType) -> DataType:
    # NOTE: It is assumed here that these dtypes don't have the same base type
    assert (
        dtype1.type_name in STRING_TYPES and dtype2.type_name in STRING_TYPES
    ), f"common_numeric must be called with types in {STRING_TYPES}"
    # Refer to https://github.com/trinodb/trino/blob/5450e782ca4b764f17465c1a40b914ad6743bd6c/core/trino-main/src/main/java/io/trino/type/TypeCoercion.java#L501
    # for why CHAR can't be convertible to VARCHAR.
    has_char = CHAR in {dtype1.type_name, dtype2.type_name}
    if has_char:
        dtype1 = (
            promote_varchar_to_char(dtype1)
            if dtype1.type_name != CHAR
            else dtype1
        )
        dtype2 = (
            promote_varchar_to_char(dtype2)
            if dtype2.type_name != CHAR
            else dtype2
        )
        return COMMON_CONVERSION_MAP[CHAR](dtype1, dtype2)
    else:
        return COMMON_CONVERSION_MAP[VARCHAR](dtype1, dtype2)


def common_numeric(dtype1: DataType, dtype2: DataType) -> DataType:
    # NOTE: It is assumed here that these dtypes don't have the same base type
    assert (
        dtype1.type_name in NUMERIC_TYPES and dtype2.type_name in NUMERIC_TYPES
    ), f"common_numeric must be called with types in {NUMERIC_TYPES}"
    if dtype1.type_name in INTEGRAL_TYPES:
        if dtype2.type_name in INTEGRAL_TYPES:
            return common_integral(dtype1, dtype2)
        if dtype2.type_name in FLOAT_TYPES:
            return dtype2
        assert (
            dtype2.type_name == DECIMAL
        ), f"Expected a decimal type for {dtype2}"
        return COMMON_CONVERSION_MAP[DECIMAL](
            promote_integral_to_decimal(dtype1), dtype2
        )
    elif dtype1.type_name in FLOAT_TYPES:
        if dtype2.type_name in FLOAT_TYPES:
            return common_float(dtype1, dtype2)
        # Floats have the highest priority
        return dtype1
    else:
        assert (
            dtype1.type_name == DECIMAL
        ), f"Expected a decimal type for {dtype2}"
        if dtype2.type_name in INTEGRAL_TYPES:
            return COMMON_CONVERSION_MAP[DECIMAL](
                dtype1, promote_integral_to_decimal(dtype2)
            )
        if dtype2.type_name in FLOAT_TYPES:
            return dtype2
    raise TypeError(
        f"Unexpected type conversion {dtype1} and {dtype2} unregistered in {NUMERIC_TYPES}"
    )


def promote_varchar_to_char(varchar_dtype: DataType) -> DataType:
    chars = varchar_dtype.parameters.get("max_chars", None)
    if chars is None:
        return char()
    # VARCHAR can be way bigger than char, so we have to be careful to cap it
    return char(max_chars=min(MAX_CHAR_LENGTH, chars))


def promote_integral_to_decimal(dtype: DataType) -> DataType:
    return decimal(
        precision=INTEGRAL_DECIMAL_PRECISION[dtype.type_name], scale=0
    )


def common_integral(dtype1: DataType, dtype2: DataType) -> DataType:
    # NOTE: It is assumed here that these dtypes don't have the same base type
    assert (
        dtype1.type_name in INTEGRAL_TYPES
        and dtype2.type_name in INTEGRAL_TYPES
    ), f"common_integral must be called with types in {INTEGRAL_TYPES}"
    # Since we're gonna use the precision map, we might as well use it here for priority
    p1, p2 = (
        INTEGRAL_DECIMAL_PRECISION[dtype1.type_name],
        INTEGRAL_DECIMAL_PRECISION[dtype2.type_name],
    )
    return dtype1 if p1 > p2 else dtype2


def common_float(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name in FLOAT_TYPES and dtype2.type_name in FLOAT_TYPES
    ), f"common_float must be called with types in {FLOAT_TYPES}"
    # TODO: So far we only have DOUBLE and FLOAT, so this is a very simple branch
    return dtype1 if dtype1.type_name == DOUBLE else dtype2


def common_decimal(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == DECIMAL
    ), "common_decimal must be called with DECIMAL type only"
    s1, s2 = dtype1.parameters["scale"], dtype2.parameters["scale"]
    p1, p2 = dtype1.parameters["precision"], dtype2.parameters["precision"]
    scale = max([s1, s2])
    return decimal(precision=max([p1 - s1, p2 - s2]) + scale, scale=scale)


def common_varchar(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == VARCHAR
    ), "common_varchar must be called with VARCHAR type only"
    chars1 = dtype1.parameters.get("max_chars", None)
    chars2 = dtype2.parameters.get("max_chars", None)

    if chars1 is None or chars2 is None:
        return varchar()

    return varchar(max_chars=max(chars1, chars2))


def common_char(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == CHAR
    ), "common_char must be called with CHAR type only"
    chars1 = dtype1.parameters.get("max_chars", None)
    chars2 = dtype2.parameters.get("max_chars", None)

    if chars1 is None or chars2 is None:
        return char()

    return char(max_chars=max(chars1, chars2))


def common_row(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == ROW
    ), "common_row must be called with ROW type only"
    dtypes = []
    for t1, t2 in zip(dtype1.parameters["dtypes"], dtype2.parameters["dtypes"]):
        dtypes.append(common_supertype(t1, t2))
    return row(dtypes=dtypes)


def common_map(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == MAP
    ), "common_map must be called with MAP type only"
    f1, f2 = dtype1.parameters["from_dtype"], dtype2.parameters["from_dtype"]
    t1, t2 = dtype1.parameters["to_dtype"], dtype2.parameters["to_dtype"]
    return map_(
        from_dtype=common_supertype(f1, f2), to_dtype=common_supertype(t1, t2)
    )


def common_array(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == ARRAY
    ), "common_array must be called with ARRAY type only"
    t1, t2 = dtype1.parameters["dtype"], dtype2.parameters["dtype"]
    return array(dtype=common_supertype(t1, t2))


def common_timestamp(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == TIMESTAMP
    ), "common_timestamp must be called with TIMESTAMPtype only"
    timezoned = dtype1.parameters["timezone"] or dtype2.parameters["timezone"]
    precision = max(
        dtype1.parameters["precision"], dtype2.parameters["precision"]
    )
    return timestamp(timezoned=timezoned, precision=precision)


def common_time(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == TIME
    ), "common_time must be called with TIME type only"
    timezoned = dtype1.parameters["timezone"] or dtype2.parameters["timezone"]
    precision = max(
        dtype1.parameters["precision"], dtype2.parameters["precision"]
    )
    return time(timezoned=timezoned, precision=precision)


def common_interval(dtype1: DataType, dtype2: DataType) -> DataType:
    assert (
        dtype1.type_name == dtype2.type_name == INTERVAL
    ), "common_interval must be called with INTERVAL type only"
    if (
        dtype1.parameters["from_interval"] == dtype2.parameters["from_interval"]
        and dtype1.parameters["to_interval"] == dtype2.parameters["to_interval"]
    ):
        return dtype1
    # No common subtype in general
    return unknown()


COMMON_CONVERSION_MAP = {
    DECIMAL: common_decimal,
    VARCHAR: common_varchar,
    CHAR: common_char,
    ROW: common_row,
    ARRAY: common_array,
    MAP: common_map,
    TIME: common_time,
    TIMESTAMP: common_timestamp,
    INTERVAL: common_interval,
}
