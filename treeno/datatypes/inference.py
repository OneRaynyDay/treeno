"""Module for inferring types from python inputs.

Should not be used in datatypes.types to prevent circular imports
"""
from decimal import Decimal
from typing import Any

from treeno.datatypes.builder import (
    bigint,
    boolean,
    char,
    decimal,
    double,
    integer,
    varchar,
)
from treeno.datatypes.types import DataType


def infer_type(value: Any) -> DataType:
    """Infers a trino type from the given python value.
    """
    if isinstance(value, bool):
        return boolean()
    if isinstance(value, int):
        return infer_integral(value)
    if isinstance(value, float):
        return double()
    if isinstance(value, Decimal):
        # NOTE: This is best-effort inference, since the precision of
        # a decimal is encoded in its string value
        return infer_decimal(value)
    if isinstance(value, str):
        # NOTE: We opt for varchar type with no length limit here because the max char limit can be any
        # integer greater than len(value)
        return varchar(max_chars=len(value))
    raise NotImplementedError(
        f"Value {value} with type {type(value).__name__} can't be inferred"
    )


def infer_char(value: str) -> DataType:
    return char(max_chars=len(value))


def infer_integral(value: int) -> DataType:
    return integer() if -(0x7FFFFFFF + 1) <= value <= 0x7FFFFFFF else bigint()


def infer_decimal(decimal_value: Decimal) -> DataType:
    """We must infer a decimal from str and not float because we'll never be sure what the
    actual scale is due to floats being imprecise.
    """
    str_val = str(decimal_value)
    if "." not in str_val:
        return decimal(precision=len(str_val), scale=0)
    else:
        assert str_val.count(".") == 1, f"Malformed decimal str_val {str_val}"
        decimal_point_pos = str_val.index(".")
        return decimal(
            precision=len(str_val) - 1,
            scale=len(str_val) - decimal_point_pos - 1,
        )


def infer_timelike_precision(value: str) -> int:
    # TODO: Use a datetime util for this at some point. Currently datetime in python only supports microseconds :(
    # Number of digits past the dot is its precision
    if "." not in value:
        return 0
    # In case we have timezone
    return len(value.rsplit(".")[-1].split(" ")[0])
