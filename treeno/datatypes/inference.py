"""Module for inferring types from python inputs.

Should not be used in datatypes.types to prevent circular imports
"""
from treeno.datatypes.types import DataType, INTEGER, BIGINT, DECIMAL


def infer_integral(value: int) -> DataType:
    return (
        DataType(INTEGER)
        if -(0x7FFFFFFF + 1) <= value <= 0x7FFFFFFF
        else DataType(BIGINT)
    )


def infer_decimal_from_str(value: str) -> DataType:
    """We must infer a decimal from str and not float because we'll never be sure what the
    actual scale is due to floats being imprecise.
    """
    if "." not in value:
        return DataType(
            DECIMAL, parameters={"precision": len(value), "scale": 0}
        )
    else:
        assert value.count(".") == 1, f"Malformed decimal value {value}"
        decimal_point_pos = value.index(".")
        return DataType(
            DECIMAL,
            parameters={
                "precision": decimal_point_pos,
                "scale": len(value) - decimal_point_pos - 1,
            },
        )
