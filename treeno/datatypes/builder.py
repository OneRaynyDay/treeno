"""Builders for common types as syntactic sugar.

Note that this module is mainly for client use. This module should not be used within treeno.datatypes.types to avoid
circular dependencies.

Note that we explicitly disallow arguments for types that can't be parametrized. We also strictly only allow keyword
only arguments.
"""
from treeno.datatypes import types


def boolean() -> types.DataType:
    return types.DataType(types.BOOLEAN)


def tinyint() -> types.DataType:
    return types.DataType(types.TINYINT)


def smallint() -> types.DataType:
    return types.DataType(types.SMALLINT)


def integer() -> types.DataType:
    return types.DataType(types.INTEGER)


def bigint() -> types.DataType:
    return types.DataType(types.BIGINT)


def real() -> types.DataType:
    return types.DataType(types.REAL)


def double() -> types.DataType:
    return types.DataType(types.DOUBLE)


def decimal(**kwargs) -> types.DataType:
    return types.DataType(types.DECIMAL, parameters=kwargs)


def varchar(**kwargs) -> types.DataType:
    return types.DataType(types.VARCHAR, parameters=kwargs)


def char(**kwargs) -> types.DataType:
    return types.DataType(types.CHAR, parameters=kwargs)


def varbinary() -> types.DataType:
    return types.DataType(types.VARBINARY)


def json() -> types.DataType:
    return types.DataType(types.JSON)


def date() -> types.DataType:
    return types.DataType(types.DATE)


def time(**kwargs) -> types.DataType:
    return types.DataType(types.TIME, parameters=kwargs)


def timestamp(**kwargs) -> types.DataType:
    return types.DataType(types.TIMESTAMP, parameters=kwargs)


def interval(**kwargs) -> types.DataType:
    return types.DataType(types.INTERVAL, parameters=kwargs)


def array(**kwargs) -> types.DataType:
    return types.DataType(types.ARRAY, parameters=kwargs)


def map_(**kwargs) -> types.DataType:
    return types.DataType(types.MAP, parameters=kwargs)


def row(**kwargs) -> types.DataType:
    return types.DataType(types.ROW, parameters=kwargs)


def unknown() -> types.DataType:
    return types.DataType(types.UNKNOWN)
