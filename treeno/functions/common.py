"""This module contains functions that belong to multiple categories. For example,
concat can be used to concat strings, varbinary, arrays, etc.
"""
import functools
from typing import Any, ClassVar, List, Type

import attr

from treeno.base import PrintOptions
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import array, char, unknown, varbinary, varchar
from treeno.datatypes.conversions import (
    STRING_TYPES,
    common_supertype,
    promote_varchar_to_char,
)
from treeno.expression import (
    OPERATOR_PRECEDENCE,
    Value,
    value_attr,
    wrap_literal_list,
)
from treeno.functions.base import Function, GenericFunction


def _num_array_layers(dtype: type_consts.DataType) -> int:
    if dtype.type_name != type_consts.ARRAY:
        return 0
    return _num_array_layers(dtype.parameters["dtype"]) + 1


def _get_array_concat_type(
    dtypes: List[type_consts.DataType]
) -> type_consts.DataType:
    # We need to find the number of layers of arrays we have, since we can only permit N or N+1 layers and we
    # strip the topmost, e.g.:
    # SELECT TYPEOF(ARRAY[ARRAY[CHAR '3']] || ARRAY['345']);
    #          _col0
    # -----------------------
    #  array(array(char(3)))
    num_layers = [_num_array_layers(dtype) for dtype in dtypes]
    max_layers = max(num_layers)
    assert (
        max_layers - min(num_layers) <= 1
    ), "Incorrect number of array layers in Concatenate"
    stripped_dtypes = [
        dtype.parameters["dtype"] if num_layers == max_layers else dtype
        for dtype, num_layers in zip(dtypes, num_layers)
    ]
    return array(
        dtype=functools.reduce(
            lambda t1, t2: common_supertype(t1, t2), stripped_dtypes
        )
    )


def _get_string_concat_type(
    dtypes: List[type_consts.DataType]
) -> type_consts.DataType:
    char_conversion = any(
        dtype.type_name == type_consts.CHAR for dtype in dtypes
    )
    if not char_conversion:
        return varchar()
    else:
        max_chars = 0
        for dtype in dtypes:
            if dtype.type_name != type_consts.CHAR:
                dtype = promote_varchar_to_char(dtype)
            max_chars += dtype.parameters["max_chars"]
        return char(max_chars=max_chars)


@value_attr
class Concatenate(Function):
    FN_NAME: ClassVar[str] = "CONCAT"
    values: List[Value] = attr.ib(converter=wrap_literal_list)

    def __attrs_post_init__(self) -> None:
        dtypes = [value.data_type for value in self.values]
        type_names = {dtype.type_name for dtype in dtypes}
        # If there's even a single unknown we should give up here.
        if type_consts.UNKNOWN in type_names:
            self.data_type = unknown()
        elif type_consts.ARRAY in type_names:
            self.data_type = _get_array_concat_type(dtypes)
        elif type_names.issubset(STRING_TYPES):
            self.data_type = _get_string_concat_type(dtypes)
        elif type_names == {type_consts.VARBINARY}:
            self.data_type = varbinary()

    @classmethod
    def from_args(
        cls: Type[GenericFunction], *args: Any, **kwargs: Any
    ) -> GenericFunction:
        assert not kwargs, "Keyword arguments not allowed for CONCAT"
        return cls(values=args)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(self.values, opts)


# TODO: See expressions.py
OPERATOR_PRECEDENCE[Concatenate] = 5
