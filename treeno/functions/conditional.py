import functools
from typing import Any, ClassVar, List, Optional, Type

import attr

from treeno.base import PrintOptions
from treeno.datatypes.builder import unknown
from treeno.datatypes.conversions import common_supertype
from treeno.expression import Value, value_attr, wrap_literal, wrap_literal_list
from treeno.functions.base import Function, GenericFunction


@value_attr
class If(Function):
    FN_NAME: ClassVar[str] = "IF"
    condition: Value = attr.ib(converter=wrap_literal)
    true_value: Value = attr.ib(converter=wrap_literal)
    false_value: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        if self.false_value is not None:
            self.data_type = common_supertype(
                self.true_value.data_type, self.false_value.data_type
            )
        else:
            self.data_type = self.true_value.data_type

    def sql(self, opts: PrintOptions) -> str:
        args = [self.condition, self.true_value]
        if self.false_value is not None:
            args.append(self.false_value)
        return self.to_string(args, opts)


@value_attr
class Coalesce(Function):
    FN_NAME: ClassVar[str] = "COALESCE"
    values: List[Value] = attr.ib(converter=wrap_literal_list)

    def __attrs_post_init__(self) -> None:
        dtypes = [
            val.data_type for val in self.values if val.data_type != unknown()
        ]
        self.data_type = functools.reduce(
            lambda t1, t2: common_supertype(t1, t2), dtypes
        )

    @classmethod
    def from_args(
        cls: Type[GenericFunction], *args: Any, **kwargs: Any
    ) -> GenericFunction:
        assert not kwargs, "Keyword arguments not allowed for COALESCE"
        return cls(values=args)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(self.values, opts)


@value_attr
class NullIf(Function):
    FN_NAME: ClassVar[str] = "NULLIF"
    value1: Value = attr.ib(converter=wrap_literal)
    value2: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value1.data_type

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value1, self.value2], opts)


@value_attr
class Try(Function):
    FN_NAME: ClassVar[str] = "TRY"
    expr: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = self.expr.data_type

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.expr], opts)
