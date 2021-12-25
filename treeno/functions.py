import inspect
from abc import ABC
from typing import ClassVar, Dict, List, Optional, Type, TypeVar

import attr

from treeno.base import PrintMode, PrintOptions
from treeno.expression import (
    Expression,
    GenericValue,
    Value,
    value_attr,
    wrap_literal,
)
from treeno.orderby import OrderTerm
from treeno.printer import StatementPrinter, join_stmts, pad
from treeno.util import parenthesize
from treeno.window import Window

GenericFunction = TypeVar("GenericFunction", bound="Function")

NAMES_TO_FUNCTIONS: Dict[str, Type["Function"]] = {}
FUNCTIONS_TO_NAMES: Dict[Type["Function"], str] = {}


@value_attr
class Function(Expression, ABC):
    """Functions are expressions that require parenthesizing and support aggregation, filter, sortItem,
    pattern recognition, etc.

    Functions are different from general expressions such as Cast, TypeConstructor, Like, etc.
    """

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)

        # TODO: We ignore abstract classes, but some already have all of their
        # abstract methods defined. Thus we do the extra check of ABC as a direct base.
        # See: https://stackoverflow.com/questions/62352982/python-determine-if-class-is-abstract-abc-without-abstractmethod
        if inspect.isabstract(cls) or ABC in cls.__bases__:
            return

        if not hasattr(cls, "FN_NAME"):
            raise TypeError(
                f"Every Function that's not an ABC must have the field FN_NAME defined. {cls.__name__} currently violates this constraint."
            )
        fn_name = getattr(cls, "FN_NAME")
        assert isinstance(fn_name, str), "FN_NAME must be a string"
        NAMES_TO_FUNCTIONS[fn_name] = cls
        FUNCTIONS_TO_NAMES[cls] = fn_name


@value_attr
class UnaryFunction(Function, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: PrintOptions) -> str:
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"


@value_attr
class AggregateFunction(Function, ABC):
    """Aggregate functions are functions that return a single aggregate value per group.
    They have special properties such as the ability to scan over windows using the OVER clause. For example:

    SELECT MAX(a) OVER (PARTITION BY date ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    """

    orderby: Optional[List[OrderTerm]] = attr.ib(default=None, kw_only=True)
    filter_: Optional[GenericValue] = attr.ib(default=None, kw_only=True)
    window: Optional[Window] = attr.ib(default=None, kw_only=True)

    def get_constraint_string(self, opts: PrintOptions) -> str:
        constraint_builder = StatementPrinter()
        if self.filter_:
            constraint_builder.add_entry(
                "FILTER", f"(WHERE {self.filter_.sql(opts)})"
            )
        if self.window:
            constraint_builder.add_entry(
                "OVER", parenthesize(self.window.sql(opts))
            )
        return constraint_builder.to_string(opts)

    def to_string(self, values: List[Value], opts: PrintOptions) -> str:
        arg_string = join_stmts([value.sql(opts) for value in values], opts)
        # TODO: We currently pretty print orderby and constraint stringss on the same indentation level.
        # Although sqlstyle.guide doesn't specify what happens when a line gets too long, I think an extra
        # indentation level would be good for visibility.
        if self.orderby:
            orderby_string = "ORDER BY " + join_stmts(
                [order.sql(opts) for order in self.orderby], opts
            )
            arg_string = join_stmts(
                [arg_string, orderby_string], opts, delimiter=" "
            )

        call_str = f"{FUNCTIONS_TO_NAMES[type(self)]}({arg_string})"
        constraint_string = self.get_constraint_string(opts)
        if constraint_string:
            spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
            constraint_string = pad(spacing + constraint_string, 4)
            return call_str + constraint_string
        return call_str


@value_attr
class UnaryAggregateFunction(AggregateFunction, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: PrintOptions) -> str:
        return self.to_string([self.value], opts)


class Sum(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "SUM"


class Arbitrary(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "ARBITRARY"


class ArrayAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "ARRAY_AGG"


class Avg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "AVG"


class BoolAnd(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_AND"


class BoolOr(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_OR"


class Checksum(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "CHECKSUM"


class Count(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT"


class CountIf(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT_IF"


class Every(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "EVERY"


class GeometricMean(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "GEOMETRIC_MEAN"


class ListAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "LISTAGG"
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: Optional[PrintOptions] = None) -> str:
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"
