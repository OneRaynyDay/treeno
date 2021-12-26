from abc import ABC
from enum import Enum
from typing import ClassVar, List, Optional

import attr

from treeno.base import PrintMode, PrintOptions, Sql
from treeno.expression import (
    GenericValue,
    Star,
    Value,
    value_attr,
    wrap_literal,
)
from treeno.functions.base import FUNCTIONS_TO_NAMES, Function, GenericFunction
from treeno.orderby import OrderTerm
from treeno.printer import StatementPrinter, join_stmts, pad
from treeno.util import parenthesize, quote_literal
from treeno.window import NullTreatment, Window


@value_attr
class AggregateFunction(Function, ABC):
    """Aggregate functions are functions that return a single aggregate value per group.
    They have special properties such as the ability to scan over windows using the OVER clause. For example:

    SELECT MAX(a) OVER (PARTITION BY date ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    """

    orderby: Optional[List[OrderTerm]] = attr.ib(default=None, kw_only=True)
    filter_: Optional[GenericValue] = attr.ib(default=None, kw_only=True)
    window: Optional[Window] = attr.ib(default=None, kw_only=True)
    null_treatment: NullTreatment = attr.ib(
        factory=NullTreatment.default, kw_only=True
    )

    def get_constraint_string(self, opts: PrintOptions) -> str:
        constraint_builder = StatementPrinter()
        if self.filter_:
            constraint_builder.add_entry(
                "FILTER", f"(WHERE {self.filter_.sql(opts)})"
            )
        if self.null_treatment != NullTreatment.RESPECT:
            constraint_builder.add_entry(self.null_treatment.value, "NULLS")
        if self.window:
            constraint_builder.add_entry(
                "OVER", parenthesize(self.window.sql(opts))
            )
        return constraint_builder.to_string(opts)

    def get_orderby_string(self, opts: PrintOptions) -> str:
        return "ORDER BY " + join_stmts(
            [order.sql(opts) for order in self.orderby], opts
        )

    def to_string(self, values: List[Value], opts: PrintOptions) -> str:
        arg_string = join_stmts([value.sql(opts) for value in values], opts)
        # TODO: We currently pretty print orderby and constraint stringss on the same indentation level.
        # Although sqlstyle.guide doesn't specify what happens when a line gets too long, I think an extra
        # indentation level would be good for visibility.
        if self.orderby:
            arg_string = join_stmts(
                [arg_string, self.get_orderby_string(opts)], opts, delimiter=" "
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


@value_attr
class Count(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT"

    def __attrs_post_init__(self) -> None:
        # TODO: Do we want to include AliasedStar?
        if isinstance(self.value, Star):
            assert (
                not self.orderby
                and self.null_treatment == NullTreatment.default()
            ), "COUNT(*) cannot be used with orderby and null treatment."


class CountIf(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT_IF"


class Every(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "EVERY"


class GeometricMean(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "GEOMETRIC_MEAN"


class CountIndication(Enum):
    WITH_COUNT = "WITH COUNT"
    WITHOUT_COUNT = "WITHOUT COUNT"


@attr.s
class OverflowFiller(Sql):
    DEFAULT_FILLER: ClassVar[str] = "..."
    count_indication: CountIndication = attr.ib()
    filler: str = attr.ib(default=DEFAULT_FILLER)

    def sql(self, opts: PrintOptions) -> str:
        return f"TRUNCATE {quote_literal(self.filler)} {self.count_indication.value}"


@value_attr
class ListAgg(AggregateFunction):
    DEFAULT_SEPARATOR: ClassVar[str] = " "
    FN_NAME: ClassVar[str] = "LISTAGG"
    value: GenericValue = attr.ib(converter=wrap_literal)
    separator: str = attr.ib(default=" ")
    # If overflow filler is None, then raise an error. Otherwise truncate
    # with the appropriate filler string.
    overflow_filler: Optional[OverflowFiller] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        assert (
            self.null_treatment == NullTreatment.default()
        ), "LISTAGG currently does not support null treatment"
        assert (
            not self.window
        ), "LISTAGG currently does not support window functions"
        assert not self.filter_, "LISTAGG currently does not support filter"
        assert (
            self.orderby
        ), "LISTAGG requires the WITHIN GROUP(ORDER BY ...) clause"

    def sql(self, opts: PrintOptions) -> str:
        value_string = self.value.sql(opts)
        if self.separator != self.DEFAULT_SEPARATOR:
            value_string = join_stmts(
                [value_string, quote_literal(self.separator)], opts
            )
        # If it's not None, then we could also write ON OVERFLOW ERROR, but it's default so let's omit it.
        if self.overflow_filler is not None:
            value_string = join_stmts(
                [value_string, f"ON OVERFLOW {self.overflow_filler.sql(opts)}"],
                opts,
                delimiter=" ",
            )
        constraint_string = f"WITHIN GROUP ({self.get_orderby_string(opts)})"
        spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
        constraint_string = pad(spacing + constraint_string, 4)
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({value_string}){constraint_string}"
