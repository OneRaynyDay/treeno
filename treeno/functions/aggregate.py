from abc import ABC
from enum import Enum
from typing import Any, ClassVar, List, Optional

import attr

from treeno.base import PrintMode, PrintOptions, Sql
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import (
    array,
    bigint,
    boolean,
    double,
    hll,
    map_,
    qdigest,
    tdigest,
    unknown,
    varbinary,
    varchar,
)
from treeno.expression import Star, Value, value_attr, wrap_literal
from treeno.functions.base import FUNCTIONS_TO_NAMES, Function
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
    filter_: Optional[Value] = attr.ib(default=None, kw_only=True)
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
        assert self.orderby, "Orderby must be defined"
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
            constraint_string = pad(spacing + constraint_string, opts.spaces)
            return call_str + constraint_string
        return call_str


@value_attr
class UnaryAggregateFunction(AggregateFunction, ABC):
    value: Value = attr.ib(converter=wrap_literal)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value], opts)


@value_attr
class BinaryStatsFunction(AggregateFunction, ABC):
    y: Value = attr.ib(converter=wrap_literal)
    x: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = double()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.y, self.x], opts)


@value_attr
class Sum(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "SUM"

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type


@value_attr
class Arbitrary(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "ARBITRARY"

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type


@value_attr
class ArrayAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "ARRAY_AGG"

    def __attrs_post_init__(self) -> None:
        self.data_type = array(dtype=self.value.data_type)


@value_attr
class Avg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "AVG"

    def __attrs_post_init__(self) -> None:
        self.data_type = double()


@value_attr
class BoolAnd(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_AND"

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()


@value_attr
class BoolOr(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_OR"

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()


@value_attr
class Checksum(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "CHECKSUM"

    def __attrs_post_init__(self) -> None:
        self.data_type = varbinary()


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
        self.data_type = bigint()


@value_attr
class CountIf(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT_IF"

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()


@value_attr
class Every(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "EVERY"

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()


@value_attr
class GeometricMean(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "GEOMETRIC_MEAN"

    def __attrs_post_init__(self) -> None:
        self.data_type = double()


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
    value: Value = attr.ib(converter=wrap_literal)
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
        self.data_type = varchar()

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
        constraint_string = pad(spacing + constraint_string, opts.spaces)
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({value_string}){constraint_string}"


@value_attr
class Max(AggregateFunction):
    FN_NAME: ClassVar[str] = "MAX"
    value: Value = attr.ib(converter=wrap_literal)
    num_values: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value]
        if self.num_values:
            values.append(self.num_values)
        return self.to_string(values, opts)


@value_attr
class MaxBy(AggregateFunction):
    FN_NAME: ClassVar[str] = "MAX_BY"
    value: Value = attr.ib(converter=wrap_literal)
    max_by: Value = attr.ib(converter=wrap_literal)
    num_values: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value, self.max_by]
        if self.num_values:
            values.append(self.num_values)
        return self.to_string(values, opts)


@value_attr
class Min(AggregateFunction):
    FN_NAME: ClassVar[str] = "MIN"
    value: Value = attr.ib(converter=wrap_literal)
    num_values: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value]
        if self.num_values:
            values.append(self.num_values)
        return self.to_string(values, opts)


@value_attr
class MinBy(AggregateFunction):
    FN_NAME: ClassVar[str] = "MIN_BY"
    value: Value = attr.ib(converter=wrap_literal)
    max_by: Value = attr.ib(converter=wrap_literal)
    num_values: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value, self.max_by]
        if self.num_values:
            values.append(self.num_values)
        return self.to_string(values, opts)


@value_attr
class BitwiseAndAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BITWISE_AND_AGG"

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()


@value_attr
class BitwiseOrAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BITWISE_OR_AGG"

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()


@value_attr
class Histogram(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "HISTOGRAM"

    def __attrs_post_init__(self) -> None:
        self.data_type = map_(
            from_dtype=self.value.data_type, to_dtype=bigint()
        )


@value_attr
class MapAgg(AggregateFunction):
    FN_NAME: ClassVar[str] = "MAP_AGG"
    key: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = map_(
            from_dtype=self.key.data_type, to_dtype=self.value.data_type
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.key, self.value], opts)


@value_attr
class MapUnion(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "MAP_UNION"

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type


@value_attr
class MultiMapAgg(AggregateFunction):
    FN_NAME: ClassVar[str] = "MULTIMAP_AGG"
    key: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = map_(
            from_dtype=self.key.data_type,
            to_dtype=array(dtype=self.value.data_type),
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.key, self.value], opts)


@value_attr
class ApproxDistinct(AggregateFunction):
    FN_NAME: ClassVar[str] = "APPROX_DISTINCT"
    key: Value = attr.ib(converter=wrap_literal)
    epsilon: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()

    def sql(self, opts: PrintOptions) -> str:
        values = [self.key]
        if self.epsilon:
            values.append(self.epsilon)
        return self.to_string(values, opts)


@value_attr
class ApproxMostFrequent(AggregateFunction):
    FN_NAME: ClassVar[str] = "APPROX_MOST_FREQUENT"
    buckets: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)
    capacity: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = map_(
            from_dtype=self.value.data_type, to_dtype=bigint()
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.buckets, self.value, self.capacity], opts)


@value_attr(init=False)
class ApproxPercentile(AggregateFunction):
    FN_NAME: ClassVar[str] = "APPROX_PERCENTILE"
    value: Value = attr.ib(converter=wrap_literal)
    percentage: Value = attr.ib(converter=wrap_literal)
    weight: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __init__(self, value: Value, *args: Any, **kwargs: Any):
        # This function is strange in that the overloads causes positional arguments to mean different things.
        # We have to swap weight and percentage if they're both specified.
        if len(args) == 2:
            args = (args[1], args[0])
        self.__attrs_init__(value, *args, **kwargs)  # type: ignore

        # If percentage is an array, then we can be sure the output type is array
        # otherwise if it's a scalar not-unknown type, then we know the output type is scalar.
        # If we just don't know about the underlying data type then this should return unknown.
        if self.percentage.data_type.type_name == type_consts.ARRAY:
            self.data_type = array(dtype=self.value.data_type)
        elif self.percentage.data_type != unknown():
            self.data_type = self.value.data_type
        else:
            self.data_type = unknown()

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value, self.percentage]
        if self.weight:
            values.append(self.weight)
        return self.to_string(values, opts)


@value_attr
class ApproxSet(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "APPROX_SET"

    def __attrs_post_init__(self) -> None:
        self.data_type = hll()


@value_attr
class Merge(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "MERGE"

    def __attrs_post_init__(self) -> None:
        if (
            self.value.data_type.type_name == type_consts.TDIGEST
            or self.value.data_type.type_name == type_consts.QDIGEST
        ):
            self.data_type = self.value.data_type
        else:
            self.data_type = hll()


@value_attr
class NumericHistogram(AggregateFunction):
    FN_NAME: ClassVar[str] = "NUMERIC_HISTOGRAM"
    buckets: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)
    weight: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = map_(from_dtype=double(), to_dtype=double())

    def sql(self, opts: PrintOptions) -> str:
        values = [self.buckets, self.value]
        if self.weight:
            values.append(self.weight)
        return self.to_string(values, opts)


@value_attr
class QDigestAgg(AggregateFunction):
    FN_NAME: ClassVar[str] = "QDIGEST_AGG"
    value: Value = attr.ib(converter=wrap_literal)
    weight: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )
    accuracy: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = qdigest(dtype=self.value.data_type)

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value]
        if self.weight:
            values.append(self.weight)
        if self.accuracy:
            values.append(self.accuracy)
        return self.to_string(values, opts)


@value_attr
class TDigestAgg(AggregateFunction):
    FN_NAME: ClassVar[str] = "TDIGEST_AGG"
    value: Value = attr.ib(converter=wrap_literal)
    weight: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = tdigest()

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value]
        if self.weight:
            values.append(self.weight)
        return self.to_string(values, opts)


@value_attr
class Corr(BinaryStatsFunction):
    FN_NAME: ClassVar[str] = "CORR"


@value_attr
class CovarPop(BinaryStatsFunction):
    FN_NAME: ClassVar[str] = "COVAR_POP"


@value_attr
class CovarSamp(BinaryStatsFunction):
    FN_NAME: ClassVar[str] = "COVAR_SAMP"


@value_attr
class Kurtosis(BinaryStatsFunction):
    FN_NAME: ClassVar[str] = "KURTOSIS"


@value_attr
class RegrIntercept(BinaryStatsFunction):
    FN_NAME: ClassVar[str] = "REGR_INTERCEPT"


@value_attr
class RegrSlope(BinaryStatsFunction):
    FN_NAME: ClassVar[str] = "REGR_SLOPE"


@value_attr
class Skewness(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "SKEWNESS"


@value_attr
class StdDev(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "STDDEV"


@value_attr
class StdDevPop(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "STDDEV_POP"


@value_attr
class StdDevSamp(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "STDDEV_SAMP"


@value_attr
class Variance(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "VARIANCE"


@value_attr
class VarPop(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "VAR_POP"


@value_attr
class VarSamp(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "VAR_SAMP"


# TODO: Add reduce()
