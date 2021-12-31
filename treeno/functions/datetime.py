from abc import ABC
from typing import Any, ClassVar, Optional

import attr

from treeno.base import PrintOptions
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import (
    bigint,
    date,
    double,
    interval,
    time,
    timestamp,
    varchar,
)
from treeno.expression import Value, value_attr, wrap_literal
from treeno.functions.base import FUNCTIONS_TO_NAMES, Function, UnaryFunction
from treeno.util import parenthesize

# I tried even setting hive.timestamp_precision=NANOSECONDS in session properties
# and CURRENT_TIMESTAMP still gives the default precision
DEFAULT_DATETIME_PRECISION: int = 3


@value_attr
class CurrentDate(Function):
    FN_NAME: ClassVar[str] = "CURRENT_DATE"

    def __attrs_post_init__(self) -> None:
        self.data_type = date()

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class ParametrizedDateTimeFunction(Function, ABC):
    """This class doesn't inherit from Function because it doesn't have to have parentheses when there's no arguments.
    """

    precision: int = attr.ib(default=DEFAULT_DATETIME_PRECISION)

    def sql(self, opts: PrintOptions) -> str:
        precision_str = ""
        if self.precision != DEFAULT_DATETIME_PRECISION:
            precision_str = parenthesize(str(self.precision))
        return f"{FUNCTIONS_TO_NAMES[type(self)]}{precision_str}"


@value_attr
class CurrentTime(ParametrizedDateTimeFunction):
    FN_NAME: ClassVar[str] = "CURRENT_TIME"

    def __attrs_post_init__(self) -> None:
        self.data_type = time(precision=self.precision)


@value_attr
class CurrentTimestamp(ParametrizedDateTimeFunction):
    FN_NAME: ClassVar[str] = "CURRENT_TIMESTAMP"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=self.precision)


@value_attr
class LocalTime(ParametrizedDateTimeFunction):
    FN_NAME: ClassVar[str] = "LOCALTIME"

    def __attrs_post_init__(self) -> None:
        self.data_type = time(precision=self.precision)


@value_attr
class LocalTimestamp(ParametrizedDateTimeFunction):
    FN_NAME: ClassVar[str] = "LOCALTIMESTAMP"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=self.precision)


@value_attr
class Date(UnaryFunction):
    FN_NAME: ClassVar[str] = "DATE"

    def __attrs_post_init__(self) -> None:
        self.data_type = date()


@value_attr
class LastDayOfMonth(UnaryFunction):
    FN_NAME: ClassVar[str] = "LAST_DAY_OF_MONTH"

    def __attrs_post_init__(self) -> None:
        self.data_type = date()


@value_attr
class FromISO8601Timestamp(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_ISO8601_TIMESTAMP"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=3, timezone=True)


@value_attr
class FromISO8601TimestampNanos(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_ISO8601_TIMESTAMP_NANOS"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=9, timezone=True)


@value_attr
class FromISO8601Date(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_ISO8601_DATE"

    def __attrs_post_init__(self) -> None:
        self.data_type = date()


@value_attr
class AtTimezone(Function):
    FN_NAME: ClassVar[str] = "AT_TIMEZONE"
    value: Value = attr.ib(converter=wrap_literal)
    zone: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        # If it's something else like unknown, then we just keep the data type unknown
        dtype = self.value.data_type
        if dtype.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=dtype.parameters["precision"], timezone=True
            )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value, self.zone], opts)


@value_attr
class WithTimezone(Function):
    FN_NAME: ClassVar[str] = "WITH_TIMEZONE"
    value: Value = attr.ib(converter=wrap_literal)
    zone: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        dtype = self.value.data_type
        if dtype.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=dtype.parameters["precision"], timezone=True
            )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value, self.zone], opts)


@value_attr
class FromUnixtime(Function):
    FN_NAME: ClassVar[str] = "FROM_UNIXTIME"
    value: Value = attr.ib(converter=wrap_literal)
    zone: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )
    hours: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )
    minutes: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    @classmethod
    def from_args(cls, *args: Any, **kwargs: Any) -> "FromUnixtime":
        assert (
            not kwargs
        ), "No keyword arguments allowed for FromUnixTime.from_args"
        if len(args) == 2:
            return FromUnixtime(args[0], zone=args[1])
        if len(args) == 3:
            return FromUnixtime(args[0], hours=args[1], minutes=args[2])
        raise ValueError(f"Unrecognized arguments {args} and {kwargs}")

    def __attrs_post_init__(self) -> None:
        dtype = self.value.data_type
        if dtype.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=dtype.parameters["precision"], timezone=True
            )

        if self.zone:
            assert (
                not self.hours and not self.minutes
            ), "If zone is specified, can't specify hours or minutes"
        if self.hours or self.minutes:
            assert (
                self.hours and self.minutes and not self.zone
            ), "Hours and minutes must be both specified without zone"

    def sql(self, opts: PrintOptions) -> str:
        values = [self.value]
        if self.zone:
            values.append(self.zone)
        if self.hours:
            assert self.minutes is not None
            values += [self.hours, self.minutes]
        return self.to_string(values, opts)


@value_attr
class FromUnixtimeNanos(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_UNIXTIME_NANOS"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=9, timezone=True)


@value_attr
class Now(Function):
    FN_NAME: ClassVar[str] = "NOW"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=3, timezone=True)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([], opts)


@value_attr
class ToISO8601(UnaryFunction):
    FN_NAME: ClassVar[str] = "TO_ISO8601"

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()


@value_attr
class ToMilliseconds(UnaryFunction):
    FN_NAME: ClassVar[str] = "TO_MILLISECONDS"

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()


@value_attr
class ToUnixtime(UnaryFunction):
    FN_NAME: ClassVar[str] = "TO_UNIXTIME"

    def __attrs_post_init__(self) -> None:
        self.data_type = double()


@value_attr
class DateTrunc(Function):
    FN_NAME: ClassVar[str] = "DATE_TRUNC"
    unit: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.unit, self.value], opts)


@value_attr
class DateAdd(Function):
    FN_NAME: ClassVar[str] = "DATE_ADD"
    unit: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)
    timestamp: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = self.timestamp.data_type

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.unit, self.value, self.timestamp], opts)


@value_attr
class DateDiff(Function):
    FN_NAME: ClassVar[str] = "DATE_DIFF"
    unit: Value = attr.ib(converter=wrap_literal)
    timestamp1: Value = attr.ib(converter=wrap_literal)
    timestamp2: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(
            [self.unit, self.timestamp1, self.timestamp2], opts
        )


@value_attr
class ParseDuration(UnaryFunction):
    FN_NAME: ClassVar[str] = "PARSE_DURATION"

    def __attrs_post_init__(self) -> None:
        self.data_type = interval(from_interval="DAY", to_interval="SECOND")


@value_attr
class HumanReadableSeconds(UnaryFunction):
    FN_NAME: ClassVar[str] = "HUMAN_READABLE_SECONDS"

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()


# TODO: This isn't the exhaustive list of datetime functions yet.
