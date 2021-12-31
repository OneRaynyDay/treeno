from abc import ABC
from typing import Any, ClassVar, Optional

import attr

from treeno.base import PrintOptions
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import date, time, timestamp
from treeno.expression import GenericValue, value_attr, wrap_literal
from treeno.functions.base import Function, UnaryFunction
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
        return f"{self.FN_NAME}{precision_str}"


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
        self.data_type = timestamp(precision=3, timezoned=True)


@value_attr
class FromISO8601TimestampNanos(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_ISO8601_TIMESTAMP_NANOS"

    def __attrs_post_init__(self) -> None:
        self.data_type = timestamp(precision=9, timezoned=True)


@value_attr
class FromISO8601Date(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_ISO8601_DATE"

    def __attrs_post_init__(self) -> None:
        self.data_type = date()


@value_attr
class AtTimezone(Function):
    FN_NAME: ClassVar[str] = "AT_TIMEZONE"
    value: GenericValue = attr.ib(converter=wrap_literal)
    zone: GenericValue = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        # If it's something else like unknown, then we just keep the data type unknown
        dtype = self.value.data_type
        if dtype.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=dtype.parameters["precision"], timezoned=True
            )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value, self.zone], opts)


@value_attr
class WithTimezone(Function):
    FN_NAME: ClassVar[str] = "WITH_TIMEZONE"
    value: GenericValue = attr.ib(converter=wrap_literal)
    zone: GenericValue = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        dtype = self.value.data_type
        if dtype.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=dtype.parameters["precision"], timezoned=True
            )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value, self.zone], opts)


@value_attr
class FromUnixtime(Function):
    FN_NAME: ClassVar[str] = "FROM_UNIXTIME"
    value: GenericValue = attr.ib(converter=wrap_literal)
    zone: Optional[GenericValue] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )
    hours: Optional[GenericValue] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )
    minutes: Optional[GenericValue] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    @classmethod
    def from_args(cls, *args: Any, **kwargs: Any) -> "FromUnixtime":
        assert (
            not kwargs
        ), "No keyword arguments allowed for FromUnixTime.from_args"
        if len(args) == 2:
            return FromUnixtime(value=args[0], zone=args[1])
        if len(args) == 3:
            return FromUnixtime(value=args[0], hours=args[1], minutes=args[2])
        raise ValueError(f"Unrecognized arguments {args} and {kwargs}")

    def __attrs_post_init__(self) -> None:
        dtype = self.value.data_type
        if dtype.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=dtype.parameters["precision"], timezoned=True
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
            values += [self.hours, self.minutes]
        return self.to_string(values, opts)


@value_attr
class FromUnixtimeNanos(UnaryFunction):
    FN_NAME: ClassVar[str] = "FROM_UNIXTIME_NANOS"
    value: GenericValue = attr.ib(converter=wrap_literal)
