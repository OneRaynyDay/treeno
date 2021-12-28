from abc import ABC
from typing import ClassVar

import attr

from treeno.base import PrintOptions
from treeno.datatypes.builder import date, time, timestamp
from treeno.expression import value_attr
from treeno.functions.base import Function
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
