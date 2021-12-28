from abc import ABC
from typing import ClassVar

import attr

from treeno.base import PrintOptions
from treeno.expression import value_attr
from treeno.functions.base import Function
from treeno.util import parenthesize

# I tried even setting hive.timestamp_precision=NANOSECONDS in session properties
# and CURRENT_TIMESTAMP still gives the default precision
DEFAULT_DATETIME_PRECISION: int = 3


@value_attr
class CurrentDate(Function):
    FN_NAME: ClassVar[str] = "CURRENT_DATE"

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class ParametrizedTimestampFunction(Function, ABC):
    """This class doesn't inherit from Function because it doesn't have to have parentheses when there's no arguments.
    """

    precision: int = attr.ib(default=DEFAULT_DATETIME_PRECISION)

    def __attrs_post_init__(self) -> None:
        assert 0 <= self.precision <= 12, f"Invalid precision {self.precision}"

    def sql(self, opts: PrintOptions) -> str:
        precision_str = ""
        if self.precision != DEFAULT_DATETIME_PRECISION:
            precision_str = parenthesize(str(self.precision))
        return f"{self.FN_NAME}{precision_str}"


@value_attr
class CurrentTimestamp(ParametrizedTimestampFunction):
    FN_NAME: ClassVar[str] = "CURRENT_TIMESTAMP"


@value_attr
class CurrentTime(ParametrizedTimestampFunction):
    FN_NAME: ClassVar[str] = "CURRENT_TIME"


@value_attr
class LocalTime(ParametrizedTimestampFunction):
    FN_NAME: ClassVar[str] = "LOCALTIME"


@value_attr
class LocalTimestamp(ParametrizedTimestampFunction):
    FN_NAME: ClassVar[str] = "LOCALTIMESTAMP"
