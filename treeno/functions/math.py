from typing import ClassVar

import attr

from treeno.base import PrintOptions
from treeno.datatypes.builder import double
from treeno.expression import Value, value_attr, wrap_literal
from treeno.functions.base import Function


@value_attr
class Power(Function):
    FN_NAME: ClassVar[str] = "POWER"
    x: Value = attr.ib(converter=wrap_literal)
    p: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = double()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.x, self.p], opts)
