import attr
from treeno.expression import Value
from treeno.base import Sql, PrintOptions
from enum import Enum, auto


class OrderType(Enum):
    ASC = auto()
    DESC = auto()


class NullOrder(Enum):
    FIRST = auto()
    LAST = auto()


@attr.s
class OrderTerm(Sql):
    value: Value = attr.ib()
    order_type: OrderType = attr.ib(default=OrderType.ASC)
    null_order: NullOrder = attr.ib(default=NullOrder.LAST)

    def sql(self, opts: PrintOptions):
        order_string = self.value.sql(opts)
        if self.order_type != OrderType.ASC:
            order_string += f" {self.order_type.name}"
        if self.null_order != NullOrder.LAST:
            order_string += f" NULLS {self.null_order.name}"
        return order_string
