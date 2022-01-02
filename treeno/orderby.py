from enum import auto

import attr

from treeno.base import DefaultableEnum, GenericEnum, PrintOptions, Sql
from treeno.expression import Value


class OrderType(DefaultableEnum):
    ASC = auto()
    DESC = auto()

    @classmethod
    def default(cls: GenericEnum) -> GenericEnum:
        return cls.ASC


class NullOrder(DefaultableEnum):
    FIRST = auto()
    LAST = auto()

    @classmethod
    def default(cls: GenericEnum) -> GenericEnum:
        return cls.LAST


@attr.s
class OrderTerm(Sql):
    value: Value = attr.ib()
    order_type: OrderType = attr.ib(factory=OrderType.default)
    null_order: NullOrder = attr.ib(factory=NullOrder.default)

    def sql(self, opts: PrintOptions):
        order_string = self.value.sql(opts)
        if self.order_type != OrderType.default():
            order_string += f" {self.order_type.name}"
        if self.null_order != NullOrder.default():
            order_string += f" NULLS {self.null_order.name}"
        return order_string
