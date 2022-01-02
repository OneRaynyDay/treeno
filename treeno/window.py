from abc import ABC
from enum import Enum
from typing import List, Optional

import attr

from treeno.base import DefaultableEnum, GenericEnum, PrintOptions, Sql
from treeno.expression import Value, wrap_literal
from treeno.orderby import OrderTerm
from treeno.printer import StatementPrinter, join_stmts


class NullTreatment(DefaultableEnum):
    """Used in conjunction with window functions in primary expressions"""

    IGNORE = "IGNORE"
    RESPECT = "RESPECT"

    @classmethod
    def default(cls: GenericEnum) -> GenericEnum:
        return NullTreatment.RESPECT


class FrameType(DefaultableEnum):
    RANGE = "RANGE"
    ROWS = "ROWS"
    GROUPS = "GROUPS"

    @classmethod
    def default(cls: GenericEnum) -> GenericEnum:
        return FrameType.RANGE


class BoundType(Enum):
    PRECEDING = "PRECEDING"
    FOLLOWING = "FOLLOWING"


class FrameBound(Sql, ABC):
    """Represents a bound in the range of a window frame specification"""


@attr.s
class BoundedFrameBound(FrameBound):
    bound_type: BoundType = attr.ib()
    offset: Value = attr.ib(converter=wrap_literal)

    def sql(self, opts: PrintOptions) -> str:
        return f"{self.offset.sql(opts)} {self.bound_type.value}"


@attr.s
class UnboundedFrameBound(FrameBound):
    bound_type: BoundType = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        return f"UNBOUNDED {self.bound_type.value}"


@attr.s
class CurrentFrameBound(FrameBound):
    def sql(self, opts: PrintOptions) -> str:
        return "CURRENT ROW"


def default_start_bound() -> UnboundedFrameBound:
    return UnboundedFrameBound(bound_type=BoundType.PRECEDING)


def default_end_bound() -> CurrentFrameBound:
    return CurrentFrameBound()


@attr.s
class Window(Sql):
    # TODO: Evaluate whether it's worth it to embed a parent window class here.
    # The grammar makes it hard to embed a linked list data structure of window references during parse.
    parent_window: Optional[str] = attr.ib(default=None)
    orderby: Optional[List[OrderTerm]] = attr.ib(default=None)
    partitions: Optional[List[Value]] = attr.ib(default=None)
    frame_type: FrameType = attr.ib(factory=FrameType.default)
    # TODO: For now we represent missing bounds as default bounds
    start_bound: FrameBound = attr.ib(factory=default_start_bound)
    end_bound: FrameBound = attr.ib(factory=default_end_bound)

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        if self.parent_window:
            # Empty string for no value on the right side of the river
            builder.add_entry(self.parent_window, "")
        if self.partitions:
            partition_value = join_stmts(
                [partition.sql(opts) for partition in self.partitions], opts
            )
            builder.add_entry("PARTITION", "BY " + partition_value)
        if self.orderby:
            order_value = join_stmts(
                [order_term.sql(opts) for order_term in self.orderby], opts
            )
            builder.add_entry("ORDER", "BY " + order_value)
        builder.add_entry(
            self.frame_type.value,
            " ".join(
                [
                    "BETWEEN",
                    self.start_bound.sql(opts),
                    "AND",
                    self.end_bound.sql(opts),
                ]
            ),
        )
        return builder.to_string(opts)
