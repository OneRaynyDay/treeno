import attr
from abc import ABC
from treeno.expression import Value, GenericValue, wrap_literal
from treeno.base import Sql, PrintOptions
from typing import Optional, List
from treeno.orderby import OrderTerm
from enum import Enum


class FrameType(Enum):
    RANGE = "RANGE"
    ROWS = "ROWS"
    GROUPS = "GROUPS"


class BoundType(Enum):
    PRECEDING = "PRECEDING"
    FOLLOWING = "FOLLOWING"


class FrameBound(Sql, ABC):
    """Represents a bound in the range of a window frame specification"""


@attr.s
class BoundedFrameBound(FrameBound):
    bound_type: BoundType = attr.ib()
    offset: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        return f"{self.offset.sql(opts)} {self.bound_type.value}"


@attr.s
class UnboundedFrameBound(FrameBound):
    bound_type: BoundType = attr.ib()

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        return f"UNBOUNDED {self.bound_type.value}"


@attr.s
class CurrentFrameBound(FrameBound):
    def sql(self, opts: Optional[PrintOptions] = None) -> str:
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
    frame_type: FrameType = attr.ib(default=FrameType.RANGE)
    # TODO: For now we represent missing bounds as default bounds
    start_bound: FrameBound = attr.ib(factory=default_start_bound)
    end_bound: FrameBound = attr.ib(factory=default_end_bound)

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        window_string_builder = []
        if self.parent_window:
            window_string_builder.append(self.parent_window)
        if self.partitions:
            window_string_builder += [
                "PARTITION BY",
                ",".join(partition.sql(opts) for partition in self.partitions),
            ]
        if self.orderby:
            window_string_builder += [
                "ORDER BY",
                ",".join(order_term.sql(opts) for order_term in self.orderby),
            ]
        window_string_builder += [
            self.frame_type.value,
            "BETWEEN",
            self.start_bound.sql(opts),
            "AND",
            self.end_bound.sql(opts),
        ]
        return " ".join(window_string_builder)
