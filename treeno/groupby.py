from abc import ABC
from typing import List

import attr

from treeno.base import PrintOptions, SetQuantifier, Sql
from treeno.expression import Value
from treeno.printer import join_stmts


class Group(Sql, ABC):
    """Base class to describe all grouping entities.
    """


@attr.s
class GroupBy(Sql):
    """GroupBys are used to group rows by their membership in grouping sets to get partial aggregates.
    """

    groups: List[Group] = attr.ib()
    groupby_quantifier: SetQuantifier = attr.ib(factory=SetQuantifier.default)

    def sql(self, opts: PrintOptions) -> str:
        groupby_string = join_stmts(
            [group.sql(opts) for group in self.groups], opts
        )
        if self.groupby_quantifier != SetQuantifier.default():
            groupby_string = f"{self.groupby_quantifier.name} {groupby_string}"
        return groupby_string


@attr.s
class GroupingSet(Group):
    """Simple group involving input expr(s). Note that this doesn't accept aliased values, since group bys directly
    reference the input column.

    Note that the grouping set can be empty - it would correspond to interpreting the entire table as one group.
    """

    values: List[Value] = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        if len(self.values) == 1:
            return self.values[0].sql(opts)
        return f"({join_stmts([val.sql(opts) for val in self.values], opts)})"


@attr.s
class GroupingSetList(Group):
    """Describes multiple groupings.
    NOTE: We explicitly disallow the feature of grouping by an index here. This is not a TODO.
    TODO: Give this a better name
    """

    groups: List[GroupingSet] = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert (
            len(self.groups) > 0
        ), "GroupingSetLists must have at least one grouping set"

    def sql(self, opts: PrintOptions) -> str:
        return f"GROUPING SETS ({join_stmts([group.sql(opts) for group in self.groups], opts)})"


@attr.s
class Cube(Group):
    """Creates a grouping set of the powerset of all fields

    For example, CUBE(a,b) yields the grouping sets (), (a), (b), (a,b)
    """

    values: List[Value] = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert (
            len(self.values) > 0
        ), "Cube must have at least one value to group by"

    def sql(self, opts: PrintOptions) -> str:
        return (
            f"CUBE ({join_stmts([val.sql(opts) for val in self.values], opts)})"
        )


@attr.s
class Rollup(Group):
    """Creates a grouping set of cumulatively aggregated columns in order of input.

    For example, ROLLUP(a,b) yields the grouping sets (), (a), (a,b)
    """

    values: List[Value] = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert (
            len(self.values) > 0
        ), "GroupingSets must have at least one value to group by"

    def sql(self, opts: PrintOptions) -> str:
        return f"ROLLUP ({join_stmts([val.sql(opts) for val in self.values], opts)})"
