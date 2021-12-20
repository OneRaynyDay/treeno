import attr
from abc import ABC
from treeno.expression import Value
from treeno.base import Sql, SetQuantifier, PrintOptions
from typing import Optional, List


class Group(Sql, ABC):
    """Base class to describe all grouping entities.
    """


@attr.s
class GroupBy(Sql):
    """GroupBys are used to group rows by their membership in grouping sets to get partial aggregates.
    """

    groups: List[Group] = attr.ib()
    groupby_quantifier: SetQuantifier = attr.ib(default=SetQuantifier.ALL)

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        multi_groupby_string = ",".join()
        return f"{self.groupby_quantifier.name} {multi_groupby_string}"


@attr.s
class GroupingSet(Group):
    """Simple group involving input expr(s). Note that this doesn't accept aliased values, since group bys directly
    reference the input column.
    """

    values: List[Value] = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert (
            len(self.values) > 0
        ), "GroupingSets must have at least one value to group by"

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        if len(self.values) == 1:
            return self.values[0].sql()
        multi_expr_string = ",".join(str(val) for val in self.values)
        return f"({multi_expr_string})"


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

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        multi_group_string = ",".join(str(group) for group in self.groups)
        return f"GROUPING SETS ({multi_group_string})"


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

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        multi_expr_string = ",".join(str(val) for val in self.values)
        return f"CUBE ({multi_expr_string})"


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

    def sql(self, opts: Optional[PrintOptions] = None) -> str:
        multi_expr_string = ",".join(str(val) for val in self.values)
        return f"ROLLUP ({multi_expr_string})"
