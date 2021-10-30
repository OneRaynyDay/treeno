import attr
from abc import ABC, abstractmethod
from typing import Optional, List
from treeno.util import chain_identifiers
from treeno.expression import Value
from enum import Enum


class Relation(ABC):
    """A value can be one of the following:

    1. (Table, ValuesTable) A table reference or an inline table
    2. (QueryBuilder, Lateral) A subquery. This subquery may or may not be correlated
        (which means the query gets executed once per row fetched from the outer query)
    3. (Unnest) An unnested expression of arrays
    4. (TableSample) A subset sample of any 1-5.
    5. (Join) A join composing of any 1-5.
    """

    def __init__(self):
        print("HRLELLO")

    @abstractmethod
    def __str__(self):
        raise NotImplementedError("All relations must implement __str__")


@attr.s
class Table(Relation):
    """A table reference uniquely identified by a qualified name
    """

    name: str = attr.ib()
    schema: Optional[str] = attr.ib(default=None)
    catalog: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.catalog:
            assert (
                self.schema
            ), "If a catalog is specified, a schema must be specified as well"

    def __str__(self):
        return chain_identifiers(self.catalog, self.schema, self.name)


@attr.s
class AliasedRelation(Relation):
    """Represents an alias corresponding to a relation
    """

    relation: Relation = attr.ib()
    alias: str = attr.ib()
    column_aliases: Optional[List[str]] = attr.ib(default=None)

    def __str__(self):
        return f'{self.relation} "{self.alias}"'


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    OUTER = "FULL OUTER"
    CROSS = "CROSS"


class JoinCriteria(ABC):
    @abstractmethod
    def __str__(self):
        raise NotImplementedError("Join criteria must implement __str__")


@attr.s
class JoinUsingCriteria(JoinCriteria):
    """Using allows us to join on equality criterion on multiple rows.

    There's one subtle difference between USING and ON, which is the output number of columns:

    SELECT * FROM (SELECT 1 AS "foo") JOIN (SELECT 1 AS "foo") USING("foo");
     foo
    -----
       1
    (1 row)

    SELECT * FROM (SELECT 1 AS "foo") "a" JOIN (SELECT 1 AS "foo") "b" ON "a"."foo" = "b"."foo";
     foo | foo
    -----+-----
       1 |   1
    (1 row)

    If the output columns have the same name, then upon referencing the columns Trino will fail.
    """

    # We can't use Field from treeno.expression since they refer to a single relation. These column names refer to
    # both the left and right relations of the join.
    column_names: List[str] = attr.ib()

    def __str__(self):
        return (
            f"USING({chain_identifiers(*self.column_names, join_string=',')})"
        )


@attr.s
class JoinOnCriteria(JoinCriteria):
    """Perform a join between two relations using arbitrary relations.
    """

    relation: Value = attr.ib()

    def __str__(self):
        return f"ON {self.relation}"


@attr.s
class JoinConfig:
    join_type: JoinType = attr.ib()
    # Natural joins are dangerous and should be avoided, but since
    # it's valid Trino grammar we'll allow it for now.
    natural: bool = attr.ib(default=False)
    criteria: Optional[JoinCriteria] = attr.ib(default=None)

    def __attrs_post_init__(self):
        if self.criteria:
            assert (
                not self.natural
            ), "If criteria is specified, the join cannot be natural"

    def __str__(self):
        join_config_string = "NATURAL " if self.natural else ""
        join_config_string += f"{self.join_type.value} JOIN"


@attr.s
class Join(Relation):
    """Represents a join between two relations
    """

    left_relation: Relation = attr.ib()
    right_relation: Relation = attr.ib()
    config: JoinConfig = attr.ib()

    def __str__(self):
        join_config_string = str(self.left_relation)
        if self.config.natural:
            join_config_string += " NATURAL"
        join_config_string += f" {self.config.join_type.value} JOIN "
        join_config_string += str(self.right_relation)
        return join_config_string


class Unnest(Relation):
    """Represents an unnested set of arrays representing a table
    """

    def __init__(self):
        super().__init__()
        raise NotImplementedError("Unnest currently not implemented")


class Lateral(Relation):
    """Represents a correlated subquery.
    """

    def __init__(self):
        super().__init__()
        raise NotImplementedError("Unnest currently not implemented")
