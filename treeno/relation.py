import attr
from abc import ABC
from typing import Optional, List
from treeno.util import chain_identifiers, parenthesize
from treeno.expression import Value
from treeno.base import Sql, SetQuantifier
from treeno.groupby import GroupBy
from enum import Enum


class Relation(Sql, ABC):
    """A value can be one of the following:

    1. (Table, ValuesTable) A table reference or an inline table
    2. (Query) A subquery. This subquery may or may not be correlated
        (which means the query gets executed once per row fetched from the outer query)
    3. (Unnest) An unnested expression of arrays
    4. (TableSample) A subset sample of any 1-5.
    5. (Join) A join composing of any 1-5.

    In the case of 1), they can be reinterpreted as standalone queries, which are also relations
    but belong to 2). We have SelectQuery, TablesQuery, and ValuesQuery for this purpose.
    """


@attr.s
class Query(Relation, ABC):
    """Represents a query with filtered outputs
    """

    offset: Optional[Value] = attr.ib(default=None, kw_only=True)
    limit: Optional[int] = attr.ib(default=None, kw_only=True)
    orderby: Optional[List[Value]] = attr.ib(default=None, kw_only=True)
    with_queries: List["Query"] = attr.ib(factory=list, kw_only=True)

    def with_query_string_builder(self) -> List[str]:
        # TODO: Use this to include with in the output
        if not self.with_queries:
            return []
        return ["WITH", ",".join(str(query) for query in self.with_queries)]

    def constraint_string_builder(self) -> List[str]:
        str_builder = []
        if self.orderby:
            str_builder += [
                "ORDER BY ",
                ",".join(str(order) for order in self.orderby),
            ]
        if self.offset:
            str_builder += ["OFFSET", str(self.offset)]
        if self.limit:
            str_builder += ["LIMIT", str(self.limit)]
        return str_builder


@attr.s
class SelectQuery(Query):
    """Represents a high level SELECT query.
    """

    select: List[Value] = attr.ib()
    from_relation: Optional[Relation] = attr.ib(default=None)
    where: Optional[Value] = attr.ib(default=None)
    groupby: Optional[GroupBy] = attr.ib(default=None)
    having: Optional[Value] = attr.ib(default=None)
    select_quantifier: SetQuantifier = attr.ib(default=SetQuantifier.ALL)
    window: Optional[Value] = attr.ib(default=None, kw_only=True)

    def __attrs_post_init__(self) -> None:
        assert not self.offset, "Offset isn't supported"
        assert not self.window, "Window isn't supported"

    def sql(self, pretty=False) -> str:
        str_builder = ["SELECT"]
        # All is the default, so we don't need to mention it
        if self.select_quantifier != SetQuantifier.ALL:
            str_builder.append(self.select_quantifier.name)

        str_builder.append(",".join(str(val) for val in self.select))
        if self.from_relation:
            relation_str = str(self.from_relation)
            # Queries need to be parenthesized to be considered relations
            if isinstance(self.from_relation, Query):
                relation_str = parenthesize(relation_str)
            str_builder += ["FROM", relation_str]
        if self.where:
            str_builder += ["WHERE", str(self.where)]
        if self.groupby:
            str_builder += ["GROUP BY", str(self.groupby)]
        if self.having:
            str_builder += ["HAVING", str(self.having)]
        if self.window:
            str_builder += ["WINDOW", str(self.window)]
        str_builder += self.constraint_string_builder()
        return " ".join(str_builder)


@attr.s
class Table(Relation):
    """A table reference uniquely identified by a qualified name

    Tables can be standalone queries by themselves (in the form TABLE {table name}), and thus
    can be subject to orderby, offset, and limit constraints. Refer to TableQuery for more information.
    """

    name: str = attr.ib()
    schema: Optional[str] = attr.ib(default=None)
    catalog: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.catalog:
            assert (
                self.schema
            ), "If a catalog is specified, a schema must be specified as well"

    def sql(self, pretty=False) -> str:
        return chain_identifiers(self.catalog, self.schema, self.name)


@attr.s
class TableQuery(Query):
    table: Table = attr.ib()

    def sql(self, pretty=False) -> str:
        table_str = [str(self.table)]
        table_str += self.constraint_string_builder()
        return " ".join(table_str)


@attr.s
class ValuesTable(Relation):
    """A literal table constructed by literal ROWs

    Values tables can be standalone queries by themselves, and thus can be subject to
    orderby, offset, and limit constraints. Refer to ValuesQuery for more information.
    """

    exprs: List[Value] = attr.ib()


@attr.s
class ValuesQuery(Query):
    """Represents a literal table query.
    """

    table: ValuesTable = attr.ib()

    def sql(self, pretty=False) -> str:
        values_str = [str(self.table)]
        values_str += self.constraint_string_builder()
        return " ".join(values_str)


@attr.s
class AliasedRelation(Relation):
    """Represents an alias corresponding to a relation
    """

    relation: Relation = attr.ib()
    alias: str = attr.ib()
    column_aliases: Optional[List[str]] = attr.ib(default=None)

    def sql(self, pretty=False) -> str:
        # TODO: Keep the "AS"?
        alias_str = f'{self.relation} "{self.alias}"'
        if self.column_aliases:
            columns_str = ",".join(self.column_aliases)
            alias_str += f" ({columns_str})"
        return alias_str


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    OUTER = "FULL OUTER"
    CROSS = "CROSS"


class JoinCriteria(Sql, ABC):
    """Join criterias are complex expressions that describe exactly how a JOIN is done."""


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

    # NOTE: We can't use Field from treeno.expression since they refer to a single relation. These column names refer to
    # both the left and right relations of the join.
    column_names: List[str] = attr.ib()

    def sql(self, pretty=False):
        return (
            f"USING({chain_identifiers(*self.column_names, join_string=',')})"
        )


@attr.s
class JoinOnCriteria(JoinCriteria):
    """Perform a join between two relations using arbitrary relations.
    """

    relation: Value = attr.ib()

    def sql(self, pretty=False):
        return f"ON {self.relation}"


@attr.s
class JoinConfig:
    join_type: JoinType = attr.ib()
    # Natural joins are dangerous and should be avoided, but since
    # it's valid Trino grammar we'll allow it for now.
    natural: bool = attr.ib(default=False)
    criteria: Optional[JoinCriteria] = attr.ib(default=None)

    def __attrs_post_init__(self):
        if self.join_type == JoinType.CROSS:
            assert not self.criteria, "Cross joins cannot specify join criteria"

        if self.criteria:
            assert (
                not self.natural
            ), "If criteria is specified, the join cannot be natural"


@attr.s
class Join(Relation):
    """Represents a join between two relations
    """

    left_relation: Relation = attr.ib()
    right_relation: Relation = attr.ib()
    config: JoinConfig = attr.ib()

    def sql(self, pretty=False) -> str:
        join_config_string = str(self.left_relation)
        if self.config.natural:
            join_config_string += " NATURAL"
        join_config_string += f" {self.config.join_type.value} JOIN "
        join_config_string += str(self.right_relation)
        if self.config.criteria is not None:
            join_config_string += str(self.config.criteria)
        return join_config_string


@attr.s
class Unnest(Relation):
    """Represents an unnested set of arrays representing a table
    """

    array: List[Value] = attr.ib()
    with_ordinality: bool = attr.ib(default=False, kw_only=True)

    def sql(self, pretty=False) -> str:
        arrays_str = ",".join(str(arr) for arr in self.array)
        str_builder = [f"UNNEST({arrays_str})"]
        if self.with_ordinality:
            str_builder += " WITH ORDINALITY"
        return " ".join(str_builder)


@attr.s
class Lateral(Relation):
    """Represents a correlated subquery.
    """

    subquery: Query = attr.ib()

    def sql(self, pretty=False) -> str:
        return f"LATERAL ({self.subquery})"
