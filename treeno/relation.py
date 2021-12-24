import attr
from abc import ABC
from typing import Optional, List, Dict
from treeno.util import chain_identifiers, parenthesize, quote_identifier
from treeno.expression import Value
from treeno.base import Sql, SetQuantifier, PrintOptions, PrintMode
from treeno.printer import StatementPrinter, pad, join_stmts
from treeno.groupby import GroupBy
from treeno.orderby import OrderTerm
from treeno.window import Window
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

    # TODO: Technically, an offset can either be an integer or a question mark(as a parameter).
    offset: Optional[int] = attr.ib(default=None, kw_only=True)
    limit: Optional[int] = attr.ib(default=None, kw_only=True)
    orderby: Optional[List[OrderTerm]] = attr.ib(default=None, kw_only=True)
    # TODO: This doesn't currently support column aliases on the WITH level.
    with_queries: Dict[str, "Query"] = attr.ib(factory=dict, kw_only=True)

    def with_query_string_builder(self, opts: PrintOptions) -> Dict[str, str]:
        if isinstance(self.with_queries, list):
            import pdb

            pdb.set_trace()
        if not self.with_queries:
            return {}
        newline_if_pretty = "\n" if opts.mode == PrintMode.PRETTY else ""
        return {
            "WITH": join_stmts(
                [
                    f"{quote_identifier(name)} AS ({newline_if_pretty}{query.sql(opts)})"
                    for name, query in self.with_queries.items()
                ],
                opts,
            )
        }

    def constraint_string_builder(self, opts: PrintOptions) -> Dict[str, str]:
        str_builder = {}
        if self.orderby:
            # Typically the ORDER BY clause goes across the indentation river.
            # TODO: The "BY" will offset the max length slightly.
            str_builder["ORDER"] = "BY " + join_stmts(
                [order.sql(opts) for order in self.orderby], opts
            )
        if self.offset:
            str_builder["OFFSET"] = str(self.offset)
        if self.limit:
            str_builder["LIMIT"] = str(self.limit)
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
    window: Optional[Dict[str, Window]] = attr.ib(default=None, kw_only=True)

    def __attrs_post_init__(self) -> None:
        assert not self.offset, "Offset isn't supported"
        assert not self.window, "Window isn't supported"

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        select_value = join_stmts([val.sql(opts) for val in self.select], opts)
        # All is the default, so we don't need to mention it
        if self.select_quantifier != SetQuantifier.ALL:
            select_value = self.select_quantifier.name + " " + select_value
        builder.add_entry("SELECT", select_value)

        if self.from_relation:
            relation_str = self.from_relation.sql(opts)
            # Queries need to be parenthesized to be considered relations
            if isinstance(self.from_relation, Query):
                # Add padding of 1 character to realign the statement)
                relation_str = pad(parenthesize(relation_str), 1)
            builder.add_entry("FROM", relation_str)
        if self.where:
            builder.add_entry("WHERE", self.where.sql(opts))
        if self.groupby:
            builder.add_entry("GROUP", "BY " + self.groupby.sql(opts))
        if self.having:
            builder.add_entry("HAVING", self.having.sql(opts))
        if self.window:
            builder.add_entry("WINDOW", self.window.sql(opts))
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)


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

    def sql(self, opts: PrintOptions) -> str:
        return chain_identifiers(self.catalog, self.schema, self.name)


@attr.s
class TableQuery(Query):
    table: Table = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        builder.add_entry("TABLE", self.table.sql(opts))
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)


@attr.s
class ValuesQuery(Query):
    """A literal table constructed by literal ROWs

    Values tables can be standalone queries by themselves, and thus can be subject to
    orderby, offset, and limit constraints. Refer to ValuesQuery for more information.
    """

    exprs: List[Value] = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        builder.add_entry(
            "VALUES", join_stmts([expr.sql(opts) for expr in self.exprs], opts)
        )
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)


@attr.s
class AliasedRelation(Relation):
    """Represents an alias corresponding to a relation
    """

    relation: Relation = attr.ib()
    alias: str = attr.ib()
    column_aliases: Optional[List[str]] = attr.ib(default=None)

    def sql(self, opts: PrintOptions) -> str:
        # TODO: Keep the "AS"?
        alias_str = f'{self.relation} "{self.alias}"'
        if self.column_aliases:
            alias_str += f" ({join_stmts(self.column_aliases, opts)})"
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

    def sql(self, opts: PrintOptions):
        return f"USING({join_stmts(self.column_names, opts)})"


@attr.s
class JoinOnCriteria(JoinCriteria):
    """Perform a join between two relations using arbitrary relations.
    """

    relation: Value = attr.ib()

    def sql(self, opts: PrintOptions):
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

    def sql(self, opts: PrintOptions) -> str:
        join_config_string = self.left_relation.sql(opts)
        if self.config.natural:
            join_config_string += " NATURAL"
        join_config_string += f" {self.config.join_type.value} JOIN "
        join_config_string += self.right_relation.sql(opts)
        if self.config.criteria is not None:
            join_config_string += f" {self.config.criteria.sql(opts)}"
        return join_config_string


@attr.s
class Unnest(Relation):
    """Represents an unnested set of arrays representing a table
    """

    array: List[Value] = attr.ib()
    with_ordinality: bool = attr.ib(default=False, kw_only=True)

    def sql(self, opts: PrintOptions) -> str:
        str_builder = [
            f"UNNEST({join_stmts([arr.sql(opts) for arr in self.array], opts)})"
        ]
        if self.with_ordinality:
            str_builder += " WITH ORDINALITY"
        return " ".join(str_builder)


@attr.s
class Lateral(Relation):
    """Represents a correlated subquery.
    """

    subquery: Query = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        return f"LATERAL ({self.subquery.sql(opts)})"
