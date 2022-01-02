from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import attr

from treeno.base import PrintMode, PrintOptions, SetQuantifier, Sql
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import row, unknown
from treeno.datatypes.types import DataType
from treeno.expression import Field, Value
from treeno.groupby import GroupBy
from treeno.orderby import OrderTerm
from treeno.printer import StatementPrinter, join_stmts, pad
from treeno.util import chain_identifiers, parenthesize, quote_identifier
from treeno.window import Window

Schema = List[Tuple[Optional[Field], DataType]]


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

    @abstractmethod
    def get_schema(self) -> Optional[Schema]:
        raise NotImplementedError(
            "All Relations must implement a get_schema method"
        )


@attr.s
class Query(Relation, Value, ABC):
    """Represents a query with filtered outputs. Queries are also values, in that they yield row types
    """

    # TODO: Technically, an offset can either be an integer or a question mark(as a parameter).
    offset: Optional[int] = attr.ib(default=None, kw_only=True)
    limit: Optional[int] = attr.ib(default=None, kw_only=True)
    orderby: Optional[List[OrderTerm]] = attr.ib(default=None, kw_only=True)
    # TODO: This doesn't currently support column aliases on the WITH level.
    with_queries: Dict[str, "Query"] = attr.ib(factory=dict, kw_only=True)

    def with_query_string_builder(self, opts: PrintOptions) -> Dict[str, str]:
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
    from_: Optional[Relation] = attr.ib(default=None)
    where: Optional[Value] = attr.ib(default=None)
    groupby: Optional[GroupBy] = attr.ib(default=None)
    having: Optional[Value] = attr.ib(default=None)
    select_quantifier: SetQuantifier = attr.ib(factory=SetQuantifier.default)
    window: Optional[Dict[str, Window]] = attr.ib(default=None, kw_only=True)

    def __attrs_post_init__(self) -> None:
        assert not self.offset, "Offset isn't supported"
        assert not self.window, "Window isn't supported"
        self.data_type = row(dtypes=[val.data_type for val in self.select])

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        select_value = join_stmts([val.sql(opts) for val in self.select], opts)
        # All is the default, so we don't need to mention it
        if self.select_quantifier != SetQuantifier.default():
            select_value = self.select_quantifier.name + " " + select_value
        builder.add_entry("SELECT", select_value)

        if self.from_:
            relation_str = self.from_.sql(opts)
            # Queries need to be parenthesized to be considered relations
            if isinstance(self.from_, Query):
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
            window_string = join_stmts(
                [
                    f"{window_name} AS {parenthesize(window.sql(opts))}"
                    for window_name, window in self.window.items()
                ],
                opts,
            )
            builder.add_entry("WINDOW", window_string)
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)

    def get_schema(self) -> Optional[Schema]:
        schema = []
        for value in self.select:
            identifier = value.identifier()
            field = Field(identifier) if identifier else None
            schema.append((field, value.data_type))
        return schema


@attr.s
class Table(Relation):
    """A table reference uniquely identified by a qualified name

    Tables can be standalone queries by themselves (in the form TABLE {table name}), and thus
    can be subject to orderby, offset, and limit constraints. Refer to TableQuery for more information.
    """

    name: str = attr.ib()
    schema: Optional[str] = attr.ib(default=None)
    catalog: Optional[str] = attr.ib(default=None)

    _column_schema: Optional[Schema] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.catalog:
            assert (
                self.schema
            ), "If a catalog is specified, a schema must be specified as well"

    def sql(self, opts: PrintOptions) -> str:
        return chain_identifiers(self.catalog, self.schema, self.name)

    def get_schema(self) -> Optional[Schema]:
        return self._column_schema


@attr.s
class TableQuery(Query):
    table: Table = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        builder.add_entry("TABLE", self.table.sql(opts))
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)

    def get_schema(self) -> Optional[Schema]:
        return self.table.get_schema()


@attr.s
class ValuesQuery(Query):
    """A literal table constructed by literal ROWs

    Values tables can be standalone queries by themselves, and thus can be subject to
    orderby, offset, and limit constraints. Refer to ValuesQuery for more information.
    """

    exprs: List[Value] = attr.ib()

    def __attrs_post_init__(self) -> None:
        self.data_type = row(dtypes=[val.data_type for val in self.exprs])

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        builder.add_entry(
            "VALUES", join_stmts([expr.sql(opts) for expr in self.exprs], opts)
        )
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)

    def get_schema(self) -> Optional[Schema]:
        return [(None, val.data_type) for val in self.exprs]


@attr.s
class AliasedRelation(Relation):
    """Represents an alias corresponding to a relation
    """

    relation: Relation = attr.ib()
    alias: str = attr.ib()
    column_aliases: Optional[List[str]] = attr.ib(default=None)

    def sql(self, opts: PrintOptions) -> str:
        # TODO: Keep the "AS"?
        alias_str = f'{relation_string(self.relation, opts)} "{self.alias}"'
        if self.column_aliases:
            alias_str += f" ({join_stmts(self.column_aliases, opts)})"
        return alias_str

    def get_schema(self) -> Optional[Schema]:
        old_schema = self.relation.get_schema()
        if not old_schema:
            return None
        else:
            new_schema = []
            for idx, tpl in enumerate(old_schema):
                field, dtype = tpl
                if field is not None:
                    field_name = (
                        self.column_aliases[idx]
                        if self.column_aliases
                        else field.name
                    )
                    field = attr.evolve(
                        field, table=self.alias, name=field_name
                    )
                new_schema.append((field, dtype))
            return new_schema


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    OUTER = "FULL OUTER"
    CROSS = "CROSS"


class JoinCriteria(Sql, ABC):
    """Join criterias are complex expressions that describe exactly how a JOIN is done."""

    @abstractmethod
    def build_sql(self, opts: PrintOptions) -> Dict[str, Any]:
        ...

    def sql(self, opts: PrintOptions) -> str:
        return StatementPrinter(stmt_mapping=self.build_sql(opts)).to_string(
            opts
        )


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

    def build_sql(self, opts: PrintOptions):
        return {"USING": parenthesize(join_stmts(self.column_names, opts))}


@attr.s
class JoinOnCriteria(JoinCriteria):
    """Perform a join between two relations using arbitrary relations.
    """

    relation: Value = attr.ib()

    def build_sql(self, opts: PrintOptions):
        # For complex boolean expressions i.e. conjunctions and disjunctions we have a new line, so we have to
        # indent it here for readability
        return {"ON": pad(self.relation.sql(opts), opts.spaces)}


@attr.s
class JoinConfig(Sql):
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

    def sql(self, opts: PrintOptions) -> str:
        raise NotImplementedError(
            "JoinConfig.sql should not be used. Refer to Join"
        )


@attr.s
class Join(Relation):
    """Represents a join between two relations
    """

    left_relation: Relation = attr.ib()
    right_relation: Relation = attr.ib()
    config: JoinConfig = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter(river=False)
        # No value to the key, which is just the relation itself
        builder.add_entry(relation_string(self.left_relation, opts), "")
        join_type = ""
        if self.config.natural:
            join_type += "NATURAL "
        join_type += f"{self.config.join_type.value} JOIN"
        builder.add_entry(join_type, relation_string(self.right_relation, opts))
        if self.config.criteria is not None:
            builder.update(self.config.criteria.build_sql(opts))
        return builder.to_string(opts)

    def get_schema(self) -> Optional[Schema]:
        return (
            self.left_relation.get_schema() + self.right_relation.get_schema()
        )


@attr.s
class Unnest(Relation):
    """Represents an unnested set of arrays representing a table
    """

    arrays: List[Value] = attr.ib()
    with_ordinality: bool = attr.ib(default=False, kw_only=True)

    def __attrs_post_init__(self) -> None:
        dtypes = []
        for val in self.arrays:
            if val.data_type.type_name != type_consts.ARRAY:
                dtypes.append(unknown())
            else:
                dtypes.append(val.data_type.parameters["dtype"])
        self.data_type = row(dtypes=dtypes)

    def sql(self, opts: PrintOptions) -> str:
        str_builder = [
            f"UNNEST({join_stmts([arr.sql(opts) for arr in self.arrays], opts)})"
        ]
        if self.with_ordinality:
            str_builder += " WITH ORDINALITY"
        return " ".join(str_builder)

    def get_schema(self) -> Optional[Schema]:
        return [(None, dtype) for dtype in self.data_type.parameters["dtypes"]]


@attr.s
class Lateral(Relation):
    """Represents a correlated subquery.
    """

    subquery: Query = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        return f"LATERAL({self.subquery.sql(opts)})"

    def get_schema(self) -> Optional[Schema]:
        return self.subquery.get_schema()


class SampleType(Enum):
    BERNOULLI = "BERNOULLI"
    SYSTEM = "SYSTEM"


@attr.s
class TableSample(Relation):
    """Represents a sampled table/subquery
    TODO: We should do some checks that a TableSample can't contain a Join relation for example.
    """

    relation: Relation = attr.ib()
    sample_type: SampleType = attr.ib()
    percentage: Value = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        # Queries need to be parenthesized to be considered relations
        relation_sql = relation_string(self.relation, opts)
        return f"{relation_sql} TABLESAMPLE {self.sample_type.value}({self.percentage.sql(opts)})"

    def get_schema(self) -> Optional[Schema]:
        return self.relation.get_schema()


def relation_string(relation: Relation, opts: PrintOptions) -> str:
    relation_str = relation.sql(opts)
    if isinstance(relation, Query):
        if opts.mode == PrintMode.PRETTY:
            relation_str = "\n" + relation_str
        return parenthesize(relation_str)
    else:
        return relation_str
