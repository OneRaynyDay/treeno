"""
Every relation in Treeno is a :class:`Relation`. `Relations
<https://en.wikipedia.org/wiki/Relation_(database)>`_ are SQL entities that contain rows of data,
with every row being the same data type, and each element in the row are assigned its corresponding column name.
You can think of relations as "things I can FROM in a SELECT query".

For example, the following constructs are all relations:

.. code-block:: sql

    trino> VALUES 1,2,3;
     _col0
    -------
         1
         2
         3

    trino> SELECT 1,2,3;
     _col0 | _col1 | _col2
    -------+-------+-------
         1 |     2 |     3

    trino> SELECT * FROM UNNEST(ARRAY[1,2,3]);
     _col0
    -------
         1
         2
         3

    trino> SELECT * FROM some_table;
     _col0
    -------
         1
         2
         3

Not all :class:`Relation` s are directly queryable though! For those that are directly queryable, i.e. SELECT statements,
TABLE statements, etc, they inherit from :class:`Query` which directly inherits from :class:`Relation`.

Since Treeno does not try to communicate with the metastore(s) powering :class:`Table` s, we need user input to resolve
schemas. More information can be found in :class:`Relation`.
"""

import copy
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Set, Type

import attr

from treeno.base import PrintMode, PrintOptions, SetQuantifier, Sql
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import row, unknown
from treeno.datatypes.conversions import common_supertype
from treeno.datatypes.types import DataType
from treeno.expression import Value, value_attr
from treeno.groupby import GroupBy
from treeno.orderby import OrderTerm
from treeno.printer import StatementPrinter, join_stmts, pad
from treeno.util import chain_identifiers, parenthesize, quote_identifier
from treeno.window import Window


class Relation(Sql, ABC):
    """Represents a SQL relation.

    A :class:`Relation` can be one of the following:

    1. (:class:`Table`) A table reference.
    2. (:class:`Query`) A subquery. :class:`Query` is an abstract class, and its subclasses contain
        :class:`SelectQuery`, :class:`ValuesTable`, :class:`SetQuery` and its subclasses, etc.
    3. (:class:`Unnest`) An unnested expression of arrays.
    4. (:class:`Lateral`) Shares the current relation's namespace with any :class:`Query`.
    5. (:class:`TableSample`) A subset sample of any relation.
    6. (:class:`Join`) A join composing of any relation.
    7. (:class:`AliasedRelation`) An alias (both as relation name and its column names) of any relation.
    """

    def identifier(self) -> Optional[str]:
        """Shorthand identifier for the relation

        .. todo:: This identifier function is used to check for whether dereferences in fields correspond to a relation,
            but there are multiple ways we can dereference. catalog.schema.table.field is a valid identifier, not only
            table.field, which identifier would return "table" for and match on.

        Returns:
            A string if the relation has a well-defined identifier, otherwise None
        """
        return None

    @abstractmethod
    def resolve(self, existing_schema: "Schema") -> "Schema":
        """Resolves the current relation's schema with supplemental schema information.

        This function is used to resolve missing types in queries. The user can pass in existing_schemas to
        hint at types of dynamically determined fields i.e. a table in Trino, but we also use resolve() underneath
        the hood in order to resolve some types that can only be determined through a cross-query tree traversal. For
        more information refer to :class:`Schema`.

        Args:
            existing_schema: Schema with a partial list of fields to resolve the current field's schema
        Returns:
            A new schema for the current relation
        """
        ...


@attr.s
class SchemaField:
    """Represents a single field in a :class:`Schema`

    A schema must have a data type, a source, and optionally have a name. For example, a SUM(x) term with no alias
    should have no name. It still shows up in the output, but there's no way to refer to it.

    Attributes:
        name: An optional name of the schema field. If the name is None, then the field cannot be selected later on,
            and Trino will autogenerate column names for these e.g. _col0
        source: The source of which the field came from. Important to disambiguate :class:`SchemaField` with same
            names across different sources
        data_type: The data type of the field
    """

    name: Optional[str] = attr.ib()
    source: Relation = attr.ib()
    data_type: DataType = attr.ib()


@attr.s
class Schema:
    """Represents the output schema of a given :class:`Relation`

    :py:class:`Schema` s are used to impute missing types from trees with partial type information. This class serves two
    main functions. The first is to resolve cross-query references i.e. WITH queries which can be referenced from the
    FROM clause in a :class:`SelectQuery`. The second is to allow the user to pass in schemas to resolve relations
    like :class:`Table` s which can't resolve its own schema without access to external metadata (i.e. from a metastore).

    Attributes:
        fields: A list of :class:`SchemaField` s that exist in the current namespace
        Relation_ids: A set of relations used to denote the existence of a relation included in the schema.
            This set can contain relations that contains no fields - e.g. for a table with an undefined schema,
            the fields would be empty but the relation would still be in the set
    """

    fields: List[SchemaField] = attr.ib()
    relation_ids: Set[str] = attr.ib()

    @classmethod
    def empty_schema(cls) -> "Schema":
        return cls(fields=[], relation_ids=set())

    def merge(self, another_schema: "Schema") -> "Schema":
        """Merge two schemas together to create a new schema

        >>> from treeno.datatypes.builder import bigint
        >>> schema_a = Schema([SchemaField("x", Table("a"), bigint())], relation_ids={"a"})
        >>> schema_b = Schema([SchemaField("y", Table("b"), bigint())], relation_ids={"b"})
        >>> resulting_schema = schema_a.merge(schema_b)
        >>> assert resulting_schema.fields == schema_a.fields + schema_b.fields
        >>> assert resulting_schema.relation_ids == {"a", "b"}

        Args:
            another_schema: The other schema to merge with. The fields from it are appended to the end of the resulting
                :class:`Schema` fields
        Returns:
            A new :class:`Schema` object with both schemas' fields and relations in the same namespace.
        """
        # .. todo:: Should this be deep copies? I don't see a point in trying to deepcopy relations since they're expensive.
        fields = copy.copy(self.fields)
        relation_ids = copy.copy(self.relation_ids)
        # .. todo:: This is also pretty slow, O(N^2)
        for f in another_schema.fields:
            if f not in fields:
                fields.append(f)
        relation_ids |= another_schema.relation_ids
        return Schema(fields, relation_ids)


@value_attr
class Query(Relation, Value, ABC):
    """Represents a query with filtered outputs.

    Note that Queries are also :class:`treeno.expression.Values`, in that they are just row data typed expressions.
    """

    # .. todo:: Technically, an offset can either be an integer or a question mark(as a parameter).
    offset: Optional[int] = attr.ib(default=None, kw_only=True)
    limit: Optional[int] = attr.ib(default=None, kw_only=True)
    orderby: Optional[List[OrderTerm]] = attr.ib(default=None, kw_only=True)
    with_: List["AliasedRelation"] = attr.ib(factory=dict, kw_only=True)

    def _with_query_string_builder(self, opts: PrintOptions) -> Dict[str, str]:
        """Creates a dictionary mapping of the WITH clause to the queries.

        This function is used solely for :func:`treeno.Sql.sql`, where we use :class:`treeno.printer.StatementPrinter`
        to format the "river" for readability.
        """
        if not self.with_:
            return {}
        # NOTE: We hold AliasedRelations in self.with_, but the sql output for it should be in
        # cte namedQuery form - it's not the same as a typical AliasedRelation.sql().
        return {
            "WITH": join_stmts(
                [query._named_query_sql(opts) for query in self.with_], opts
            )
        }

    def _resolve_with(self) -> Schema:
        """Resolves the queries in WITH statement with context that carries over.

        It's important to note that CTE's can actually refer to previously defined CTE's using FROM:

        .. code-block:: sql

            trino> WITH a (foo) AS (SELECT 1), b (bar) AS (SELECT a.foo + 1 FROM a) SELECT bar FROM b;
             bar
            -----
               2
            (1 row)

        In the above example, ``b`` is able to refer to ``a`` because it's defined before in the WITH sequence.
        """
        merging_schema = Schema.empty_schema()
        for w in self.with_:
            new_schema = w.resolve(merging_schema)
            merging_schema = merging_schema.merge(new_schema)
        return merging_schema

    def _constraint_string_builder(self, opts: PrintOptions) -> Dict[str, str]:
        """Creates a dictionary mapping of a couple output-filtering constraints to the query.

        This function is used solely for :func:`treeno.Sql.sql`, where we use :class:`treeno.printer.StatementPrinter`
        to format the "river" for readability.
        """
        str_builder = {}
        if self.orderby:
            # Typically the BY in ORDER BY clause goes across the indentation river.
            # .. todo:: The "BY" will offset the max length slightly - we might see join_stmts max out at 80 characters
            #       but we also have a few characters added due to the BY.
            str_builder["ORDER"] = "BY " + join_stmts(
                [order.sql(opts) for order in self.orderby], opts
            )
        if self.offset:
            str_builder["OFFSET"] = str(self.offset)
        if self.limit:
            str_builder["LIMIT"] = str(self.limit)
        return str_builder


@value_attr
class SetQuery(Query, ABC):
    """Represents a set operation on two subqueries.

    For all set operations, the input query schemas must be the coercible with each other (i.e. integer and bigint).
    Otherwise, Trino will complain:

    .. code-block:: sql

        trino> SELECT 1 UNION SELECT 'a';
        ... column 1 in UNION query has incompatible types: integer, varchar(1)

    Attributes:
        left_query: An arbitrary :class:`Query`. The set query's output will be identical in schema to this query.
        right_query: An arbitrary :class:`Query` to filter ``left_query`` on.
        set_quantifier: Quantifier for the output rows. If DISTINCT, then the output rows will only contain unique rows.
            If ALL, then the output rows will contain all rows of the set operation.
    """

    left_query: Query = attr.ib()
    right_query: Query = attr.ib()
    # NOTE: Set quantifier for set queries is DISTINCT by default, instead of ALL.
    set_quantifier: SetQuantifier = attr.ib(default=SetQuantifier.DISTINCT)

    def __attrs_post_init__(self) -> None:
        self.data_type = self._compute_data_type()

    def _compute_data_type(self) -> DataType:
        left_type, right_type = (
            self.left_query.data_type,
            self.right_query.data_type,
        )
        if left_type == unknown() or right_type == unknown():
            return unknown()

        # The subqueries not being row-like is possible for a table/subquery that only has 1 column
        left_row_like = left_type.type_name == type_consts.ROW
        right_row_like = right_type.type_name == type_consts.ROW
        assert (
            left_row_like == right_row_like
        ), f"Either both types are ROWs or neither are. Found {left_type} and {right_type}"

        if not left_row_like:
            return common_supertype(left_type, right_type)

        # .. todo:: Redundant after previous assertion
        assert (
            left_row_like and right_row_like
        ), "Input data types for query set operations must be ROW"
        dtypes = []
        for t1, t2 in zip(
            left_type.parameters["dtypes"], right_type.parameters["dtypes"]
        ):
            dtypes.append(common_supertype(t1, t2))
        return row(dtypes=dtypes)

    def _to_string(self, set_type: str, opts: PrintOptions) -> str:
        spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
        query_string = relation_string(self.left_query, opts, newline=False)
        query_string += spacing
        query_string += set_type
        # NOTE: We currently have set_quantifier explicitly spelled out for set operations because the default ALL
        # for expressions doesn't apply here(the default here is DISTINCT).
        query_string += f" {self.set_quantifier.name}"
        query_string += spacing
        query_string += relation_string(self.right_query, opts, newline=False)
        return query_string

    def resolve(self, existing_schema: Schema) -> Schema:
        left_schema = self.left_query.resolve(
            maybe_prune_schema(self.left_query, existing_schema)
        )
        self.right_query.resolve(
            maybe_prune_schema(self.right_query, existing_schema)
        )
        # The column names and such are always taken from the left schema.
        schema_fields = []
        for schema_field in left_schema.fields:
            # Basically take the schema that belonged to left schema and re-assign it to this set query
            schema_fields.append(
                SchemaField(schema_field.name, self, schema_field.data_type)
            )
        return Schema(schema_fields, relation_ids=left_schema.relation_ids)


@value_attr
class IntersectQuery(SetQuery):
    """Represents an intersection of two queries.

    Note that intersect queries cannot use set quantifier ALL, since Trino implements INTERSECT by hashing the rows
    and subsequently deduplicates input query rows.

    >>> from treeno.expression import NULL
    >>> q = SelectQuery([NULL])
    >>> str(IntersectQuery(q, q, set_quantifier=SetQuantifier.DISTINCT))
    '(SELECT NULL) INTERSECT DISTINCT (SELECT NULL)'
    >>> IntersectQuery(q, q, set_quantifier=SetQuantifier.ALL)
    Traceback (most recent call last):
        ...
    AssertionError: INTERSECT does not support ALL
    """

    def __attrs_post_init__(self) -> None:
        assert (
            self.set_quantifier == SetQuantifier.DISTINCT
        ), f"INTERSECT does not support {self.set_quantifier.name}"

    def sql(self, opts: PrintOptions) -> str:
        return self._to_string("INTERSECT", opts)


@value_attr
class ExceptQuery(SetQuery):
    """Represents a set subtract of left query with right query.

    Note that except queries cannot use set quantifier ALL, since Trino implements EXCEPT by hashing the rows
    and subsequently deduplicates input query rows.

    >>> from treeno.expression import NULL
    >>> q = SelectQuery([NULL])
    >>> str(ExceptQuery(q, q, set_quantifier=SetQuantifier.DISTINCT))
    '(SELECT NULL) EXCEPT DISTINCT (SELECT NULL)'
    >>> ExceptQuery(q, q, set_quantifier=SetQuantifier.ALL)
    Traceback (most recent call last):
        ...
    AssertionError: EXCEPT does not support ALL
    """

    def __attrs_post_init__(self) -> None:
        assert (
            self.set_quantifier == SetQuantifier.DISTINCT
        ), f"EXCEPT does not support {self.set_quantifier.name}"

    def sql(self, opts: PrintOptions) -> str:
        return self._to_string("EXCEPT", opts)


@value_attr
class UnionQuery(SetQuery):
    """Represents a union of two queries' rows.

    >>> from treeno.expression import NULL
    >>> q = SelectQuery([NULL])
    >>> str(UnionQuery(q, q, set_quantifier=SetQuantifier.DISTINCT))
    '(SELECT NULL) UNION DISTINCT (SELECT NULL)'
    >>> str(UnionQuery(q, q, set_quantifier=SetQuantifier.ALL))
    '(SELECT NULL) UNION ALL (SELECT NULL)'
    """

    def sql(self, opts: PrintOptions) -> str:
        return self._to_string("UNION", opts)


@value_attr
class SelectQuery(Query):
    """Represents a high level SELECT query.

    >>> from treeno.expression import wrap_literal, AliasedValue, Field
    >>> table = Table("a")
    >>> query = SelectQuery(
    ...     select=[AliasedValue(wrap_literal(2), "foo"), Field("a") / 5],
    ...     from_=table,
    ...     where=Field("a") > 5
    ... )
    >>> # We can get the SQL string of the query via __str__
    >>> str(query)
    'SELECT 2 "foo","a" / 5 FROM "a" WHERE "a" > 5'
    >>> # The schema of the fields can be retrieved from resolve
    >>> query_fields = query.resolve(Schema.empty_schema()).fields
    >>> # We aliased the literal value as "foo", but we didn't give a name for the second field
    >>> [schema_field.name for schema_field in query_fields]
    ['foo', None]
    >>> # The type for field a is unknown without a schema supplied to the Table object.
    >>> [str(schema_field.data_type) for schema_field in query_fields]
    ['INTEGER', 'UNKNOWN']
    >>> # Alternatively, we can also get the whole row type of the select query via:
    >>> str(query.data_type)
    'ROW(INTEGER,UNKNOWN)'

    Attributes:
        select: A list of :class:`Value` s as outputs to the query.
        from_: An optional relation to select from. If from is not specified, select must contain values that do not
            reference any external relations, i.e. ``SELECT 1``.
        where: An optional boolean clause to filter rows by. If where is not specified, all rows are used.
        groupby: An optional :class:`GroupBy` which allows the user to run partial aggregate functions over different
            groupings. For more information refer to :mod:`treeno.groupby`.
        having: An optional boolean clause to filter groups by. If having is not specified, all groups are used.
        select_quantifier: Whether to return all rows or only the distinct ones. Defaults to ALL.
        window: An optional mapping of window names to :class:`Window` s. For more information refer to
            :mod:`treeno.window`.
    """

    select: List[Value] = attr.ib()
    from_: Optional[Relation] = attr.ib(default=None)
    where: Optional[Value] = attr.ib(default=None)
    groupby: Optional[GroupBy] = attr.ib(default=None)
    having: Optional[Value] = attr.ib(default=None)
    select_quantifier: SetQuantifier = attr.ib(factory=SetQuantifier.default)
    window: Optional[Dict[str, Window]] = attr.ib(default=None, kw_only=True)

    def __attrs_post_init__(self) -> None:
        assert len(
            self.select
        ), "Must select at least one value for SELECT clause"
        self.data_type = self._compute_data_type()

    def _compute_data_type(self) -> DataType:
        # Special case: If the select clause only has one output element, it's not a row.
        if len(self.select) == 1:
            return self.select[0].data_type
        return row(dtypes=[val.data_type for val in self.select])

    def resolve(self, existing_schema: Schema) -> Schema:
        from treeno.datatypes.resolve import resolve_fields

        # .. todo:: Currently resolve only handles a single layer. We should make this a common function across all
        #       relations so we can recursively call resolve before performing resolve on this query.
        # Also, it's probably worth allowing table inputs.
        # Pass in existing relations from CTE into from_ as long as from_ is not in its own namespace i.e. a subquery.
        if self.from_ is not None:
            existing_schema = existing_schema.merge(self._resolve_with())
            schema = self.from_.resolve(
                maybe_prune_schema(self.from_, existing_schema)
            )
            self.select = [resolve_fields(val, schema) for val in self.select]
            self.data_type = self._compute_data_type()

        schema_fields = []
        for value in self.select:
            # All schema fields have the source to the current object
            schema_fields.append(
                SchemaField(
                    name=value.identifier(),
                    source=self,
                    data_type=value.data_type,
                )
            )
        return Schema(schema_fields, relation_ids=set())

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self._with_query_string_builder(opts))
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
        builder.update(self._constraint_string_builder(opts))
        return builder.to_string(opts)


@attr.s
class Table(Relation):
    """A table reference uniquely identified by a qualified name

    Tables can be standalone queries by themselves (in the form TABLE {table name}), and thus
    can be subject to orderby, offset, and limit constraints. Refer to TableQuery for more information.

    >>> from treeno.datatypes.builder import bigint
    >>> table = Table("a")
    >>> assert table._column_schema == Schema([], relation_ids={"a"})
    >>> # Supply a schema to the table via resolve
    >>> new_schema = table.resolve(Schema([SchemaField("x", table, bigint())], relation_ids={"a"}))
    >>> schema_fields = new_schema.fields
    >>> assert table._column_schema == new_schema
    >>> [schema_field.name for schema_field in schema_fields]
    ['x']
    >>> [str(schema_field.data_type) for schema_field in schema_fields]
    ['BIGINT']

    Attributes:
        name: The name of the table.
        schema: The schema the table belongs to. Can be unspecified to denote the current session's schema.
        catalog: The catalog the schema belongs to. Can be unspecified to denote the current session's catalog.
    """

    name: str = attr.ib()
    schema: Optional[str] = attr.ib(default=None)
    catalog: Optional[str] = attr.ib(default=None)

    # The table object will remember its own column_schema here.
    _column_schema: Schema = attr.ib(factory=Schema.empty_schema)

    def __attrs_post_init__(self) -> None:
        if self.catalog:
            assert (
                self.schema
            ), "If a catalog is specified, a schema must be specified as well"
        if self.name not in self._column_schema.relation_ids:
            self._column_schema.relation_ids.add(self.name)

    def sql(self, opts: PrintOptions) -> str:
        return chain_identifiers(self.catalog, self.schema, self.name)

    def resolve(self, existing_schema: Schema) -> Schema:
        # If this schema isn't defined
        if self.name not in existing_schema.relation_ids:
            return self._column_schema
        # Otherwise, the schema IS defined e.g. by a previous CTE
        schema_fields = [
            f
            for f in existing_schema.fields
            if f.source.identifier() == self.identifier()
        ]
        self._column_schema = Schema(
            schema_fields, relation_ids={self.identifier()}
        )
        return self._column_schema

    def identifier(self) -> Optional[str]:
        return self.name


@value_attr
class TableQuery(Query):
    """A light wrapper around a table for querying.

    >>> str(TableQuery(Table("x")))
    'TABLE "x"'

    The above statement is equivalent to SELECT * FROM x.

    Attributes:
        table: The underlying table to select from.
    """

    table: Table = attr.ib()

    def __attrs_post_init__(self) -> None:
        self.data_type = self._compute_data_type()

    def _compute_data_type(self) -> DataType:
        if self.table._column_schema == Schema.empty_schema():
            return row(dtypes=[f.data_type for f in self.table._column_schema])
        return unknown()

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self._with_query_string_builder(opts))
        builder.add_entry("TABLE", self.table.sql(opts))
        builder.update(self._constraint_string_builder(opts))
        return builder.to_string(opts)

    def resolve(self, existing_schema: Schema) -> Schema:
        # We not only need to take the existing schema, but we also need to shadow some fields with the
        # local WITH clause
        with_schema = self._resolve_with()
        table_schema = self.table.resolve(with_schema)
        self.data_type = self._compute_data_type()
        return table_schema


@value_attr
class ValuesQuery(Query):
    """A literal table constructed by literal ROWs

    For a given comma separated list of expressions, the VALUES keyword turns the expression into a subquery that can
    be used later on. VALUES is often used for unit testing fixtures for mock tables.

    Values tables can be standalone queries by themselves, and thus can be subject to
    orderby, offset, and limit constraints. Refer to ValuesQuery for more information.

    >>> from decimal import Decimal
    >>> from treeno.expression import wrap_literal, RowConstructor
    >>> # Types are inferred from all input values coerced together
    >>> query = ValuesQuery([wrap_literal(1), wrap_literal(Decimal("2.2"))])
    >>> str(query)
    'VALUES 1,2.2'
    >>> str(query.data_type)
    'DECIMAL(11,1)'
    >>> query = ValuesQuery([
    ...     RowConstructor([
    ...         wrap_literal(1),
    ...         wrap_literal('a')
    ...     ]),
    ...     RowConstructor([
    ...         wrap_literal(2),
    ...         wrap_literal('b')
    ...     ])
    ... ])
    >>> str(query)
    "VALUES (1,'a'),(2,'b')"
    >>> str(query.data_type)
    'ROW(INTEGER,VARCHAR(1))'

    Attributes:
        exprs: Values for each row of the query.
    """

    exprs: List[Value] = attr.ib()

    def __attrs_post_init__(self) -> None:
        for val in self.exprs:
            if self.data_type == unknown():
                self.data_type = val.data_type
            else:
                self.data_type = common_supertype(self.data_type, val.data_type)

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self._with_query_string_builder(opts))
        builder.add_entry(
            "VALUES", join_stmts([expr.sql(opts) for expr in self.exprs], opts)
        )
        builder.update(self._constraint_string_builder(opts))
        return builder.to_string(opts)

    def resolve(self, existing_schema: Schema) -> Schema:
        # ..todo:: Does ValuesQuery dereference anything by itself?
        return Schema(
            [SchemaField(None, self, val.data_type) for val in self.exprs]
        )


@attr.s
class AliasedRelation(Relation):
    """Represents an alias corresponding to a relation

    Aliased relations can change/add identifier to another underlying :class:`Relation`, and can also rname the columns.
    Here's an example using resolve:

    >>> from treeno.expression import wrap_literal
    >>> aliased_relation = AliasedRelation(SelectQuery([wrap_literal(1), wrap_literal('2')]), "query", ["x", "y"])
    >>> aliased_schema = aliased_relation.resolve(Schema.empty_schema())
    >>> [schema_field.name for schema_field in aliased_schema.fields]
    ['x', 'y']
    >>> aliased_schema.relation_ids
    {'query'}

    .. todo:: Currently we don't check to see whether the field length is equal to our column_aliases parameter. Should
        we check this or let it fail later?

    Attributes:
        relation: A relation to alias over.
        alias: Alias for the relation.
        column_aliases: An optional list of names for each column of the underlying relation.
    """

    relation: Relation = attr.ib()
    alias: str = attr.ib()
    column_aliases: Optional[List[str]] = attr.ib(default=None)

    def sql(self, opts: PrintOptions) -> str:
        # .. todo:: Keep the "AS"?
        alias_str = f"{relation_string(self.relation, opts)} {quote_identifier(self.alias)}"
        if self.column_aliases:
            alias_str += f" ({join_stmts(self.column_aliases, opts)})"
        return alias_str

    def _named_query_sql(self, opts: PrintOptions) -> str:
        alias_str = quote_identifier(self.alias)
        if self.column_aliases:
            alias_str += f" {join_stmts(self.column_aliases, opts)}"
        alias_str += f" AS {relation_string(self.relation, opts)}"
        return alias_str

    def resolve(self, existing_schema: Schema) -> Schema:
        relation_schema = self.relation.resolve(
            maybe_prune_schema(self.relation, existing_schema)
        )
        new_schema_fields = []
        for idx, schema_field in enumerate(relation_schema.fields):
            field_name = (
                self.column_aliases[idx]
                if self.column_aliases
                else schema_field.name
            )
            new_schema_fields.append(
                SchemaField(field_name, self, schema_field.data_type)
            )
        return Schema(new_schema_fields, relation_ids={self.identifier()})

    def identifier(self) -> Optional[str]:
        return self.alias


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    OUTER = "FULL OUTER"
    CROSS = "CROSS"


class JoinCriteria(Sql, ABC):
    """Join criterias are complex expressions that describe exactly how a JOIN is done."""

    @abstractmethod
    def build_sql(self, opts: PrintOptions) -> Dict[str, str]:
        """Creates a dictionary mapping of statements to strings
        """
        ...

    def sql(self, opts: PrintOptions) -> str:
        return StatementPrinter(stmt_mapping=self.build_sql(opts)).to_string(
            opts
        )


@attr.s
class JoinUsingCriteria(JoinCriteria):
    """Using allows us to join on equality criterion on multiple rows.

    There's one subtle difference between USING and ON, which is the output number of columns:

    .. code-block:: sql

        SELECT * FROM (SELECT 1 AS "foo") JOIN (SELECT 1 AS "foo") USING("foo");
         foo
        -----
           1
        (1 row)

    Selects the column once. If we use ON:

    .. code-block:: sql

        SELECT * FROM (SELECT 1 AS "foo") "a" JOIN (SELECT 1 AS "foo") "b" ON "a"."foo" = "b"."foo";
         foo | foo
        -----+-----
           1 |   1
        (1 row)

    If the output columns have the same name, then upon referencing the columns Trino will fail.

    Attributes:
        column_names: A list of column names to check equality clauses for.
    """

    # NOTE: We can't use Field from treeno.expression since they refer to a single relation. These column names refer to
    # both the left and right relations of the join.
    column_names: List[str] = attr.ib()

    def build_sql(self, opts: PrintOptions) -> Dict[str, str]:
        return {"USING": parenthesize(join_stmts(self.column_names, opts))}


@attr.s
class JoinOnCriteria(JoinCriteria):
    """Perform a join between two relations using boolean expressions.

    An example ON usage:

    .. code-block:: sql

        SELECT * FROM (SELECT 1 AS "foo") "a" JOIN (SELECT 1 AS "foo") "b" ON "a"."foo" = "b"."foo";
         foo | foo
        -----+-----
           1 |   1
        (1 row)

    Attributes:
        constraint: A boolean expression constraining the join to where the expression evaluates to True using
            two subqueries' rows.
    """

    constraint: Value = attr.ib()

    def build_sql(self, opts: PrintOptions) -> Dict[str, str]:
        # For complex boolean expressions i.e. conjunctions and disjunctions we have a new line, so we have to
        # indent it here for readability
        return {"ON": pad(self.constraint.sql(opts), opts.spaces)}


@attr.s
class JoinConfig(Sql):
    """Details the method of join used in a :class:`Join`.

    Attributes:
        join_type: The type of JOIN (i.e. CROSS JOIN, INNER JOIN, etc)
        natural: Whether the join is natural (i.e. joins on equality on column names that are the same between two queries)
        criteria: What boolean expression criteria the join requires. Refer to :class:`JoinCriteria` for more info.
    """

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

    Attributes:
        left_relation: An arbitrary relation to be joined on
        right_relation: An arbitrary relation to be joined on
        config: Details on what type of join we're performing. Refer to :class:`JoinConfig` for more info.
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

    def resolve(self, existing_schema: Schema) -> Schema:
        pruned_left_arg = maybe_prune_schema(
            self.left_relation, existing_schema
        )
        left_schema = self.left_relation.resolve(pruned_left_arg)
        existing_schema = existing_schema.merge(left_schema)
        # Pass in the namespace of the first relation to the second. This is important for sql statemnts like
        # SELECT x FROM a CROSS JOIN UNNEST(a.foo), where UNNEST requires a to be selected first
        pruned_right_arg = maybe_prune_schema(
            self.right_relation, existing_schema
        )
        right_schema = self.right_relation.resolve(pruned_right_arg)
        return left_schema.merge(right_schema)


@attr.s
class Unnest(Relation):
    """Represents an unnested set of arrays representing a table

    An example of unnest:

    .. code-block:: sql

        trino> SELECT * FROM UNNEST(ARRAY[1,2]);
         _col0
        -------
             1
             2

    Attributes:
        arrays: The array(s) to use in an unnest. The i-th array in the list corresponds to the i-th output column.
        with_ordinality: Whether there should be an ordinality column appended at the end of the UNNEST.
    """

    arrays: List[Value] = attr.ib()
    with_ordinality: bool = attr.ib(default=False, kw_only=True)

    def __attrs_post_init__(self) -> None:
        self.data_type = self._compute_data_type()

    def _compute_data_type(self) -> DataType:
        dtypes = []
        for val in self.arrays:
            if val.data_type.type_name != type_consts.ARRAY:
                dtypes.append(unknown())
            else:
                dtypes.append(val.data_type.parameters["dtype"])
        return row(dtypes=dtypes)

    def sql(self, opts: PrintOptions) -> str:
        str_builder = [
            f"UNNEST({join_stmts([arr.sql(opts) for arr in self.arrays], opts)})"
        ]
        if self.with_ordinality:
            str_builder += " WITH ORDINALITY"
        return " ".join(str_builder)

    def resolve(self, existing_schema: Schema) -> Schema:
        from treeno.datatypes.resolve import resolve_fields

        # There's no base
        self.arrays = [
            resolve_fields(arr, existing_schema) for arr in self.arrays
        ]
        self.data_type = self._compute_data_type()
        return Schema(
            fields=[
                SchemaField(name=None, source=self, data_type=dtype)
                for dtype in self.data_type.parameters["dtypes"]
            ],
            relation_ids=set(),
        )


@attr.s
class Lateral(Relation):
    """Represents a correlated subquery.

    Attributes:
        subquery: The subquery to correlate with the current scope.
    """

    subquery: Query = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        return f"LATERAL({self.subquery.sql(opts)})"

    def resolve(self, existing_schema: Schema) -> Schema:
        # NOTE: We explicitly don't maybe_prune_schema here, since lateral needs to explicitly pass it down
        return self.subquery.resolve(existing_schema)


class SampleType(Enum):
    BERNOULLI = "BERNOULLI"
    SYSTEM = "SYSTEM"


@attr.s
class TableSample(Relation):
    """Represents a sampled relation.

    Note that TABLESAMPLE can work with all relations, including JOINs. However, for JOINs we need to take extra care
    to parenthesize the expression. This doesn't work:

    .. code-block:: sql

        trino> WITH f (a) AS (SELECT 1), g (b) AS (SELECT 1) SELECT f.a, g.b FROM f JOIN g ON f.a = g.b TABLESAMPLE BERNOULLI(99);
        ... mismatched input 'TABLESAMPLE' ...

    But this does:

    .. code-block:: sql

        trino> WITH f (a) AS (SELECT 1), g (b) AS (SELECT 1) SELECT f.a, g.b FROM (f JOIN g ON f.a = g.b) TABLESAMPLE BERNOULLI(99);
         a | b
        ---+---
         1 | 1

    Because JOINs are higher on the parsing hierarchy than TABLESAMPLEs, potentially due to an ambiguous parse
    otherwise where TABLESAMPLE can be applied to the right relation in a JOIN.
    """

    relation: Relation = attr.ib()
    sample_type: SampleType = attr.ib()
    percentage: Value = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        # Queries need to be parenthesized to be considered relations
        relation_sql = relation_string(
            self.relation,
            opts,
            newline=False,
            special_parenthesize_relations=[Join],
        )
        return f"{relation_sql} TABLESAMPLE {self.sample_type.value}({self.percentage.sql(opts)})"

    def resolve(self, existing_schema: Schema) -> Schema:
        # NOTE: We don't maybe_prune_schema here, because table sample should not affect the namespace scope
        return self.relation.resolve(existing_schema)


def relation_string(
    relation: Relation,
    opts: PrintOptions,
    newline: bool = True,
    special_parenthesize_relations: Optional[List[Type[Relation]]] = None,
) -> str:
    relation_str = relation.sql(opts)
    parenthesize_classes = [Query]
    if special_parenthesize_relations:
        parenthesize_classes.extend(special_parenthesize_relations)
    if isinstance(relation, tuple(parenthesize_classes)):
        if opts.mode == PrintMode.PRETTY and newline:
            relation_str = "\n" + relation_str
        return parenthesize(relation_str)
    else:
        return relation_str


def maybe_prune_schema(relation: Relation, existing_schema: Schema) -> Schema:
    """Prune the schema depending on whether the relation will enter a new namespace

    Some relations don't want extra schema information to be passed through because it's in its own parenthesized
    expression which is not aware of the outer namespace.

    Args:
        relation: The relation to enter into with the given schema
        existing_schema: The schema to pass into the relation
    Returns:
        Either an empty :class:`Schema` or the original :class:`Schema`.
    """
    if isinstance(relation, Query):
        return Schema.empty_schema()
    return existing_schema
