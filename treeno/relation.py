import copy
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import attr

from treeno.base import PrintMode, PrintOptions, SetQuantifier, Sql
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import row, unknown
from treeno.datatypes.types import DataType
from treeno.expression import Value
from treeno.groupby import GroupBy
from treeno.orderby import OrderTerm
from treeno.printer import StatementPrinter, join_stmts, pad
from treeno.util import chain_identifiers, parenthesize, quote_identifier
from treeno.window import Window


@attr.s
class SchemaField:
    """Represents a single field in a schema
    A schema must have a data type, a source, and optionally have a name. For example, a SUM(x) term with no alias
    should have no name. It still shows up in the output, but there's no way to refer to it.
    """

    name: Optional[str] = attr.ib()
    source: "Relation" = attr.ib()
    data_type: DataType = attr.ib()


@attr.s
class Schema:
    """Represents the output schema of a given relation.
    Schemas are used to impute missing types from trees with partial type information.
    Relation_ids does not denormalize this object(as in add redundant information that can be inferred from fields) -
    it is used to denote the existence of a relation with no well-defined schema, in which fields would be empty.
    """

    fields: List[SchemaField] = attr.ib()
    relation_ids: Set[str] = attr.ib()

    @classmethod
    def empty_schema(cls) -> "Schema":
        return cls(fields=[], relation_ids=set())

    def merge(self, another_schema: "Schema") -> "Schema":
        # TODO: Should this be deep copies? I don't see a point in trying to deepcopy relations since they're expensive.
        fields = copy.copy(self.fields)
        relation_ids = copy.copy(self.relation_ids)
        # TODO: This is also pretty slow, O(N^2)
        for f in another_schema.fields:
            if f not in fields:
                fields.append(f)
        relation_ids |= another_schema.relation_ids
        return Schema(fields, relation_ids)


def maybe_prune_schema(relation: "Relation", existing_schema: Schema) -> Schema:
    """Maybe prune the schema to pass to a relation.
    Some relations don't want extra schema information to be passed through, because it's in its own parenthesized
    expression which is not aware of the outer namespace.
    """
    if isinstance(relation, Query):
        return Schema.empty_schema()
    return existing_schema


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

    def identifier(self) -> Optional[str]:
        """TODO: This identifier function is used to check for whether dereferences in fields correspond to a relation,
        but there are multiple ways we can dereference. catalog.schema.table.field is a valid identifier, not only
        table.field, which identifier would return "table" for and match on.
        """
        return None

    @abstractmethod
    def resolve(self, existing_schema: Schema) -> Schema:
        """Used to resolve missing types in queries. The user can pass in existing_schemas to hint at what types of
        fields are when they're dynamically determined i.e. a table in Trino, but we also use resolve() underneath
        the hood in order to resolve some types that can only be determined through a tree traversal i.e. fields to
        relations whose schemas are known"""
        ...


@attr.s
class Query(Relation, Value, ABC):
    """Represents a query with filtered outputs. Queries are also values, in that they yield row types
    """

    # TODO: Technically, an offset can either be an integer or a question mark(as a parameter).
    offset: Optional[int] = attr.ib(default=None, kw_only=True)
    limit: Optional[int] = attr.ib(default=None, kw_only=True)
    orderby: Optional[List[OrderTerm]] = attr.ib(default=None, kw_only=True)
    with_: List["AliasedRelation"] = attr.ib(factory=dict, kw_only=True)

    def with_query_string_builder(self, opts: PrintOptions) -> Dict[str, str]:
        if not self.with_:
            return {}
        # NOTE: We hold AliasedRelations in self.with_, but the sql output for it should be in
        # cte namedQuery form - it's not the same as a typical AliasedRelation.sql().
        return {
            "WITH": join_stmts(
                [query.named_query_sql(opts) for query in self.with_], opts
            )
        }

    def resolve_with(self) -> Schema:
        # The CTE's can actually refer to previously defined CTE's!
        merging_schema = Schema.empty_schema()
        for w in self.with_:
            new_schema = w.resolve(merging_schema)
            merging_schema = merging_schema.merge(new_schema)
        return merging_schema

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
        self.data_type = self._compute_data_type()

    def _compute_data_type(self) -> DataType:
        return row(dtypes=[val.data_type for val in self.select])

    def resolve(self, existing_schema: Schema) -> Schema:
        from treeno.datatypes.resolve import resolve_fields

        # TODO: Currently resolve only handles a single layer. We should make this a common function across all
        # relations so we can recursively call resolve before performing resolve on this query.
        # Also, it's probably worth allowing table inputs.
        # Pass in existing relations from CTE into from_ as long as from_ is not in its own namespace i.e. a subquery.
        if self.from_ is not None:
            self.from_.resolve(maybe_prune_schema(self.from_, existing_schema))
            schema = self.from_.resolve(existing_schema=self.resolve_with())
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


@attr.s
class Table(Relation):
    """A table reference uniquely identified by a qualified name

    Tables can be standalone queries by themselves (in the form TABLE {table name}), and thus
    can be subject to orderby, offset, and limit constraints. Refer to TableQuery for more information.
    """

    name: str = attr.ib()
    schema: Optional[str] = attr.ib(default=None)
    catalog: Optional[str] = attr.ib(default=None)

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


@attr.s
class TableQuery(Query):
    table: Table = attr.ib()

    def __attrs_post_init__(self) -> None:
        self.data_type = self._compute_data_type()

    def _compute_data_type(self) -> DataType:
        if self.table._column_schema == Schema.empty_schema():
            return row(dtypes=[f.data_type for f in self.table._column_schema])
        return unknown()

    def sql(self, opts: PrintOptions) -> str:
        builder = StatementPrinter()
        builder.update(self.with_query_string_builder(opts))
        builder.add_entry("TABLE", self.table.sql(opts))
        builder.update(self.constraint_string_builder(opts))
        return builder.to_string(opts)

    def resolve(self, existing_schema: Schema) -> Schema:
        # We not only need to take the existing schema, but we also need to shadow some fields with the
        # local WITH clause
        with_schema = self.resolve_with()
        table_schema = self.table.resolve(with_schema)
        self.data_type = self._compute_data_type()
        return table_schema


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

    def resolve(self, existing_schema: Schema) -> Schema:
        """TODO: Does ValuesQuery dereference anything by itself?
        """
        return Schema(
            [SchemaField(None, self, val.data_type) for val in self.exprs]
        )


@attr.s
class AliasedRelation(Relation):
    """Represents an alias corresponding to a relation
    """

    relation: Relation = attr.ib()
    alias: str = attr.ib()
    column_aliases: Optional[List[str]] = attr.ib(default=None)

    def sql(self, opts: PrintOptions) -> str:
        # TODO: Keep the "AS"?
        alias_str = f"{relation_string(self.relation, opts)} {quote_identifier(self.alias)}"
        if self.column_aliases:
            alias_str += f" ({join_stmts(self.column_aliases, opts)})"
        return alias_str

    def named_query_sql(self, opts: PrintOptions) -> str:
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

    def resolve(self, existing_schema: Schema) -> Schema:
        # NOTE: We don't maybe_prune_schema here, because table sample should not affect the namespace scope
        return self.relation.resolve(existing_schema)


def relation_string(relation: Relation, opts: PrintOptions) -> str:
    relation_str = relation.sql(opts)
    if isinstance(relation, Query):
        if opts.mode == PrintMode.PRETTY:
            relation_str = "\n" + relation_str
        return parenthesize(relation_str)
    else:
        return relation_str
