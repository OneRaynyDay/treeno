import functools
from abc import ABC
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import attr

from treeno.base import PrintMode, PrintOptions, Sql
from treeno.datatypes import types as type_consts
from treeno.datatypes.builder import (
    array,
    boolean,
    interval,
    row,
    time,
    timestamp,
    unknown,
)
from treeno.datatypes.conversions import get_arithmetic_type
from treeno.datatypes.inference import (
    infer_char,
    infer_decimal,
    infer_timelike_precision,
    infer_type,
)
from treeno.datatypes.types import DataType
from treeno.printer import join_stmts, pad
from treeno.util import (
    chain_identifiers,
    children,
    construct_container,
    is_dictlike,
    is_listlike,
    parenthesize,
    quote_literal,
)

if TYPE_CHECKING:
    from treeno.relation import Query

GenericValue = TypeVar("GenericValue", bound="Value")

# Attr tries to assign __le__, __ge__, __eq__ and friends by default. We define our own.
value_attr = functools.partial(attr.s, order=False, eq=False, str=False)


@value_attr
class Value(Sql, ABC):
    """A value can be one of the following:

    1. (Literal) A literal value with well-defined type
    2. (Field) A reference to a field in a table, which doesn't always have a well-defined type before resolution.
    3. (Expression) A nested complex expression involving any of (1), (2) and (3).
        3.5. (SubqueryExpression) A subquery reinterpreted as ROW's. This is considered an Expression as well.

    Every expression has a type, and elementary operations on expressions should be supported
    as syntactic sugar for Expression(<op>, operands...).
    """

    data_type: DataType = attr.ib(factory=unknown, kw_only=True)

    def __invert__(self):
        return Not(self)

    def __pos__(self):
        return Positive(self)

    def __neg__(self):
        return Negative(self)

    def __add__(self, other):
        return Add(self, other)

    def __sub__(self, other):
        return Minus(self, other)

    def __mul__(self, other):
        return Multiply(self, other)

    def __truediv__(self, other):
        return Divide(self, other)

    def __pow__(self, other):
        from treeno.functions.math import Power

        return Power(self, other)

    def __mod__(self, other):
        return Modulus(self, other)

    def __eq__(self, other):
        return Equal(self, other)

    def __ne__(self, other):
        return NotEqual(self, other)

    def __gt__(self, other):
        return GreaterThan(self, other)

    def __ge__(self, other):
        return GreaterThanOrEqual(self, other)

    def __lt__(self, other):
        return LessThan(self, other)

    def __le__(self, other):
        return LessThanOrEqual(self, other)

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)


@value_attr
class Expression(Value, ABC):
    """Represents a complex expression which involves a function and its corresponding
    arguments.
    """


@value_attr
class Literal(Value):
    value: Any = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        """
        TODO: The stringification is actively under development as we add more literal types
        """
        s = str(self.value)
        if self.value is None:
            s = "NULL"
        if isinstance(self.value, bool):
            s = s.upper()
        if isinstance(self.value, str):
            # Single quotes to mean literal string
            s = quote_literal(self.value)
        # NOTE: Literal decimals can be directly convertible
        # through string representation, i.e. 3.14 is DECIMAL(3,2)
        return s


@value_attr
class Field(Value):
    """Represents a field referenced in the input relations of a SELECT query"""

    name: str = attr.ib()
    table: Optional[Union[str, Value]] = attr.ib(default=None)

    def __attrs_post_init__(self):
        assert not isinstance(
            self.table, Field
        ), "Table must be either a complex expression or a string representing the table, not a field"

    def sql(self, opts: PrintOptions) -> str:
        table_sql = None
        if self.table:
            if isinstance(self.table, str):
                table_sql = self.table
            else:
                table_sql = self.table.sql(opts)
        return chain_identifiers(table_sql, self.name)


@value_attr
class AliasedValue(Value):
    """Represents an alias on a value. For unpacking individual column aliases
    from a star, see AliasedStar
    """

    value: Value = attr.ib()
    alias: str = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert not isinstance(
            self.value, Star
        ), "Stars cannot have aliases. Consider using AliasedStar"
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        return f'{self.value.sql(opts)} "{self.alias}"'


@value_attr
class Star(Value):
    """Represents a `*` or a `table.*` statement
    NOTE: The reason Star does not inherit from Field is because a star has no name.
    Fields must have a name, and allow an optional table identifier.
    """

    # TODO: It should be noted that if table is a Field, that is a field referring to a table, not a column.
    table: Optional[Union[str, Value]] = attr.ib(default=None)

    def __attrs_post_init__(self):
        assert not isinstance(
            self.table, Field
        ), "Table must be either a complex expression or a string representing the table, not a field"

    def sql(self, opts: PrintOptions) -> str:
        star_string = ""
        if self.table:
            if isinstance(self.table, str):
                table_sql = self.table
            else:
                table_sql = self.table.sql(opts)
            star_string = f"{table_sql}."
        star_string += "*"
        return star_string


@value_attr
class AliasedStar(Value):
    """Represents one or more aliases corresponding to an unpacked star
    """

    star: Star = attr.ib()
    aliases: List[str] = attr.ib()

    def __attrs_post_init__(self) -> None:
        self.data_type = self.star.data_type

    def sql(self, opts: PrintOptions) -> str:
        alias_str = join_stmts(self.aliases, opts)
        return f"{self.star.sql(opts)} ({alias_str})"


@value_attr
class Lambda(Expression):
    """Represents an anonymous function. This expression will currently have an unknown type because it can never be
    expressed as a standalone expression.
    """

    @value_attr
    class Variable(Value):
        name: str = attr.ib()

        def sql(self, opts: PrintOptions) -> str:
            return self.name

    inputs: List[Variable] = attr.ib()
    expr: Value = attr.ib()

    def __attrs_post_init__(self):
        assert len(set(input.name for input in self.inputs)) == len(self.inputs)

    @classmethod
    def from_generic_expr(
        cls, inputs: List[Variable], expr: GenericValue
    ) -> "Lambda":
        input_set = set(input.name for input in inputs)

        def _from_expr(node: Any) -> Any:
            if is_listlike(node):
                return construct_container(
                    node, iter(_from_expr(child) for child in node)
                )
            if is_dictlike(node):
                return construct_container(
                    node,
                    iter((k, _from_expr(child)) for k, child in node.items()),
                )
            if not isinstance(node, Sql):
                return node
            if (
                isinstance(node, Field)
                and node.table is None
                and node.name in input_set
            ):
                return cls.Variable(node.name)
            changes = {}
            for k, v in children(node).items():
                changes[k] = _from_expr(v)
            return attr.evolve(node, **changes)

        return cls(inputs, _from_expr(expr))

    def sql(self, opts: PrintOptions) -> str:
        input_string = join_stmts([var.sql(opts) for var in self.inputs], opts)
        if len(self.inputs) > 1:
            input_string = parenthesize(input_string)
        return f"{input_string} -> {self.expr.sql(opts)}"


def wrap_literal(val: Any) -> Value:
    """Convenience method to wrap a literal value into a treeno Value"""
    if isinstance(val, Value):
        return val
    if val is None:
        # return NullValue()
        raise NotImplementedError("Null type not supported yet")
    assert not isinstance(
        val, (list, tuple, set, dict)
    ), "wrap_literal should not be used with composable types like ARRAY/MAP/ROW"
    return Literal(val, data_type=infer_type(val))


def wrap_literal_list(vals: List[Any]) -> List[Value]:
    return [wrap_literal(val) for val in vals]


def pemdas_str(
    current_type: Type[Value],
    val: Value,
    opts: PrintOptions,
    is_left: bool = True,
) -> str:
    """Apply parenthesization onto the nested expression if required for pemdas.

    Args:
        current_type: Current value's type.
        val: One of the direct children of the current value.
        is_left: Whether this value is the leftmost child. This is used to determine whether the right child should
            be parenthesized or not. i.e. 1+(2+3) or 1+2+3.
    """
    # If the type is not specified, we assume the syntax is well-formed without any parentheses as it's probably a fn
    # call.
    val_type = type(val)
    if (
        current_type not in OPERATOR_PRECEDENCE
        or val_type not in OPERATOR_PRECEDENCE
    ):
        return val.sql(opts)
    current_precedence = OPERATOR_PRECEDENCE[current_type]
    val_precedence = OPERATOR_PRECEDENCE[val_type]
    # The expression's precedence is the same, so we must obey left-to-right ordering.
    if current_precedence == val_precedence:
        if is_left:
            return val.sql(opts)
        else:
            return parenthesize(val)

    # The underlying value's precedence is lower, BUT it's deeper in the tree, which means we need to parenthesize it.
    if val_precedence < current_precedence:
        return parenthesize(val)
    return val.sql(opts)


@value_attr
class BinaryExpression(Expression, ABC):
    left: Value = attr.ib(converter=wrap_literal)
    right: Value = attr.ib(converter=wrap_literal)


@value_attr
class UnaryExpression(Expression, ABC):
    value: Value = attr.ib(converter=wrap_literal)


def builtin_binary_str(
    val: BinaryExpression, string_format: str, opts: PrintOptions
) -> str:
    return string_format.format(
        left=pemdas_str(type(val), val.left, opts, is_left=True),
        right=pemdas_str(type(val), val.right, opts, is_left=False),
    )


def builtin_unary_str(
    val: UnaryExpression, string_format: str, opts: PrintOptions
) -> str:
    return string_format.format(
        value=pemdas_str(type(val), val.value, opts, is_left=True)
    )


@value_attr
class Positive(UnaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        return builtin_unary_str(self, "+{value}", opts)


@value_attr
class Negative(UnaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions) -> str:
        return builtin_unary_str(self, "-{value}", opts)


@value_attr
class Add(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = get_arithmetic_type(
            self.left.data_type, self.right.data_type
        )

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} + {right}", opts)


@value_attr
class Minus(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = get_arithmetic_type(
            self.left.data_type, self.right.data_type
        )
        # Special minus operators for datetimes
        left_name, right_name = (
            self.left.data_type.type_name,
            self.right.data_type.type_name,
        )
        if {left_name, right_name}.issubset(
            {type_consts.TIMESTAMP, type_consts.DATE}
        ) or left_name == right_name == type_consts.TIME:
            self.data_type = interval(from_interval="DAY", to_interval="SECOND")

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} - {right}", opts)


@value_attr
class Multiply(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = get_arithmetic_type(
            self.left.data_type, self.right.data_type
        )

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} * {right}", opts)


@value_attr
class Divide(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = get_arithmetic_type(
            self.left.data_type, self.right.data_type
        )

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} / {right}", opts)


@value_attr
class Not(UnaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        # Specializations on Not
        if isinstance(
            self.value,
            (
                DistinctFrom,
                IsNull,
                Like,
                InList,
                InQuery,
                Between,
                Equal,
                NotEqual,
            ),
        ):
            return self.value.to_string(opts, negate=True)

        return builtin_unary_str(self, "NOT {value}", opts)


@value_attr
class Modulus(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = get_arithmetic_type(
            self.left.data_type, self.right.data_type
        )

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} % {right}", opts)


@value_attr
class Equal(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        if negate:
            return NotEqual(self.left, self.right).sql(opts)
        return builtin_binary_str(self, "{left} = {right}", opts)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class NotEqual(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        if negate:
            return str(Equal(self.left, self.right))
        return builtin_binary_str(self, "{left} <> {right}", opts)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class GreaterThan(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} > {right}", opts)


@value_attr
class GreaterThanOrEqual(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} >= {right}", opts)


@value_attr
class LessThan(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} < {right}", opts)


@value_attr
class LessThanOrEqual(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} <= {right}", opts)


@value_attr
class And(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        # According to the specifications, AND and OR should be on the next line if we're in pretty mode
        spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
        return builtin_binary_str(
            self, "{left}" + spacing + "AND {right}", opts
        )


@value_attr
class Or(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
        return builtin_binary_str(self, "{left}" + spacing + "OR {right}", opts)


@value_attr
class IsNull(UnaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False):
        return builtin_unary_str(
            self, "{value} IS NOT NULL" if negate else "{value} IS NULL", opts
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class DistinctFrom(BinaryExpression):
    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False):
        return builtin_binary_str(
            self,
            "{left} IS NOT DISTINCT FROM {right}"
            if negate
            else "{left} IS DISTINCT FROM {right}",
            opts,
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class Between(Expression):
    value: Value = attr.ib(converter=wrap_literal)
    lower: Value = attr.ib(converter=wrap_literal)
    upper: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        between_string = pemdas_str(type(self), self.value, opts)
        if negate:
            between_string += " NOT"
        between_string += f" BETWEEN {pemdas_str(type(self), self.lower, opts)} AND {pemdas_str(type(self), self.upper, opts)}"
        return between_string

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class Array(Expression):
    values: List[Value] = attr.ib(converter=wrap_literal_list)

    def __attrs_post_init__(self) -> None:
        assert len(self.values), "values must be a non-empty list for Array"
        # TODO: This is currently incorrect. We have to widen the data type depending on the largest precision we have
        # i.e. TYPEOF(ARRAY[CHAR '3'] || '345') yields array(char(3))
        self.data_type = array(dtype=self.values[0].data_type)

    @classmethod
    def from_values(cls, *vals: Any) -> "Array":
        return cls(vals)

    def sql(self, opts: PrintOptions) -> str:
        values_str = join_stmts([val.sql(opts) for val in self.values], opts)
        return f"ARRAY[{values_str}]"


@value_attr
class InQuery(Expression):
    value: Value = attr.ib(converter=wrap_literal)
    query: "Query" = attr.ib()

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        in_list_string = pemdas_str(type(self), self.value, opts)
        if negate:
            in_list_string += " NOT"
        in_list_string += f" IN ({self.query.sql(opts)})"
        return in_list_string

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class InList(Expression):
    value: Value = attr.ib(converter=wrap_literal)
    exprs: List[Value] = attr.ib(converter=wrap_literal_list)

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        expr_list = join_stmts([expr.sql(opts) for expr in self.exprs], opts)
        in_list_string = pemdas_str(type(self), self.value, opts)
        if negate:
            in_list_string += " NOT"
        in_list_string += f" IN ({expr_list})"
        return in_list_string

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class Like(Expression):
    value: Value = attr.ib(converter=wrap_literal)
    pattern: Value = attr.ib(converter=wrap_literal)
    escape: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        like_str = pemdas_str(type(self), self.value, opts)
        if negate:
            like_str += " NOT"
        like_str += f" LIKE {pemdas_str(type(self), self.pattern, opts)}"
        if self.escape:
            # TODO: Should we pemdas this? I don't see it in the specs
            like_str += f" ESCAPE {self.escape.sql(opts)}"
        return like_str

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@value_attr
class TypeConstructor(Expression):
    value: str = attr.ib()
    type_name: str = attr.ib(converter=str.upper)

    def __attrs_post_init__(self) -> None:
        # This can't be timezoned, so we ignore that
        if self.type_name == type_consts.TIMESTAMP:
            self.data_type = timestamp(
                precision=infer_timelike_precision(self.value)
            )
        if self.type_name == type_consts.TIME:
            self.data_type = time(
                precision=infer_timelike_precision(self.value)
            )
        elif self.type_name == type_consts.DECIMAL:
            self.data_type = infer_decimal(Decimal(self.value))
        elif self.type_name == type_consts.CHAR:
            self.data_type = infer_char(self.value)
        else:
            self.data_type = DataType(self.type_name)

    def sql(self, opts: PrintOptions) -> str:
        # We aren't allowed to parametrize the types here.
        # There's an edge case where parametrizing timestamp with timezone=True is not in the parens notation
        # but that also fails for this(since the parser recognizes a single string as the type) which means we
        # can just take the type name.
        return f"{self.type_name} {quote_literal(self.value)}"


@value_attr
class RowConstructor(Expression):
    values: List[Value] = attr.ib()

    def __attrs_post_init__(self) -> None:
        self.data_type = row(dtypes=[value.data_type for value in self.values])

    def sql(self, opts: PrintOptions) -> str:
        values_string = join_stmts(
            [value.sql(opts) for value in self.values], opts
        )
        return f"({values_string})"


@value_attr
class Interval(Expression):
    value: str = attr.ib()
    from_interval: str = attr.ib()
    to_interval: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.from_interval in ("YEAR", "MONTH"):
            self.data_type = interval(from_interval="YEAR", to_interval="MONTH")
        else:
            assert self.from_interval in (
                "DAY",
                "HOUR",
                "MINUTE",
                "SECOND",
            ), f"Unknown interval type {self.from_interval}"
            self.data_type = interval(from_interval="DAY", to_interval="SECOND")

    def sql(self, opts: PrintOptions) -> str:
        to_interval_str = f" TO {self.to_interval}" if self.to_interval else ""
        return f"INTERVAL {self.value} {self.from_interval}" + to_interval_str


@value_attr
class Cast(Expression):
    expr: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        # TODO: We should probably just set init=False to attr.s and force them to input the data type as a positional
        # argument. This is a temporary workaround.
        assert self.data_type != unknown(), "data_type must be defined for Cast"

    def sql(self, opts: PrintOptions) -> str:
        return f"CAST({self.expr.sql(opts)} AS {self.data_type.sql(opts)})"


@value_attr
class TryCast(Expression):
    expr: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self):
        assert (
            self.data_type != unknown()
        ), "data_type must be defined for TryCast"

    def sql(self, opts: PrintOptions) -> str:
        return f"TRY_CAST({self.expr.sql(opts)} AS {self.data_type.sql(opts)})"


@value_attr
class Subscript(Expression):
    value: Value = attr.ib()
    index: Value = attr.ib(converter=wrap_literal)

    def sql(self, opts: PrintOptions) -> str:
        return f"{self.value.sql(opts)}[{self.index.sql(opts)}]"


# These are not values by themselves and must be used in the context of `Case`.
@attr.s
class When(Sql):
    condition: Value = attr.ib(converter=wrap_literal)
    value: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions):
        return f"WHEN {self.condition.sql(opts)} THEN {self.value.sql(opts)}"


@attr.s
class Else(Sql):
    value: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = self.value.data_type

    def sql(self, opts: PrintOptions):
        return f"ELSE {self.value.sql(opts)}"


@value_attr
class Case(Expression):
    branches: List[When] = attr.ib()
    else_: Optional[Else] = attr.ib(default=None)
    value: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        assert len(
            self.branches
        ), "There must be at least one WHEN condition in a CASE"
        self.data_type = self.branches[0].data_type

    def sql(self, opts: PrintOptions) -> str:
        spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
        conds = self.branches
        if self.else_:
            conds = conds + [self.else_]
        branches_string = pad(
            spacing + spacing.join([cond.sql(opts) for cond in conds]), 4
        )
        end_string = spacing + "END"
        value_string = self.value.sql(opts) if self.value is not None else ""
        return f"CASE {value_string}{branches_string}{end_string}"


# Operator precedence according to:
# https://docs.oracle.com/cd/B19306_01/server.102/b14200/operators001.htm
# https://docs.oracle.com/cd/B19306_01/server.102/b14200/conditions001.htm#i1034834
# Still waiting to hear back about Trino's custom precedence:
# https://trinodb.slack.com/archives/CFLB9AMBN/p1637528834018300
OPERATOR_PRECEDENCE: Dict[Type[Value], int] = {
    Positive: 8,
    Negative: 8,
    Multiply: 7,
    Divide: 7,
    Modulus: 7,
    Add: 6,
    Minus: 6,
    # TODO: Currently we run into an import loop trying to add Concatenate here, so we add it at the end in functions.common
    Equal: 4,
    NotEqual: 4,
    GreaterThan: 4,
    GreaterThanOrEqual: 4,
    LessThan: 4,
    LessThanOrEqual: 4,
    IsNull: 3,
    Like: 3,
    Between: 3,
    InList: 3,
    InQuery: 3,
    Not: 2,
    And: 1,
    Or: 0,
}
