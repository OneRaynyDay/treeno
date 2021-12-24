from abc import ABC
from decimal import Decimal
from typing import TypeVar, List, Optional, Type, Any
import attr
from treeno.datatypes.types import DataType
from treeno.datatypes.inference import infer_type
from treeno.base import Sql, PrintOptions, PrintMode
from treeno.printer import join_stmts
from treeno.util import (
    chain_identifiers,
    quote_literal,
    quote_identifier,
    parenthesize,
)

GenericValue = TypeVar("GenericValue", bound="Value")


class Value(Sql, ABC):
    """A value can be one of the following:

    1. (Literal) A literal value with well-defined type
    2. (Field) A reference to a field in a table, which doesn't always have a well-defined type before resolution.
    3. (Expression) A nested complex expression involving any of (1), (2) and (3).
        3.5. (SubqueryExpression) A subquery reinterpreted as ROW's. This is considered an Expression as well.

    Every expression has a type, and elementary operations on expressions should be supported
    as syntactic sugar for Expression(<op>, operands...).
    """

    def __init__(self, data_type: Optional[DataType] = None):
        # TODO: We don't have full data type inference support yet
        self.data_type = data_type

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


class Expression(Value, ABC):
    """Represents a complex expression which involves a function and its corresponding
    arguments.
    """

    def __init__(self):
        super().__init__()


class Literal(Value):
    def __init__(self, value: Any, data_type: DataType) -> None:
        super().__init__(data_type)
        self.value = value
        # Literals can't be NaN or Infinity by definition.

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
        if isinstance(self.value, Decimal):
            # Literal decimals can only be directly convertible through cast via string representation
            s = Cast(s, self.data_type).sql(opts)
        return s


class Field(Value):
    """Represents a field referenced in the input relations of a SELECT query"""

    def __init__(self, name: str, table: Optional[str] = None):
        super().__init__()
        # None if we're selecting all columns from a source
        self.name = name
        # None if we're not referencing any underlying tables
        self.table = table

    def sql(self, opts: PrintOptions) -> str:
        return chain_identifiers(self.table, self.name)


class AliasedValue(Value):
    """Represents an alias on a value. For unpacking individual column aliases
    from a star, see AliasedStar
    """

    def __init__(self, value: Value, alias: str):
        super().__init__()
        assert not isinstance(
            value, Star
        ), "Stars cannot have aliases. Consider using AliasedStar"
        self.value = value
        self.alias = alias

    def sql(self, opts: PrintOptions) -> str:
        return f'{self.value} "{self.alias}"'


class Star(Value):
    """Represents a `*` or a `table.*` statement
    NOTE: The reason Star does not inherit from Field is because a star has no name.
    Fields must have a name, and allow an optional table identifier.
    """

    def __init__(self, table: Optional[str] = None):
        super().__init__()
        self.table = table

    def sql(self, opts: PrintOptions) -> str:
        star_string = f"{quote_identifier(self.table)}." if self.table else ""
        star_string += "*"
        return star_string


class AliasedStar(Star):
    """Represents one or more aliases corresponding to an unpacked star
    """

    def __init__(self, table: str, aliases: List[str]):
        super().__init__(table=table)
        assert (
            self.table is not None
        ), "Stars without a table cannot have column aliases"
        self.aliases = aliases

    def sql(self, opts: PrintOptions) -> str:
        alias_str = ",".join(self.aliases)
        return f"{super().__str__()} ({alias_str})"


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
    return Literal(val, infer_type(val))


def wrap_literal_list(vals: List[Any]) -> List[Value]:
    return [wrap_literal(val) for val in vals]


def pemdas_str(
    current_type: Type[Value],
    val: Value,
    opts: [PrintOptions],
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


def builtin_binary_str(
    val: Value, string_format: str, opts: PrintOptions
) -> str:
    return string_format.format(
        left=pemdas_str(type(val), val.left, opts, is_left=True),
        right=pemdas_str(type(val), val.right, opts, is_left=False),
    )


def builtin_unary_str(
    val: Value, string_format: str, opts: PrintOptions
) -> str:
    return string_format.format(
        value=pemdas_str(type(val), val.value, opts, is_left=True)
    )


def call_str(
    function_name: str, opts: PrintOptions, *expressions: GenericValue
):
    arg_str = ", ".join([expr.sql(opts) for expr in expressions])
    return f"{function_name}{parenthesize(arg_str)}"


@attr.s
class BinaryExpression(Expression, ABC):
    left: GenericValue = attr.ib(converter=wrap_literal)
    right: GenericValue = attr.ib(converter=wrap_literal)


@attr.s
class UnaryExpression(Expression, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)


class Positive(UnaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_unary_str(self, "+{value}", opts)


class Negative(UnaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_unary_str(self, "-{value}", opts)


class Add(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} + {right}", opts)


class Minus(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} - {right}", opts)


class Multiply(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} * {right}", opts)


class Divide(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} / {right}", opts)


class Not(UnaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        # Specializations on Not
        if isinstance(
            self.value,
            (DistinctFrom, IsNull, Like, InList, Between, Equal, NotEqual),
        ):
            return self.value.to_string(opts, negate=True)

        return builtin_unary_str(self, "NOT {value}", opts)


class Power(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return call_str("POWER", self.left, self.right)


class Modulus(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} % {right}", opts)


class Equal(BinaryExpression):
    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        if negate:
            return NotEqual(self.left, self.right).sql(opts)
        return builtin_binary_str(self, "{left} = {right}", opts)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


class NotEqual(BinaryExpression):
    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        if negate:
            return str(Equal(self.left, self.right))
        return builtin_binary_str(self, "{left} <> {right}", opts)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


class GreaterThan(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} > {right}", opts)


class GreaterThanOrEqual(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} >= {right}", opts)


class LessThan(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} < {right}", opts)


class LessThanOrEqual(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        return builtin_binary_str(self, "{left} <= {right}", opts)


class And(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        # According to the specifications, AND and OR should be on the next line if we're in pretty mode
        spacing = "\n" if opts.mode == PrintMode.PRETTY else " "
        return builtin_binary_str(
            self, "{left}" + spacing + "AND {right}", opts
        )


class Or(BinaryExpression):
    def sql(self, opts: PrintOptions) -> str:
        spacing = "\n" if opts.mode == PrintMode.PRETTY else ""
        return builtin_binary_str(self, "{left}" + spacing + "OR {right}", opts)


class IsNull(UnaryExpression):
    def to_string(self, opts: PrintOptions, negate: bool = False):
        return builtin_unary_str(
            self, "{value} IS NOT NULL" if negate else "{value} IS NULL", opts
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


class DistinctFrom(BinaryExpression):
    def to_string(self, opts: PrintOptions, negate: bool = False):
        return builtin_unary_str(
            self,
            "{left} IS NOT DISTINCT FROM {right}"
            if negate
            else "{left} IS DISTINCT FROM {right}",
            opts,
        )

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@attr.s
class Between(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    lower: GenericValue = attr.ib(converter=wrap_literal)
    upper: GenericValue = attr.ib(converter=wrap_literal)

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        between_string = pemdas_str(type(self), self.value, opts)
        if negate:
            between_string += " NOT"
        between_string += f" BETWEEN {pemdas_str(type(self), self.lower, opts)} AND {pemdas_str(type(self), self.upper, opts)}"
        return between_string

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@attr.s
class Array(Expression):
    values: List[GenericValue] = attr.ib(converter=wrap_literal_list)

    @classmethod
    def from_values(cls, *vals: Any) -> "Array":
        return cls(vals)

    def sql(self, opts: PrintOptions) -> str:
        values_str = join_stmts([val.sql(opts) for val in self.values], opts)
        return f"ARRAY[{values_str}]"


@attr.s
class InList(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    exprs: List[GenericValue] = attr.ib(converter=wrap_literal_list)

    def to_string(self, opts: PrintOptions, negate: bool = False) -> str:
        expr_list = join_stmts([expr.sql(opts) for expr in self.exprs], opts)
        in_list_string = pemdas_str(type(self), self.value, opts)
        if negate:
            in_list_string += " NOT"
        in_list_string += f" IN ({expr_list})"
        return in_list_string

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string(opts, negate=False)


@attr.s
class Like(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    pattern: GenericValue = attr.ib(converter=wrap_literal)
    escape: Optional[GenericValue] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

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


@attr.s
class TypeConstructor(Expression):
    value: str = attr.ib()
    type: DataType = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        # We aren't allowed to parametrize the types here.
        # There's an edge case where parametrizing timestamp with timezone=True is not in the parens notation
        # but that also fails for this(since the parser recognizes a single string as the type) which means we
        # can just take the type name.
        return f"{self.type.type_name} {quote_literal(self.value)}"


@attr.s
class RowConstructor(Expression):
    values: List[Value] = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        values_string = join_stmts(
            [value.sql(opts) for value in self.values], opts
        )
        return f"({values_string})"


@attr.s
class Interval(Expression):
    value: str = attr.ib()
    from_interval: str = attr.ib()
    to_interval: Optional[str] = attr.ib(default=None)

    def sql(self, opts: PrintOptions) -> str:
        to_interval_str = f" TO {self.to_interval}" if self.to_interval else ""
        return f"INTERVAL {self.value} {self.from_interval}" + to_interval_str


@attr.s
class Cast(Expression):
    expr: GenericValue = attr.ib(converter=wrap_literal)
    type: DataType = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        return f"CAST({self.expr.sql(opts)} AS {self.type.sql(opts)})"


@attr.s
class TryCast(Expression):
    expr: GenericValue = attr.ib(converter=wrap_literal)
    type: DataType = attr.ib()

    def sql(self, opts: PrintOptions) -> str:
        return f"TRY_CAST({self.expr.sql(opts)} AS {self.type.sql(opts)})"


@attr.s
class Subscript(Expression):
    value: GenericValue = attr.ib()
    index: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self, opts: PrintOptions) -> str:
        return f"{self.value.sql(opts)}[{self.index.sql(opts)}]"


# Operator precedence according to:
# https://docs.oracle.com/cd/B19306_01/server.102/b14200/operators001.htm
# https://docs.oracle.com/cd/B19306_01/server.102/b14200/conditions001.htm#i1034834
# Still waiting to hear back about Trino's custom precedence:
# https://trinodb.slack.com/archives/CFLB9AMBN/p1637528834018300
OPERATOR_PRECEDENCE = {
    Positive: 7,
    Negative: 7,
    Multiply: 6,
    Divide: 6,
    Modulus: 6,
    Add: 5,
    Minus: 5,
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
    Not: 2,
    And: 1,
    Or: 0,
}
