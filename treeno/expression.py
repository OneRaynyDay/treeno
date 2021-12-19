from abc import ABC, abstractmethod
from typing import TypeVar, List, Optional, Type, Any
import attr
from treeno.datatypes.types import DataType
from treeno.util import (
    chain_identifiers,
    quote_literal,
    quote_identifier,
    parenthesize,
)

GenericValue = TypeVar("GenericValue", bound="Value")


class Value(ABC):
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

    @abstractmethod
    def __str__(self):
        raise NotImplementedError("All values must implement __str__")

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

    def __str__(self) -> str:
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
        return s


class Field(Value):
    """Represents a field referenced in the input relations of a SELECT query"""

    def __init__(self, name: str, table: Optional[str] = None):
        super().__init__()
        # None if we're selecting all columns from a source
        self.name = name
        # None if we're not referencing any underlying tables
        self.table = table

    def __str__(self) -> str:
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

    def __str__(self) -> str:
        return f'{self.value} "{self.alias}"'


class Star(Value):
    """Represents a `*` or a `table.*` statement
    NOTE: The reason Star does not inherit from Field is because a star has no name.
    Fields must have a name, and allow an optional table identifier.
    """

    def __init__(self, table: Optional[str] = None):
        super().__init__()
        self.table = table

    def __str__(self):
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

    def __str__(self) -> str:
        alias_str = ",".join(self.aliases)
        return f"{super().__str__()} ({alias_str})"


def wrap_literal(val: Any) -> Value:
    """Convenience method to wrap a literal value into a treeno Value"""
    if isinstance(val, Value):
        return val
    if val is None:
        # return NullValue()
        raise NotImplementedError("Null type not supported yet")
    if isinstance(val, list):
        # return Array(*val)
        raise NotImplementedError("Array types not supported yet")
    if isinstance(val, tuple):
        # return Tuple(*val)
        raise NotImplementedError("Tuple types not supported yet")
    return Literal(val)


def wrap_literal_list(vals: List[Any]) -> List[Value]:
    return [wrap_literal(val) for val in vals]


def pemdas_str(
    current_type: Type[Value], val: Value, is_left: bool = True
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
        return str(val)
    current_precedence = OPERATOR_PRECEDENCE[current_type]
    val_precedence = OPERATOR_PRECEDENCE[val_type]
    # The expression's precedence is the same, so we must obey left-to-right ordering.
    if current_precedence == val_precedence:
        if is_left:
            return str(val)
        else:
            return parenthesize(val)

    # The underlying value's precedence is lower, BUT it's deeper in the tree, which means we need to parenthesize it.
    if val_precedence < current_precedence:
        return parenthesize(val)
    return str(val)


def builtin_binary_str(val: Value, string_format: str) -> str:
    return string_format.format(
        left=pemdas_str(type(val), val.left, is_left=True),
        right=pemdas_str(type(val), val.right, is_left=False),
    )


def builtin_unary_str(val: Value, string_format: str) -> str:
    return string_format.format(
        value=pemdas_str(type(val), val.value, is_left=True)
    )


def call_str(function_name, *expressions):
    arg_str = ", ".join([str(expr) for expr in expressions])
    return f"{function_name}{parenthesize(arg_str)}"


@attr.s
class BinaryExpression(Expression, ABC):
    left: GenericValue = attr.ib(converter=wrap_literal)
    right: GenericValue = attr.ib(converter=wrap_literal)


@attr.s
class UnaryExpression(Expression, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)


class Positive(UnaryExpression):
    def __str__(self):
        return builtin_unary_str(self, "+{value}")


class Negative(UnaryExpression):
    def __str__(self):
        return builtin_unary_str(self, "-{value}")


class Add(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} + {right}")


class Minus(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} - {right}")


class Multiply(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} * {right}")


class Divide(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} / {right}")


class Not(UnaryExpression):
    def __str__(self):
        # Specializations on Not
        if isinstance(
            self.value,
            (DistinctFrom, IsNull, Like, InList, Between, Equal, NotEqual),
        ):
            return self.value.to_string(negate=True)

        return builtin_unary_str(self, "NOT {value}")


class Power(BinaryExpression):
    def __str__(self):
        return call_str("POWER", self.left, self.right)


class Modulus(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} % {right}")


class Equal(BinaryExpression):
    def to_string(self, negate: bool = False) -> str:
        if negate:
            return str(NotEqual(self.left, self.right))
        return builtin_binary_str(self, "{left} = {right}")

    def __str__(self):
        return self.to_string(negate=False)


class NotEqual(BinaryExpression):
    def to_string(self, negate: bool = False) -> str:
        if negate:
            return str(Equal(self.left, self.right))
        return builtin_binary_str(self, "{left} <> {right}")

    def __str__(self):
        return self.to_string(negate=False)


class GreaterThan(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} > {right}")


class GreaterThanOrEqual(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} >= {right}")


class LessThan(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} < {right}")


class LessThanOrEqual(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} <= {right}")


class And(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} AND {right}")


class Or(BinaryExpression):
    def __str__(self):
        return builtin_binary_str(self, "{left} OR {right}")


class IsNull(UnaryExpression):
    def to_string(self, negate: bool = False):
        return builtin_unary_str(
            self, "{value} IS NOT NULL" if negate else "{value} IS NULL"
        )

    def __str__(self):
        return self.to_string(negate=False)


class DistinctFrom(BinaryExpression):
    def to_string(self, negate: bool = False):
        return builtin_unary_str(
            self,
            "{left} IS NOT DISTINCT FROM {right}"
            if negate
            else "{left} IS DISTINCT FROM {right}",
        )

    def __str__(self):
        return self.to_string(negate=False)


@attr.s
class Between(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    lower: GenericValue = attr.ib(converter=wrap_literal)
    upper: GenericValue = attr.ib(converter=wrap_literal)

    def to_string(self, negate: bool = False) -> str:
        between_string = pemdas_str(type(self), self.value)
        if negate:
            between_string += " NOT"
        between_string += f" BETWEEN {pemdas_str(type(self), self.lower)} AND {pemdas_str(type(self), self.upper)}"
        return between_string

    def __str__(self) -> str:
        return self.to_string(negate=False)


@attr.s
class Array(Expression):
    values: List[GenericValue] = attr.ib(converter=wrap_literal_list)

    @classmethod
    def from_values(cls, *vals: Any) -> "Array":
        return cls(vals)

    def __str__(self) -> str:
        values_str = ",".join(str(val) for val in self.values)
        return f"ARRAY[{values_str}]"


@attr.s
class InList(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    exprs: List[GenericValue] = attr.ib(converter=wrap_literal_list)

    def to_string(self, negate: bool = False) -> str:
        expr_list = ",".join(str(expr) for expr in self.exprs)
        in_list_string = pemdas_str(type(self), self.value)
        if negate:
            in_list_string += " NOT"
        in_list_string += f" IN ({expr_list})"
        return in_list_string

    def __str__(self) -> str:
        return self.to_string(negate=False)


@attr.s
class Like(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    pattern: GenericValue = attr.ib(converter=wrap_literal)
    escape: Optional[GenericValue] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def to_string(self, negate: bool = False) -> str:
        like_str = pemdas_str(type(self), self.value)
        if negate:
            like_str += " NOT"
        like_str += f" LIKE {pemdas_str(type(self), self.pattern)}"
        if self.escape:
            # TODO: Should we pemdas this? I don't see it in the specs
            like_str += f" ESCAPE {self.escape}"
        return like_str

    def __str__(self) -> str:
        return self.to_string(negate=False)


@attr.s
class Cast(Expression):
    expr: GenericValue = attr.ib(converter=wrap_literal)
    type: DataType = attr.ib()

    def __str__(self) -> str:
        return f"CAST({self.expr} AS {self.type})"


@attr.s
class TryCast(Expression):
    expr: GenericValue = attr.ib(converter=wrap_literal)
    type: DataType = attr.ib()

    def __str__(self) -> str:
        return f"TRY_CAST({self.expr} AS {self.type})"


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
