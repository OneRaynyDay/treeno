from abc import ABC, abstractmethod
from typing import TypeVar, List, Optional
import attr
from treeno.util import chain_identifiers, quote_literal, quote_identifier

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

    def __init__(self):
        # TODO: We don't have data type inference support yet
        self.data_type = None

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

    def __init__(self, *expressions):
        super().__init__()
        # Note that expressions may contain non-Value elements, like QueryBuilder.
        self.expressions = expressions


class Literal(Value):
    def __init__(self, value) -> None:
        super().__init__()
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

    def __init__(self, name=None, table=None):
        super().__init__()
        # None if we're selecting all columns from a source
        self.name = name
        # None if we're not referencing any underlying tables
        self.table = table

    def __str__(self) -> str:
        return chain_identifiers(self.table, self.name)


class AliasedValue(Value):
    """Represents one or more aliases corresponding to a value
    An example of where a value can have multiple aliases, consider `*`
    unpacking into a list of aliases.
    TODO: Implement multiple aliasing
    """

    def __init__(self, value, alias):
        super().__init__()
        self.value = value
        self.alias = alias

    def __str__(self):
        return f'{self.value} "{self.alias}"'


class Star(Field):
    """Represents a `*` or a `table.*` statement"""

    def __init__(self, table=None):
        super().__init__(table=table)

    def __str__(self):
        star_string = f"{quote_identifier(self.table)}." if self.table else ""
        star_string += "*"
        return star_string


def wrap_literal(val):
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


def call_str(function_name, *expressions):
    arg_str = ", ".join([str(expr) for expr in expressions])
    return f"{function_name}({arg_str})"


@attr.s
class BinaryExpression(Expression, ABC):
    left: GenericValue = attr.ib(converter=wrap_literal)
    right: GenericValue = attr.ib(converter=wrap_literal)


@attr.s
class UnaryExpression(Expression, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)


class Add(BinaryExpression):
    def __str__(self):
        # TODO: Just being pedantic right now. We can figure out the
        # minimal parenthesizing later.
        return f"({self.left}) + ({self.right})"


class Minus(BinaryExpression):
    def __str__(self):
        return f"({self.left}) - ({self.right})"


class Positive(UnaryExpression):
    def __str__(self):
        return f"+({self.value})"


class Negative(UnaryExpression):
    def __str__(self):
        return f"-({self.value})"


class Multiply(BinaryExpression):
    def __str__(self):
        return f"({self.left}) * ({self.right})"


class Divide(BinaryExpression):
    def __str__(self):
        return f"({self.left}) / ({self.right})"


class Not(UnaryExpression):
    def __str__(self):
        # TODO: If the underlying expression is IsNull(),
        # then turn this into {value} IS NOT NULL
        # This also applies to many other expressions like
        # DistinctFrom, Between, InList, etc. (all under the
        # grammar rule predicate)
        return f"NOT ({self.value})"


class Power(BinaryExpression):
    def __str__(self):
        return call_str("POWER", self.left, self.right)


class Modulus(BinaryExpression):
    def __str__(self):
        return f"({self.left}) % ({self.right})"


class Equal(BinaryExpression):
    def __str__(self):
        return f"({self.left}) = ({self.right})"


class NotEqual(BinaryExpression):
    def __str__(self):
        return f"({self.left}) <> ({self.right})"


class GreaterThan(BinaryExpression):
    def __str__(self):
        return f"({self.left}) > ({self.right})"


class GreaterThanOrEqual(BinaryExpression):
    def __str__(self):
        return f"({self.left}) >= ({self.right})"


class LessThan(BinaryExpression):
    def __str__(self):
        return f"({self.left}) < ({self.right})"


class LessThanOrEqual(BinaryExpression):
    def __str__(self):
        return f"({self.left}) <= ({self.right})"


class And(BinaryExpression):
    def __str__(self):
        return f"({self.left}) AND ({self.right})"


class Or(BinaryExpression):
    def __str__(self):
        return f"({self.left}) OR ({self.right})"


class IsNull(UnaryExpression):
    def __str__(self):
        return f"({self.value}) IS NULL"


class DistinctFrom(BinaryExpression):
    def __str__(self):
        return f"({self.left}) IS DISTINCT FROM ({self.right})"


@attr.s
class Between(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    lower: GenericValue = attr.ib(converter=wrap_literal)
    upper: GenericValue = attr.ib(converter=wrap_literal)

    def __str__(self):
        return f"{self.value} BETWEEN {self.lower} AND {self.upper}"


@attr.s
class InList(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    exprs: List[GenericValue] = attr.ib(converter=wrap_literal)

    def __str__(self):
        expr_list = ",".join(str(expr) for expr in self.exprs)
        return f"{self.value} IN ({expr_list})"


@attr.s
class Like(Expression):
    value: GenericValue = attr.ib(converter=wrap_literal)
    pattern: GenericValue = attr.ib(converter=wrap_literal)
    escape: Optional[GenericValue] = attr.ib(
        converter=attr.converters.optional(wrap_literal)
    )

    def __str__(self):
        like_str = f"{self.value} LIKE {self.pattern}"
        if self.escape:
            like_str += f" ESCAPE {self.escape}"
        return like_str


@attr.s
class Cast(Expression):
    expr: GenericValue = attr.ib(converter=wrap_literal)
    type: GenericValue = attr.ib(converter=wrap_literal)

    def __str__(self):
        return f"CAST({self.expr} AS {self.type})"


@attr.s
class TryCast(Expression):
    expr: GenericValue = attr.ib(converter=wrap_literal)
    type: GenericValue = attr.ib(converter=wrap_literal)

    def __str__(self):
        return f"TRY_CAST({self.expr} AS {self.type})"
