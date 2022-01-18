"""
Treeno supports arbitrary expressions including arithmetic binary, arithmetic unary, function, boolean expressions and
more. Underneath the hood, the values to be expressed in SQL are actually nodes of a tree and the tree is traversed to
generate SQL for execution. The base class which supports a suite of syntactic sugar such as ``__add__``, ``__eq__`` and
is :class:`Value`.

Here's an example using the CLI tool:

.. code-block:: bash

    ❯ treeno tree expression "1+2*3"
                                         Add
         _________________________________|__________________________
        |                  |                                       right
        |                  |                                         |
        |                  |                                      Multiply
        |                  |               __________________________|__________________
        |                 left            |                 left                      right
        |                  |              |                  |                          |
        |               Literal           |               Literal                    Literal
        |          ________|______        |          ________|_______           ________|______
    data_type data_type         value data_type data_type          value   data_type         value
        |         |               |       |         |                |         |               |
     INTEGER   INTEGER            1    INTEGER   INTEGER             2      INTEGER            3

A simple multiply and add is represented as nodes in a tree, each node having a data type (here, all of them are
INTEGER).

A myriad of python data types are supported to convert to :class:`Literal` nodes using :func:`wrap_literal`. We even
support ``decimal.Decimal`` types for fixed precision decimal point classes in python. A notable exception is NoneType,
which cannot be wrapped to a literal. Instead, please use the :data:`NULL` singleton.
"""
import functools
from abc import ABC
from decimal import Decimal
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import attr

# NOTE: This is done so sphinx-autodoc-typehints doesn't run into a circular import issue with InQuery.
import treeno
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

GenericValue = TypeVar("GenericValue", bound="Value")

# Attr tries to assign __le__, __ge__, __eq__ and friends by default. We define our own.
value_attr = functools.partial(attr.s, order=False, eq=False, str=False)


@value_attr
class Value(Sql, ABC):
    """Represents a basic SQL value.

    A value can be one of the following:

    1. (:class:`Literal`) A literal value with well-defined type
    2. (:class:`Field`) A reference to a field in a table, which doesn't always have a well-defined type before
        resolution.
    3. (:class:`Star`) A reference to ALL of the fields in a table.
    4. (:class:`AliasedValue` and :class:`AliasedStar`) Aliased versions of (2) and (3).
    5. (:class:`Expression`) A nested complex expression involving any :class:`Value`. This can be a
        built-in operator such as +, a lambda expression, a complex function involving potentially variadic arguments
        as seen in :mod:`treeno.functions` inheriting from :class:`treeno.functions.base.Function`, etc.

    Attributes:
        data_type: The data type of the value. If not specified, defaults to UNKNOWN, which means the data type can't
            be directly determined.
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

    def identifier(self) -> Optional[str]:
        """Identifier for the value.

        Values if aliased can have identifiers which are useful to capture output information. By default,
        :class:`Value` s are not identifiable by themselves.

        Returns:
            String name for the value's identifier if one exists, otherwise None.
        """
        return None


@value_attr
class Expression(Value, ABC):
    """Represents a complex expression which involves a function and its corresponding arguments.

    Expressions can be grouped into two general groups - :class:`treeno.functions.base.Function` s and built-in SQL operators. For all
    functions, please refer to the module :mod:`treeno.functions`. Operators live in `treeno.expression`, and support
    basic arihmetic scalar transforms such as add, subtract, divide, etc. and boolean clauses such as LIKE, IS NULL,
    DISTINCT FROM, etc.
    """


@value_attr
class Literal(Value):
    """Represents a literal value in SQL.

    Literal values are expressions that don't require calling constructors to any SQL data types such as ARRAY,
    and have a known data type (with the exception of NULL).

    >>> from decimal import Decimal
    >>> # Don't directly create a Literal value
    >>> Literal("a").data_type
    Traceback (most recent call last):
        ...
    AssertionError: Please use wrap_literal to construct a Literal value so the data types are detected
    >>> print(wrap_literal("a").data_type)
    VARCHAR(1)
    >>> print(wrap_literal(True).data_type)
    BOOLEAN
    >>> print(wrap_literal(1).data_type)
    INTEGER
    >>> print(wrap_literal(100000000000).data_type)
    BIGINT
    >>> print(wrap_literal(2.2).data_type)
    DOUBLE
    >>> # Use decimal.Decimal if you wish to represent a fixed precision decimal point in SQL
    >>> print(wrap_literal(Decimal("2.2")).data_type)
    DECIMAL(2,1)

    Attributes:
        value: A value in python representing a value in SQL.
    """

    value: Any = attr.ib()

    def __attrs_post_init__(self) -> None:
        if self.value is not None:
            assert (
                self.data_type != unknown()
            ), "Please use wrap_literal to construct a Literal value so the data types are detected"

    def sql(self, opts: PrintOptions) -> str:
        # NOTE: Literal decimals can be directly convertible
        # through string representation, i.e. 3.14 is DECIMAL(3,2), so we don't need to do anything.
        s = str(self.value)
        if self.value is None:
            s = "NULL"
        if isinstance(self.value, bool):
            s = s.upper()
        if isinstance(self.value, str):
            # Single quotes to mean literal string
            s = quote_literal(self.value)
        return s


@value_attr
class Field(Value):
    """Represents a field referenced in the input relations of a SELECT query

    Attributes:
        name: The name of the field
        table: The source of the field. This is sometimes necessary when there are multiple relations with duplicate
            column names and the name is not sufficient to disambiguate the field.
    """

    name: str = attr.ib()
    table: Optional[Union[str, Value]] = attr.ib(default=None)

    def __attrs_post_init__(self):
        assert not isinstance(
            self.table, Field
        ), "Table must be either a complex expression or a string representing the table, not a field"

    def identifier(self) -> Optional[str]:
        return self.name

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

    def identifier(self) -> Optional[str]:
        return self.alias

    def sql(self, opts: PrintOptions) -> str:
        return f'{self.value.sql(opts)} "{self.alias}"'


@value_attr
class Star(Value):
    """Represents a `*` or a `table.*` statement

    Note:
        The reason Star does not inherit from Field is because a star has no name.
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

        def identifier(self) -> Optional[str]:
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
        raise ValueError(
            "Treeno explicitly disallows creating a None literal to avoid ambiguity. Please use treeno.expression.NULL instead."
        )
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
    query: "treeno.relation.Query" = attr.ib()

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

# Use this instead of wrap_literal(None), which we explicitly disallow
NULL = Literal(None, data_type=unknown())
