"""
Treeno is built around the concept of creating python nodes that represent SQL constructs. These constructs must be
interoperable with SQL strings, which means:

1. The trees created by a hierarchy of nodes can produce valid SQL.
2. We can construct a Treeno tree from valid SQL.

In order to do this, this module introduces some preliminary classes for printing/formatting SQL and the base
:class:`Sql` superclass responsible for representing all SQL nodes.

.. todo:: We currently copy this description into api.rst, because we don't want to include all the
"""

from abc import ABC, ABCMeta, abstractmethod
from enum import Enum, EnumMeta, auto
from typing import Any, TypeVar

import attr

GenericEnum = TypeVar("GenericEnum", bound=Enum)


class ABCEnumMeta(EnumMeta, ABCMeta):
    """This is required because Enum classes have a different metaclass, so we must merge both enum and abc metaclasses
    into a single class"""

    ...


class DefaultableEnum(Enum, metaclass=ABCEnumMeta):
    @classmethod
    @abstractmethod
    def default(cls: GenericEnum) -> GenericEnum:
        raise NotImplementedError(
            f"All {cls.__name__} must implement default()"
        )


class PrintMode(DefaultableEnum):
    DEFAULT = auto()
    PRETTY = auto()

    @classmethod
    def default(cls: GenericEnum) -> GenericEnum:
        return cls.DEFAULT


@attr.s
class PrintOptions:
    mode: PrintMode = attr.ib(factory=PrintMode.default)
    spaces: int = attr.ib(default=4)


@attr.s
class Sql(ABC):
    """A base class for all SQL nodes in Treeno."""

    @abstractmethod
    def sql(self, opts: PrintOptions) -> str:
        """Converts the Treeno object into SQL string.

        All objects that represent some concept in SQL inherit from :class:`~treeno.base.Sql` and must
        implement this function to return its SQL representation which may be more compact
        or more readable depending on the :class:`~treeno.base.PrintOptions`. By default, all Treeno
        objects has a ``__str__`` representation which calls this function with default
        print options.

        When writing a new class that inherits from :class:`~treeno.base.Sql`, remember to pass along
        ``opts`` to its childrens' ``sql()`` call to make sure the print options are
        recursively applied.

        >>> from treeno.expression import wrap_literal, AliasedValue, Field
        >>> from treeno.relation import SelectQuery, Table
        >>> table = Table("a")
        >>> query = SelectQuery(select=[AliasedValue(wrap_literal(2), "foo"), Field("a") / 5], from_=table, where=Field("a") > 5)
        >>> print(query.sql(PrintOptions(mode=PrintMode.DEFAULT)))
        SELECT 2 "foo","a" / 5 FROM "a" WHERE "a" > 5
        >>> print(query.sql(PrintOptions(mode=PrintMode.PRETTY, spaces=2)))
        SELECT 2 "foo","a" / 5
          FROM "a"
         WHERE "a" > 5

        Args:
            opts: The print options to control the SQL output format.
        Returns:
            A SQL string for the given object.
        """
        raise NotImplementedError(
            f"All {self.__class__.__name__} must implement sql"
        )

    def __str__(self) -> str:
        # Default print options
        return self.sql(PrintOptions())

    def equals(self, other: Any) -> bool:
        """Checks to see whether two :class:`Sql` nodes have identical content.

        Note:
            Why do we not use ``__eq__`` here? Because for the sake of syntactical sugar, we need that operator for
            :class:`~treeno.expression.Value` to evaluate whether two objects are equal in SQL space (thus generating
            a Sql node using :class:`~treeno.expression.Equal` rather than returning to us a boolean value).

        Args:
            other: Another potentially SQL-like object.
        Returns:
            True if both objects are equal, False otherwise.
        """
        if not isinstance(other, Sql) or type(self) is not type(other):
            return False
        self_dict = attr.asdict(self)
        other_dict = attr.asdict(other)
        return self_dict == other_dict

    def assert_equals(self, other: Any) -> None:
        """Assert whether two :class:`Sql` nodes are the same.

        For more information, refer to :func:`Sql.equals`.
        """
        assert self.equals(other)


class SetQuantifier(DefaultableEnum):
    """Whether to select all rows or only distinct values
    """

    DISTINCT = auto()
    ALL = auto()

    @classmethod
    def default(cls: GenericEnum) -> GenericEnum:
        return cls.ALL
