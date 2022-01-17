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
    @abstractmethod
    def sql(self, opts: PrintOptions) -> str:
        raise NotImplementedError(
            f"All {self.__class__.__name__} must implement sql"
        )

    def __str__(self) -> str:
        # Default print options
        return self.sql(PrintOptions())

    def equals(self, other: Any) -> bool:
        if not isinstance(other, Sql):
            return False
        self_dict = attr.asdict(self)
        other_dict = attr.asdict(other)
        return self_dict == other_dict

    def assert_equals(self, other: Any) -> None:
        """Because we've overridden __eq__, we can no longer use that to test equality on objects. We use attr.asdict
        to make sure the fields are completely collapsed.
        TODO: However, this doesn't completely test equality as the type of the class doesn't show up, so we have to
        add that later.
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
