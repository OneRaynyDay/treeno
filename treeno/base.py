import attr
from abc import ABC, abstractmethod
from typing import Optional
from enum import Enum, auto


@attr.s
class PrintOptions:
    pretty: bool = attr.ib()
    level: int = attr.ib(default=0)
    padding: int = attr.ib(default=4)

    def pad(self) -> str:
        return self.level * self.padding * " "

    def __enter__(self) -> "PrintOptions":
        """Create a copy of print options with deeper nested level.
        We create a copy here instead of mutating the state to keep it simple and less bug-prone.
        """
        return PrintOptions(
            pretty=self.pretty, level=self.level + 1, padding=self.padding
        )

    def __exit__(self, *args, **kwargs) -> None:
        ...


class Sql(ABC):
    @abstractmethod
    def sql(self, print_options: Optional[PrintOptions] = None):
        raise NotImplementedError(
            f"All {self.__class__.__name__} must implement sql"
        )

    def __str__(self):
        return self.sql()


class SetQuantifier(Enum):
    """Whether to select all rows or only distinct values
    """

    DISTINCT = auto()
    ALL = auto()
