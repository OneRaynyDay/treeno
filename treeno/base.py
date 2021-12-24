import attr
from abc import ABC, abstractmethod
from enum import Enum, auto


class PrintMode(Enum):
    DEFAULT = auto()
    PRETTY = auto()


@attr.s
class PrintOptions:
    mode: PrintMode = attr.ib(default=PrintMode.DEFAULT)
    spaces: int = attr.ib(default=0)

    def indent(self) -> "PrintOptions":
        """Create a copy of print options with deeper nested level.
        We create a copy here instead of mutating the state to keep it simple and less bug-prone.
        """
        return PrintOptions(pretty=self.pretty, spaces=self.spaces + 4)


class Sql(ABC):
    @abstractmethod
    def sql(self, print_options: PrintOptions):
        raise NotImplementedError(
            f"All {self.__class__.__name__} must implement sql"
        )

    def __str__(self):
        # Default print options
        return self.sql(PrintOptions())


class SetQuantifier(Enum):
    """Whether to select all rows or only distinct values
    """

    DISTINCT = auto()
    ALL = auto()
