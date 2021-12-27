from abc import ABC, abstractmethod
from enum import Enum, auto

import attr


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

    def equals(self, other):
        """Because we've overridden __eq__, we can no longer use that to test equality on objects. We use attr.asdict
        to make sure the fields are completely collapsed.
        TODO: However, this doesn't completely test equality as the type of the class doesn't show up, so we have to
        add that later.
        """
        self_dict = attr.asdict(self)
        other_dict = attr.asdict(other)
        if self_dict != other_dict:
            print(self_dict)
            print(other_dict)
        return self_dict == other_dict


class SetQuantifier(Enum):
    """Whether to select all rows or only distinct values
    """

    DISTINCT = auto()
    ALL = auto()
