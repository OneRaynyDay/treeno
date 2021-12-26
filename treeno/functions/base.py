import inspect
from abc import ABC
from typing import Dict, Type, TypeVar

import attr

from treeno.base import PrintOptions
from treeno.expression import Expression, GenericValue, value_attr, wrap_literal
from treeno.util import parenthesize

GenericFunction = TypeVar("GenericFunction", bound="Function")

NAMES_TO_FUNCTIONS: Dict[str, Type["Function"]] = {}
FUNCTIONS_TO_NAMES: Dict[Type["Function"], str] = {}


@value_attr
class Function(Expression, ABC):
    """Functions are expressions that require parenthesizing and support aggregation, filter, sortItem,
    pattern recognition, etc.

    Functions are different from general expressions such as Cast, TypeConstructor, Like, etc.
    """

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)

        # TODO: We ignore abstract classes, but some already have all of their
        # abstract methods defined. Thus we do the extra check of ABC as a direct base.
        # See: https://stackoverflow.com/questions/62352982/python-determine-if-class-is-abstract-abc-without-abstractmethod
        if inspect.isabstract(cls) or ABC in cls.__bases__:
            return

        if not hasattr(cls, "FN_NAME"):
            raise TypeError(
                f"Every Function that's not an ABC must have the field FN_NAME defined. {cls.__name__} currently violates this constraint."
            )
        fn_name = getattr(cls, "FN_NAME")
        assert isinstance(fn_name, str), "FN_NAME must be a string"
        NAMES_TO_FUNCTIONS[fn_name] = cls
        FUNCTIONS_TO_NAMES[cls] = fn_name


@value_attr
class UnaryFunction(Function, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: PrintOptions) -> str:
        return f"{FUNCTIONS_TO_NAMES[type(self)]}{parenthesize(self.value.sql(opts))}"
