from abc import ABC
from typing import Any, Dict, List, Type, TypeVar

import attr

from treeno.base import GenericVisitor, PrintOptions
from treeno.expression import Expression, Value, value_attr, wrap_literal
from treeno.printer import join_stmts
from treeno.util import is_abstract

GenericFunction = TypeVar("GenericFunction", bound="Function")

NAMES_TO_FUNCTIONS: Dict[str, Type["Function"]] = {}
FUNCTIONS_TO_NAMES: Dict[Type["Function"], str] = {}


@value_attr
class Function(Expression, ABC):
    """Functions are expressions that require parenthesizing and support aggregation, filter, sortItem,
    pattern recognition, etc.

    Functions are different from general expressions such as Cast, TypeConstructor, Like, etc.
    """

    def __init_subclass__(cls, *args: Any, **kwargs: Any):
        super().__init_subclass__(*args, **kwargs)

        if is_abstract(cls):
            return

        if not hasattr(cls, "FN_NAME"):
            raise TypeError(
                f"Every Function that's not an ABC must have the field FN_NAME defined. {cls.__name__} currently violates this constraint."
            )
        fn_name = getattr(cls, "FN_NAME")
        assert isinstance(fn_name, str), "FN_NAME must be a string"
        NAMES_TO_FUNCTIONS[fn_name] = cls
        FUNCTIONS_TO_NAMES[cls] = fn_name

    @classmethod
    def from_args(
        cls: Type[GenericFunction], *values: Any, **kwargs: Any
    ) -> GenericFunction:
        """This positional-only args constructor is required because there is no easy way to perform function overloading
        in python, so each Function class that has strange inputs should be constructed this way (and have this function
        overridden to do the right thing).
        """
        arguments = attr.fields(cls)
        keyword_only_arguments = set()
        for attribute in arguments:
            if attribute.kw_only and attribute.init:
                keyword_only_arguments.add(attribute.name)
        assert set(kwargs.keys()).issubset(
            keyword_only_arguments
        ), f"No keyword arguments allowed except for {keyword_only_arguments}"
        return cls(*values, **kwargs)

    def to_string(self, values: List[Value], opts: PrintOptions) -> str:
        arg_string = join_stmts([value.sql(opts) for value in values], opts)
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({arg_string})"


@value_attr
class UnaryFunction(Function, ABC):
    value: Value = attr.ib(converter=wrap_literal)

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.value], opts)

    def visit(self, visitor: GenericVisitor) -> None:
        visitor.visit(self.value)
