import attr
import inspect
from abc import ABC
from typing import Type, Dict, TypeVar, Optional, ClassVar
from treeno.expression import Expression, GenericValue, wrap_literal
from treeno.base import PrintOptions, Sql

GenericFunction = TypeVar("GenericFunction", bound="Function")

NAMES_TO_FUNCTIONS: Dict[str, Type["Function"]] = {}
FUNCTIONS_TO_NAMES: Dict[Type["Function"], str] = {}


class Function(Expression, ABC):
    """Functions are expressions that require parenthesizing and support aggregation, filter, sortItem,
    pattern recognition, etc.

    Functions are different from general expressions such as Cast, TypeConstructor, Like, etc.
    """

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)

        # We ignore abstract classes
        if inspect.isabstract(cls):
            return

        if not hasattr(cls, "FN_NAME"):
            raise TypeError(
                "Every Function that's not an ABC must have the field FN_NAME defined."
            )
        fn_name = getattr(cls, "FN_NAME")
        assert isinstance(fn_name, str), "FN_NAME must be a string"
        NAMES_TO_FUNCTIONS[fn_name] = cls
        FUNCTIONS_TO_NAMES[cls] = fn_name


@attr.s
class UnaryFunctionMixin(Sql, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: Optional[PrintOptions] = None) -> str:
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"


class AggregateFunction(Function, ABC):
    ...


class Sum(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "SUM"


class Arbitrary(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "ARBITRARY"


class ArrayAgg(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "ARRAY_AGG"


class Avg(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "AVG"


class BoolAnd(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_AND"


class BoolOr(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_OR"


class Checksum(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "CHECKSUM"


class Count(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT"


class CountIf(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT_IF"


class Every(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "EVERY"


class GeometricMean(UnaryFunctionMixin, AggregateFunction):
    FN_NAME: ClassVar[str] = "GEOMETRIC_MEAN"


class ListAgg(AggregateFunction):
    FN_NAME: ClassVar[str] = "LISTAGG"
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: Optional[PrintOptions] = None) -> str:
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"
