import attr
import inspect
from abc import ABC
from typing import Type, Dict, TypeVar, Optional, ClassVar
from treeno.expression import Expression, GenericValue, wrap_literal, value_attr
from treeno.window import Window
from treeno.base import PrintOptions, PrintMode

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
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"


@value_attr
class AggregateFunction(Function, ABC):
    """Aggregate functions are functions that return a single aggregate value per group.
    They have special properties such as the ability to scan over windows using the OVER clause. For example:

    SELECT MAX(a) OVER (PARTITION BY date ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    """

    window: Optional[Window] = attr.ib(default=None, kw_only=True)

    def window_string(self, opts: PrintOptions) -> Dict[str, str]:
        assert (
            self.window is not None
        ), "No window exists for the aggregate function."
        newline_if_pretty = "\n" if opts.mode == PrintMode.PRETTY else ""
        return f"OVER ({newline_if_pretty}{self.window.sql(opts)})"


@value_attr
class UnaryAggregateFunction(AggregateFunction, ABC):
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: PrintOptions) -> str:
        builder = [f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"]
        if self.window:
            builder.append(self.window_string(opts))
        return " ".join(builder)


class Sum(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "SUM"


class Arbitrary(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "ARBITRARY"


class ArrayAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "ARRAY_AGG"


class Avg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "AVG"


class BoolAnd(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_AND"


class BoolOr(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "BOOL_OR"


class Checksum(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "CHECKSUM"


class Count(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT"


class CountIf(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "COUNT_IF"


class Every(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "EVERY"


class GeometricMean(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "GEOMETRIC_MEAN"


class ListAgg(UnaryAggregateFunction):
    FN_NAME: ClassVar[str] = "LISTAGG"
    value: GenericValue = attr.ib(converter=wrap_literal)

    def sql(self: GenericFunction, opts: Optional[PrintOptions] = None) -> str:
        return f"{FUNCTIONS_TO_NAMES[type(self)]}({self.value.sql(opts)})"
