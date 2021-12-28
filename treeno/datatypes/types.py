from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

import attr

from treeno.base import PrintOptions, Sql

(
    BOOLEAN,
    INTEGER,
    TINYINT,
    SMALLINT,
    BIGINT,
    REAL,
    DOUBLE,
    DECIMAL,
    VARCHAR,
    CHAR,
    VARBINARY,
    JSON,
    DATE,
    TIME,
    TIMESTAMP,
    INTERVAL,
    IP,
    UUID,
    HLL,
    P4HLL,
    QDIGEST,
    TDIGEST,
    ARRAY,
    MAP,
    ROW,
    # NULL's are unknown types because they can be reinterpreted as any type
    # since all types in Trino are optional
    UNKNOWN,
) = ALLOWED_TYPES = [
    "BOOLEAN",
    "INTEGER",
    "TINYINT",
    "SMALLINT",
    "BIGINT",
    "REAL",
    "DOUBLE",
    "DECIMAL",
    "VARCHAR",
    "CHAR",
    "VARBINARY",
    "JSON",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "INTERVAL",
    "IPADDRESS",
    "UUID",
    "HYPERLOGLOG",
    "P4HYPERLOGLOG",
    "QDIGEST",
    "TDIGEST",
    "ARRAY",
    "MAP",
    "ROW",
    "UNKNOWN",
]


@attr.s
class DataType(Sql):
    """Trino data types are used in cast functions and in type verification in the AST.

    This class should almost never be used by the client. Please refer to builder for the available
    builder functions.
    """

    type_name: str = attr.ib()
    parameters: Dict[str, Any] = attr.ib(factory=dict)

    def __attrs_post_init__(self):
        self.type_name = self.type_name.upper()
        assert (
            self.type_name in ALLOWED_TYPES
        ), f"Type {self.type_name} is not a recognized Trino type."
        for field in FIELDS[self.type_name]:
            if not field.required and field.name not in self.parameters:
                # Unfortunately, because default values can be overridden on the session level,
                # we want to be hesitant in imputing missing values with a fixed default value.
                # For example, see hive.timestamp-precision in https://trino.io/docs/current/connector/hive.html
                # Thus, we only do it when there's a default value
                if field.default is None:
                    continue
                self.parameters[field.name] = field.default

            assert (
                field.name in self.parameters
            ), f"{field.name} not specified. Required for {self.type_name}"
            field_value = self.parameters[field.name]
            assert isinstance(field_value, field.type), (
                f"Field {field.name} for type {self.type_name} must "
                f"be of type {field.type.__name__}. "
                f"Got {type(field_value).__name__} instead with value {field_value})"
            )

        all_parameters = set(param.name for param in FIELDS[self.type_name])
        current_parameters = set(self.parameters)
        assert current_parameters.issubset(
            all_parameters
        ), f"Expected a subset of parameters from {all_parameters}, got {current_parameters} instead"
        validator = VALIDATORS.get(self.type_name, None)
        if validator is not None:
            validator(self)

    def sql(self, opts: PrintOptions) -> str:
        if self.type_name in (TIMESTAMP, TIME):
            return emit_timelike(self)
        elif self.type_name == INTERVAL:
            return emit_interval(self)
        elif self.type_name == ROW:
            return emit_row(self)

        if not self.parameters:
            return self.type_name

        type_params = FIELDS[self.type_name]
        values = []
        # We do this to ensure the order of parameters is outputted correctly.
        # Consider the following situation:
        # We pass in parameters={"scale":10} for decimal, and then we add the default precision after during post init.
        # If we were to iterate on the dict's values we'd end up switching precision and scale if we use the order to
        # print the type.
        for param in type_params:
            assert (
                param.name in self.parameters
            ), f"Missing parameter {param.name} for type {self.type_name}"
            values.append(str(self.parameters[param.name]))

        param_string = ",".join(values)
        return f"{self.type_name}({param_string})"


@attr.s
class TypeParameter:
    name: str = attr.ib()
    required: bool = attr.ib()
    type: type = attr.ib()
    validator: Optional[Callable[..., None]] = attr.ib(default=None)
    # NOTE: None is not a valid default value, it represents the absence of a default.
    default: Optional[Any] = attr.ib(default=None)


def emit_interval(interval: DataType) -> str:
    assert interval.type_name == INTERVAL
    from_interval = interval.parameters["from_interval"]
    to_interval = interval.parameters["to_interval"]
    type_string = f"{interval.type_name} {from_interval} TO {to_interval}"
    return type_string


def emit_timelike(timestamp: DataType) -> str:
    assert timestamp.type_name in (TIMESTAMP, TIME)
    type_string = timestamp.type_name
    precision = timestamp.parameters.get("precision", None)
    if precision is not None:
        type_string += f"({precision})"

    timezone = timestamp.parameters.get("timezone", None)
    if timezone:
        type_string += " WITH TIME ZONE"
    return type_string


def emit_row(row: DataType) -> str:
    assert row.type_name == ROW
    subtype_str = ",".join(str(dtype) for dtype in row.parameters["dtypes"])
    return f"{ROW}({subtype_str})"


def validate_timelike(timelike: DataType) -> None:
    precision = timelike.parameters.get("precision", None)
    if precision is not None:
        assert (
            0 <= precision <= 12
        ), f"Precision of {precision} is not supported"


def validate_single_dtype_parameter(row: DataType) -> None:
    data_type = row.parameters["dtype"]
    assert isinstance(data_type, DataType)


def validate_map(row: DataType) -> None:
    from_type = row.parameters["from_dtype"]
    to_type = row.parameters["to_dtype"]
    assert isinstance(from_type, DataType)
    assert isinstance(to_type, DataType)


def validate_row(row: DataType) -> None:
    data_types = row.parameters["dtypes"]
    assert all(
        isinstance(t, DataType) for t in data_types
    ), "dtypes argument must be a list of DataTypes"


def validate_decimal(decimal: DataType) -> None:
    # TODO: Should we validate the scale as well?
    precision = decimal.parameters["precision"]
    assert 0 <= precision <= 38, f"Precision {precision} is not supported"


def validate_nonparametric(nonparametric: DataType) -> None:
    parameters = nonparametric.parameters
    assert not len(
        parameters
    ), f"Nonparametric data type {nonparametric} does not accept parameters, got {parameters}"


def validate_interval(interval: DataType) -> None:
    from_interval = interval.parameters["from_interval"]
    to_interval = interval.parameters["to_interval"]
    assert from_interval in {
        "YEAR",
        "DAY",
    }, f"From interval must be YEAR or DAY, got {from_interval}"
    if from_interval == "YEAR":
        assert (
            to_interval == "MONTH"
        ), f"Currently only YEAR TO MONTH is allowed, not YEAR TO {to_interval}"
    elif from_interval == "DAY":
        assert (
            to_interval == "SECOND"
        ), f"Currently only DAY TO SECOND is allowed, not DAY TO {to_interval}"


FIELDS: Dict[str, List[TypeParameter]] = defaultdict(
    list,
    {
        DECIMAL: [
            # NOTE: According to Trino docs, precision is not optional, but we can still
            # CAST(3.0 AS DECIMAL) which doesn't have a precision specified (but is inferred).
            # The response from this thread:
            # https://trinodb.slack.com/archives/CFLB9AMBN/p1637464297012200
            # ... should dictate this behavior.
            TypeParameter("precision", required=False, type=int, default=38),
            TypeParameter("scale", required=False, type=int, default=0),
        ],
        # VARCHAR's max_chars is not required because it can be unbounded, but we can't supply a default for it
        # because there is no inf representable in integer space. The absence of the parameter shall denote inf.
        VARCHAR: [TypeParameter("max_chars", required=False, type=int)],
        CHAR: [TypeParameter("max_chars", required=False, type=int, default=1)],
        TIME: [
            # Read TIMESTAMP's explanation for why precision is not required but also doesn't have a default.
            TypeParameter("precision", required=False, type=int),
            TypeParameter("timezone", required=False, type=bool, default=False),
        ],
        TIMESTAMP: [
            # Precision is not required, but we also can't supply a reasonable default because
            # you can use connector session settings to tune the default precision of your queries.
            TypeParameter("precision", required=False, type=int),
            TypeParameter("timezone", required=False, type=bool, default=False),
        ],
        ARRAY: [TypeParameter("dtype", required=True, type=DataType)],
        MAP: [
            TypeParameter("from_dtype", required=True, type=DataType),
            TypeParameter("to_dtype", required=True, type=DataType),
        ],
        ROW: [TypeParameter("dtypes", required=True, type=list)],
        INTERVAL: [
            TypeParameter("from_interval", required=True, type=str),
            TypeParameter("to_interval", required=True, type=str),
        ],
        QDIGEST: [TypeParameter("dtype", required=True, type=DataType)],
    },
)

VALIDATORS: Dict[str, Callable[[DataType], None]] = {
    BOOLEAN: validate_nonparametric,
    INTEGER: validate_nonparametric,
    TINYINT: validate_nonparametric,
    SMALLINT: validate_nonparametric,
    BIGINT: validate_nonparametric,
    REAL: validate_nonparametric,
    DOUBLE: validate_nonparametric,
    VARBINARY: validate_nonparametric,
    JSON: validate_nonparametric,
    DATE: validate_nonparametric,
    IP: validate_nonparametric,
    UUID: validate_nonparametric,
    HLL: validate_nonparametric,
    P4HLL: validate_nonparametric,
    TDIGEST: validate_nonparametric,
    UNKNOWN: validate_nonparametric,
    TIME: validate_timelike,
    TIMESTAMP: validate_timelike,
    QDIGEST: validate_single_dtype_parameter,
    ARRAY: validate_single_dtype_parameter,
    MAP: validate_map,
    ROW: validate_row,
    INTERVAL: validate_interval,
    DECIMAL: validate_decimal,
}
