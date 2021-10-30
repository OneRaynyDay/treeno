from typing import List, Any, Optional, Callable, Dict
from collections import defaultdict
import attr

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
]


@attr.s
class DataType:
    """Trino data types are used in cast functions and in type verification in the AST.
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
                # TODO: We can either fill in the default values or leave them undefined.
                # Unfortunately, because default values can be overridden on the session level,
                # we want to be hesitant in imputing missing values with a fixed default value.
                # For example, see hive.timestamp-precision in https://trino.io/docs/current/connector/hive.html
                # Let's see if this will prove to be a problem later.
                continue

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

    def __str__(self):
        if self.type_name == TIMESTAMP:
            return emit_timestamp(self)
        elif self.type_name == INTERVAL:
            return emit_interval(self)

        if not self.parameters:
            return self.type_name

        param_string = ",".join(
            str(param) for param in self.parameters.values()
        )
        return f"{self.type_name}({param_string})"


@attr.s
class TypeParameter:
    name: str = attr.ib()
    required: bool = attr.ib()
    type: type = attr.ib()
    validator: Optional[Callable[..., None]] = attr.ib(default=None)


def emit_interval(interval: DataType) -> str:
    assert interval.type_name == INTERVAL
    from_interval = interval.parameters["from_interval"]
    type_string = f"{interval.type_name} {from_interval}"
    to_interval = interval.parameters.get("to_interval", None)
    if to_interval is not None:
        type_string += f" {to_interval}"
    return type_string


def emit_timestamp(timestamp: DataType) -> str:
    assert timestamp.type_name == TIMESTAMP
    type_string = timestamp.type_name
    precision = timestamp.parameters.get("precision", None)
    if precision is not None:
        type_string += f"({precision})"

    timezone = timestamp.parameters.get("timezone", None)
    if timezone:
        type_string += " WITH TIME ZONE"
    return type_string


def validate_row_dtypes(row: DataType) -> None:
    data_types = row.parameters["dtypes"]
    assert all(
        isinstance(t, DataType) for t in data_types
    ), "dtypes argument must be a list of DataTypes"


def validate_interval(interval: DataType) -> None:
    from_interval = interval.parameters["from_interval"]
    to_interval = interval.parameter.get("to_interval", from_interval)
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
            TypeParameter("precision", True, int),
            TypeParameter("scale", False, int),
        ],
        VARCHAR: [TypeParameter("max_chars", False, int)],
        CHAR: [TypeParameter("max_chars", False, int)],
        TIME: [
            TypeParameter("precision", False, int),
            TypeParameter("timezone", False, bool),
        ],
        TIMESTAMP: [
            TypeParameter("precision", False, int),
            TypeParameter("timezone", False, bool),
        ],
        ARRAY: [TypeParameter("dtype", True, DataType)],
        MAP: [
            TypeParameter("from_dtype", True, DataType),
            TypeParameter("to_dtype", True, DataType),
        ],
        ROW: [TypeParameter("dtypes", True, list)],
        INTERVAL: [
            TypeParameter("from_interval", True, str),
            TypeParameter("to_interval", True, str),
        ],
    },
)

VALIDATORS: Dict[str, Optional[Callable[[DataType], None]]] = {
    ROW: validate_row_dtypes,
    INTERVAL: validate_interval,
}
