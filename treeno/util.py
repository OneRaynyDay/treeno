import itertools
from typing import Optional, Any, Iterable, TypeVar

T = TypeVar("T")


def quote_identifier(identifier: str) -> str:
    return f'"{identifier}"'


def parenthesize(val: Any) -> str:
    return f"({val})"


def chain_identifiers(*identifiers: Optional[str], join_string=".") -> str:
    """Chains a list of identifiers together by periods or whatever join_string is
    For example, chain_identifier(None, None, "a", "b") will give us "a"."b"
    """
    identifiers = [
        identifier for identifier in identifiers if identifier is not None
    ]
    return join_string.join(
        [quote_identifier(identifier) for identifier in identifiers]
    )


def quote_literal(literal: str) -> str:
    return f"'{literal}'"


def nth(iterable: Iterable[T], n: int, default: Optional[T] = None) -> T:
    "Returns the nth item or a default value"
    return next(itertools.islice(iterable, n, None), default)
