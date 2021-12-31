import itertools
from typing import Any, Dict, Iterable, Iterator, List, Optional, TypeVar

import attr

from treeno.base import Sql

T = TypeVar("T")


def quote_identifier(identifier: str) -> str:
    return f'"{identifier}"'


def parenthesize(val: Any) -> str:
    return f"({val})"


def chain_identifiers(*identifiers: Optional[str], join_string=".") -> str:
    """Chains a list of identifiers together by periods or whatever join_string is
    For example, chain_identifier(None, None, "a", "b") will give us "a"."b"
    """
    not_null_identifiers: List[str] = [
        identifier for identifier in identifiers if identifier is not None
    ]
    return join_string.join(
        [quote_identifier(identifier) for identifier in not_null_identifiers]
    )


def quote_literal(literal: str) -> str:
    return f"'{literal}'"


def nth(
    iterable: Iterable[T], n: int, default: Optional[T] = None
) -> Optional[T]:
    "Returns the nth item or a default value"
    return next(itertools.islice(iterable, n, None), default)


def is_listlike(var: Any) -> bool:
    return isinstance(var, (tuple, list, set, frozenset))


def is_dictlike(var: Any) -> bool:
    return isinstance(var, dict)


def construct_container(var: Iterable[T], it: Iterator[T]) -> Iterable[T]:
    return type(var)(it)


def children(sql: Sql, recursive: bool = False) -> Dict[str, Any]:
    fields_dict = attr.fields_dict(type(sql))
    return {field_name: getattr(sql, field_name) for field_name in fields_dict}
