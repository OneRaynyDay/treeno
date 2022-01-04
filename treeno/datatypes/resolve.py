import copy
from typing import Any, Dict

import attr

from treeno.base import Sql
from treeno.datatypes.types import DataType
from treeno.expression import Field
from treeno.relation import Query, Schema
from treeno.util import children, is_dictlike, is_listlike


def _make_field(field: Field, schema: Schema) -> Field:
    # If field's table is a whole subquery, it would belong solely
    # to that subquery and not the relation we're dealing with.
    if isinstance(field.table, Query):
        raise NotImplementedError("Subqueries are not yet supported")

    schema = [(f, t) for f, t in schema if f.name == field.name]

    # If the schema didn't include the field, it should have an unknown type
    if not len(schema):
        return copy.deepcopy(field)

    # In the case of there being two identical names, we need there to be only 1 unique field for it.
    if field.table is None:
        assert (
            len(schema) == 1
        ), f"Without a table specified, {field} is ambiguous under the schema {schema}"
        dtype = schema[0][1]
    else:
        # Find the right table by filtering 1 more time
        schema = [(f, t) for f, t in schema if f.table == field.table]
        assert (
            len(schema) == 1
        ), f"Two ambiguous fields with same name and table found {schema}"
        dtype = schema[0][1]
    return Field(name=field.name, table=field.table, data_type=dtype)


def resolve_fields(node: Any, schema: Schema) -> Any:
    if is_listlike(node):
        return type(node)(resolve_fields(child, schema) for child in node)
    if is_dictlike(node):
        return type(node)(
            (key, resolve_fields(child, schema)) for key, child in node.items()
        )
    # Everything that includes Sql members must be Sql itself, even if it doesn't have a non-throwing sql() function
    # (see JoinConfig for example)
    if not isinstance(node, Sql):
        return copy.deepcopy(node)
    # We don't want to modify data types
    if isinstance(node, DataType):
        return copy.deepcopy(node)
    if isinstance(node, Field):
        return _make_field(node, schema)
    changes: Dict[str, Any] = {}
    for k, v in children(node).items():
        changes[k] = resolve_fields(v, schema)
    return attr.evolve(node, **changes)
