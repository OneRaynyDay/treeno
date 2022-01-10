import unittest

import attr

from treeno.datatypes.builder import bigint, unknown
from treeno.datatypes.resolve import resolve_fields
from treeno.expression import Field, wrap_literal
from treeno.functions.common import Concatenate
from treeno.relation import Schema, SchemaField, Table


class TestConvertFields(unittest.TestCase):
    def test_convert(self):
        f1 = Field("x", table=None)
        f2 = Field("y", table="a")
        f3 = Field("z", table="b")
        f4 = Field("w", table="c")
        schema = Schema(
            fields=[
                SchemaField("x", Table(name="a"), bigint()),
                SchemaField("y", Table(name="b"), bigint()),
                SchemaField("z", Table(name="c"), bigint()),
            ],
            relation_ids={"a", "b", "c"},
        )
        expr = (
            Concatenate([wrap_literal(3) + f1, wrap_literal(2) - f2]) + f3 / f4
        )

        assert f1.data_type == unknown()
        assert f2.data_type == unknown()
        assert f3.data_type == unknown()
        assert f4.data_type == unknown()

        new_expr = resolve_fields(expr, schema)
        new_f1 = attr.evolve(f1, data_type=bigint())
        new_f2 = attr.evolve(f2, data_type=bigint())
        new_f3 = attr.evolve(f3, data_type=bigint())
        # f4 was not modified
        assert (
            new_expr
            == Concatenate([wrap_literal(3) + new_f1, wrap_literal(2) - new_f2])
            + new_f3 / f4
        )
