import unittest
from treeno.base import PrintOptions, PrintMode
from treeno.relation import Table, TableQuery, ValuesQuery
from treeno.orderby import OrderTerm, OrderType
from treeno.expression import Field, wrap_literal


class TestRelations(unittest.TestCase):
    def test_table(self):
        t = Table(name="table", schema="schema", catalog="catalog")
        assert t.sql(PrintOptions()) == '"catalog"."schema"."table"'

        tq = TableQuery(t)
        assert (
            tq.sql(PrintOptions(mode=PrintMode.DEFAULT))
            == tq.sql(PrintOptions(mode=PrintMode.PRETTY))
            == 'TABLE "catalog"."schema"."table"'
        )

        # Test a richer query type
        tq = TableQuery(
            t,
            offset=2,
            limit=5,
            orderby=[OrderTerm(value=Field("x"), order_type=OrderType.DESC)],
        )
        assert (
            tq.sql(PrintOptions(mode=PrintMode.DEFAULT))
            == 'TABLE "catalog"."schema"."table" ORDER BY "x" DESC OFFSET 2 LIMIT 5'
        )
        assert tq.sql(PrintOptions(mode=PrintMode.PRETTY)) == (
            ' TABLE "catalog"."schema"."table"\n'
            ' ORDER BY "x" DESC\n'
            "OFFSET 2\n"
            " LIMIT 5"
        )

    def test_values(self):
        v = ValuesQuery([wrap_literal(1), wrap_literal(2), wrap_literal(3)])
        assert (
            v.sql(PrintOptions(mode=PrintMode.DEFAULT))
            == v.sql(PrintOptions(mode=PrintMode.PRETTY))
            == "VALUES 1,2,3"
        )

        v = ValuesQuery(
            [wrap_literal(1), wrap_literal(2), wrap_literal(3)],
            offset=3,
            with_queries={"foo": TableQuery(Table(name="foo"))},
        )
        assert (
            v.sql(PrintOptions(mode=PrintMode.DEFAULT))
            == 'WITH "foo" AS (TABLE "foo") VALUES 1,2,3 OFFSET 3'
        )
        assert v.sql(PrintOptions(mode=PrintMode.PRETTY)) == (
            '  WITH "foo" AS (\n       TABLE "foo")\n'
            "VALUES 1,2,3\n"
            "OFFSET 3"
        )


if __name__ == "__main__":
    unittest.main()
