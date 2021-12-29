import unittest
from decimal import Decimal

from treeno.datatypes.builder import (
    decimal,
    double,
    integer,
    interval,
    smallint,
    unknown,
)
from treeno.expression import (
    Add,
    Divide,
    Literal,
    Minus,
    Multiply,
    TypeConstructor,
    wrap_literal,
)


class ExpressionTests(unittest.TestCase):
    def test_binary_type_conversions(self):
        s, i, f, d = (
            Literal(1, data_type=smallint()),
            wrap_literal(3),
            wrap_literal(3.5),
            wrap_literal(Decimal("3.14")),
        )
        decimal_type = decimal(precision=3, scale=2)

        for fn in [Add, Minus, Multiply, Divide]:
            assert fn(s, s).data_type == smallint()
            assert fn(i, i).data_type == integer()
            assert fn(f, f).data_type == double()
            assert fn(d, d).data_type == decimal_type

            assert fn(s, i).data_type == integer()
            assert fn(s, f).data_type == double()
            assert fn(s, d).data_type == decimal(precision=7, scale=2)

            assert fn(i, f).data_type == double()
            assert fn(i, d).data_type == decimal(precision=12, scale=2)

            assert fn(f, d).data_type == double()

        # Check minus for datetime
        d, t, ts = (
            TypeConstructor("2021-01-01", type_name="DATE"),
            TypeConstructor("01:00:00", type_name="TIME"),
            TypeConstructor("2021-01-01 01:00:00", type_name="TIMESTAMP"),
        )
        interval_type = interval(from_interval="DAY", to_interval="SECOND")
        assert Minus(d, d).data_type == interval_type
        assert Minus(t, t).data_type == interval_type
        assert Minus(ts, ts).data_type == interval_type

        assert Minus(d, ts).data_type == interval_type
        assert Minus(ts, d).data_type == interval_type

        # Can't subtract date and time, nor timestamp and time
        assert Minus(d, t).data_type == unknown()
        assert Minus(ts, t).data_type == unknown()


if __name__ == "__main__":
    unittest.main()
