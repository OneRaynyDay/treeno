import unittest
import pytest
from treeno.datatypes.builder import (
    boolean,
    tinyint,
    smallint,
    integer,
    bigint,
    real,
    double,
    decimal,
    varchar,
    char,
    varbinary,
    json,
    date,
    time,
    timestamp,
    interval,
    array,
    map_,
    row,
    unknown,
    ip,
    uuid,
    hll,
    p4hll,
    qdigest,
    tdigest,
)


class TestNonparametricTypes(unittest.TestCase):
    def test_nonparametric(self):
        assert str(boolean()) == "BOOLEAN"
        assert str(tinyint()) == "TINYINT"
        assert str(smallint()) == "SMALLINT"
        assert str(integer()) == "INTEGER"
        assert str(bigint()) == "BIGINT"
        assert str(real()) == "REAL"
        assert str(double()) == "DOUBLE"
        assert str(varbinary()) == "VARBINARY"
        assert str(json()) == "JSON"
        assert str(date()) == "DATE"
        assert str(unknown()) == "UNKNOWN"
        assert str(ip()) == "IPADDRESS"
        assert str(uuid()) == "UUID"
        assert str(hll()) == "HYPERLOGLOG"
        assert str(p4hll()) == "P4HYPERLOGLOG"
        assert str(qdigest()) == "QDIGEST"
        assert str(tdigest()) == "TDIGEST"

    def test_decimal(self):
        assert str(decimal()) == "DECIMAL(38,0)"
        assert str(decimal(precision=30)) == "DECIMAL(30,0)"
        assert str(decimal(scale=10)) == "DECIMAL(38,10)"
        assert str(decimal(precision=30, scale=10)) == "DECIMAL(30,10)"

        with pytest.raises(
            AssertionError, match="Precision 3000 is not supported"
        ):
            decimal(precision=3000)

    def test_time_and_timestamp(self):
        assert str(timestamp()) == "TIMESTAMP"
        assert str(timestamp(timezone=True)) == "TIMESTAMP WITH TIME ZONE"
        assert str(timestamp(precision=9)) == "TIMESTAMP(9)"
        assert (
            str(timestamp(timezone=True, precision=6))
            == "TIMESTAMP(6) WITH TIME ZONE"
        )

        assert str(time()) == "TIME"
        assert str(time(timezone=True)) == "TIME WITH TIME ZONE"
        assert str(time(precision=9)) == "TIME(9)"
        assert str(time(timezone=True, precision=6)) == "TIME(6) WITH TIME ZONE"

        with pytest.raises(
            AssertionError, match="Precision of 2000 is not supported"
        ):
            timestamp(precision=2000)

    def test_interval(self):
        assert (
            str(interval(from_interval="YEAR", to_interval="MONTH"))
            == "INTERVAL YEAR TO MONTH"
        )
        assert (
            str(interval(from_interval="DAY", to_interval="SECOND"))
            == "INTERVAL DAY TO SECOND"
        )

        with pytest.raises(
            AssertionError,
            match="from_interval not specified. Required for INTERVAL",
        ):
            interval()

        with pytest.raises(
            AssertionError,
            match="to_interval not specified. Required for INTERVAL",
        ):
            interval(from_interval="SECOND")

        with pytest.raises(
            AssertionError,
            match="Currently only YEAR TO MONTH is allowed, not YEAR TO SECOND",
        ):
            interval(from_interval="YEAR", to_interval="SECOND")

        with pytest.raises(
            AssertionError,
            match="Currently only DAY TO SECOND is allowed, not DAY TO HOUR",
        ):
            interval(from_interval="DAY", to_interval="HOUR")


if __name__ == "__main__":
    unittest.main()
