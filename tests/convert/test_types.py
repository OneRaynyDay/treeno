import pytest

from treeno.datatypes.builder import (
    array,
    bigint,
    decimal,
    double,
    interval,
    map_,
    row,
    timestamp,
    varchar,
)
from treeno.datatypes.types import DataType
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestDataTypes(VisitorTest):
    def test_generic_type(self):
        for t in [
            "BIGINT",
            "BOOLEAN",
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "REAL",
            "DOUBLE",
            "VARCHAR",
            "CHAR",
            "VARBINARY",
            "JSON",
            "DECIMAL",
        ]:
            ast = get_parser(t).type_()
            assert isinstance(ast, SqlBaseParser.Type_Context)
            type_obj = DataType(t)
            assert self.visitor.visit(ast) == type_obj

    def test_decimal_type(self):
        # Decimal is a bit of a special case - DECIMAL is syntactic sugar for DECIMAL(38, 0), which is max precision
        # and no scale
        ast = get_parser("DECIMAL").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = decimal(precision=38, scale=0)
        assert self.visitor.visit(ast) == type_obj

        ast = get_parser("DECIMAL(30)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = decimal(precision=30, scale=0)
        assert self.visitor.visit(ast) == type_obj

        ast = get_parser("DECIMAL(30, 10)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = decimal(precision=30, scale=10)
        assert self.visitor.visit(ast) == type_obj

    def test_row_type(self):
        ast = get_parser("ROW(BIGINT, DECIMAL(30), VARCHAR)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = row(dtypes=[bigint(), decimal(precision=30), varchar()])
        assert self.visitor.visit(ast) == type_obj

    def test_interval_type(self):
        ast = get_parser("INTERVAL YEAR TO MONTH").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = interval(from_interval="YEAR", to_interval="MONTH")
        assert self.visitor.visit(ast) == type_obj

        ast = get_parser("INTERVAL DAY TO SECOND").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = interval(from_interval="DAY", to_interval="SECOND")
        assert self.visitor.visit(ast) == type_obj

        # some intervals aren't available right now in Trino:
        ast = get_parser("INTERVAL MONTH TO SECOND").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        with pytest.raises(
            AssertionError, match="From interval must be YEAR or DAY, got MONTH"
        ):
            self.visitor.visit(ast)

    def test_array_type(self):
        ast = get_parser("ARRAY(BIGINT)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = array(dtype=bigint())
        assert self.visitor.visit(ast) == type_obj

    def test_map_type(self):
        ast = get_parser("MAP(BIGINT, VARCHAR)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = map_(from_dtype=bigint(), to_dtype=varchar())
        assert self.visitor.visit(ast) == type_obj

    def test_datetime_type(self):
        ast = get_parser("TIMESTAMP").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(timezone=False)
        assert self.visitor.visit(ast) == type_obj

        ast = get_parser("TIMESTAMP(9)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(precision=9, timezone=False)
        assert self.visitor.visit(ast) == type_obj

        ast = get_parser("TIMESTAMP WITH TIME ZONE").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(timezone=True)
        assert self.visitor.visit(ast) == type_obj

        ast = get_parser("TIMESTAMP(9) WITH TIME ZONE").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(precision=9, timezone=True)
        assert self.visitor.visit(ast) == type_obj

    def test_double_precision_type(self):
        ast = get_parser("DOUBLE PRECISION").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = double()
        assert self.visitor.visit(ast) == type_obj
