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
            self.visitor.visit(ast).assert_equals(type_obj)

    def test_decimal_type(self):
        # Decimal is a bit of a special case - DECIMAL is syntactic sugar for DECIMAL(38, 0), which is max precision
        # and no scale
        ast = get_parser("DECIMAL").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = decimal(precision=38, scale=0)
        self.visitor.visit(ast).assert_equals(type_obj)

        ast = get_parser("DECIMAL(30)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = decimal(precision=30, scale=0)
        self.visitor.visit(ast).assert_equals(type_obj)

        ast = get_parser("DECIMAL(30, 10)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = decimal(precision=30, scale=10)
        self.visitor.visit(ast).assert_equals(type_obj)

    def test_row_type(self):
        ast = get_parser("ROW(BIGINT, DECIMAL(30), VARCHAR)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = row(dtypes=[bigint(), decimal(precision=30), varchar()])
        self.visitor.visit(ast).assert_equals(type_obj)

    def test_interval_type(self):
        ast = get_parser("INTERVAL YEAR TO MONTH").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = interval(from_interval="YEAR", to_interval="MONTH")
        self.visitor.visit(ast).assert_equals(type_obj)

        ast = get_parser("INTERVAL DAY TO SECOND").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = interval(from_interval="DAY", to_interval="SECOND")
        self.visitor.visit(ast).assert_equals(type_obj)

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
        self.visitor.visit(ast).assert_equals(type_obj)

    def test_map_type(self):
        ast = get_parser("MAP(BIGINT, VARCHAR)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = map_(from_dtype=bigint(), to_dtype=varchar())
        self.visitor.visit(ast).assert_equals(type_obj)

    def test_datetime_type(self):
        ast = get_parser("TIMESTAMP").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(timezone=False)
        self.visitor.visit(ast).assert_equals(type_obj)

        ast = get_parser("TIMESTAMP(9)").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(precision=9, timezone=False)
        self.visitor.visit(ast).assert_equals(type_obj)

        ast = get_parser("TIMESTAMP WITH TIME ZONE").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(timezone=True)
        self.visitor.visit(ast).assert_equals(type_obj)

        ast = get_parser("TIMESTAMP(9) WITH TIME ZONE").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = timestamp(precision=9, timezone=True)
        self.visitor.visit(ast).assert_equals(type_obj)

    def test_double_precision_type(self):
        ast = get_parser("DOUBLE PRECISION").type_()
        assert isinstance(ast, SqlBaseParser.Type_Context)
        type_obj = double()
        self.visitor.visit(ast).assert_equals(type_obj)
