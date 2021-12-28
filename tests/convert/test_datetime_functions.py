import pytest

from treeno.functions.datetime import (
    DEFAULT_DATETIME_PRECISION,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    LocalTime,
    LocalTimestamp,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestFunction(VisitorTest):
    def test_special_datetime(self):
        ast = get_parser("CURRENT_DATE").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(CurrentDate())

        ast = get_parser("CURRENT_TIMESTAMP").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            CurrentTimestamp(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("CURRENT_TIMESTAMP(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(CurrentTimestamp(9))

        ast = get_parser("CURRENT_TIMESTAMP(13)").primaryExpression()
        with pytest.raises(AssertionError, match="Invalid precision 13"):
            self.visitor.visit(ast)

        ast = get_parser("CURRENT_TIME").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            CurrentTime(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("CURRENT_TIME(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(CurrentTime(9))

        ast = get_parser("LOCALTIME").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            LocalTime(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("LOCALTIME(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(LocalTime(9))

        ast = get_parser("LOCALTIMESTAMP").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            LocalTimestamp(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("LOCALTIMESTAMP(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(LocalTimestamp(9))
