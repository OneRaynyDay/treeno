from decimal import Decimal

import pytest

from treeno.datatypes.builder import (
    boolean,
    decimal,
    double,
    integer,
    unknown,
    varchar,
)
from treeno.expression import Literal
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestIdentifier(VisitorTest):
    def test_quoted(self):
        ast = get_parser('"foo"').identifier()
        assert isinstance(ast, SqlBaseParser.QuotedIdentifierContext)
        assert self.visitor.visit(ast) == "foo"

    def test_unquoted(self):
        ast = get_parser("foo").identifier()
        assert isinstance(ast, SqlBaseParser.UnquotedIdentifierContext)
        assert self.visitor.visit(ast) == "foo"

    def test_backquoted(self):
        ast = get_parser("`foo`").identifier()
        assert isinstance(ast, SqlBaseParser.BackQuotedIdentifierContext)
        with pytest.raises(NotImplementedError):
            self.visitor.visit(ast)

    def test_digit(self):
        ast = get_parser("12abc").identifier()
        assert isinstance(ast, SqlBaseParser.DigitIdentifierContext)
        with pytest.raises(NotImplementedError):
            self.visitor.visit(ast)


class TestLiterals(VisitorTest):
    def test_null(self):
        ast = get_parser("NULL").primaryExpression()
        assert isinstance(ast, SqlBaseParser.NullLiteralContext)
        assert self.visitor.visit(ast) == Literal(None, data_type=unknown())

    def test_integer(self):
        ast = get_parser("123").number()
        assert isinstance(ast, SqlBaseParser.IntegerLiteralContext)
        assert self.visitor.visit(ast) == Literal(123, data_type=integer())

        ast = get_parser("-123").number()
        assert isinstance(ast, SqlBaseParser.IntegerLiteralContext)
        assert self.visitor.visit(ast) == Literal(123, data_type=integer())

    def test_decimal(self):
        ast = get_parser("123.45").number()
        assert isinstance(ast, SqlBaseParser.DecimalLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            Decimal("123.45"), data_type=decimal(precision=5, scale=2)
        )

        ast = get_parser("-123.45").number()
        assert isinstance(ast, SqlBaseParser.DecimalLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            Decimal("-123.45"), data_type=decimal(precision=5, scale=2)
        )

    def test_double(self):
        ast = get_parser("-1.23E+2").number()
        assert isinstance(ast, SqlBaseParser.DoubleLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            pytest.approx(-1.23e2), data_type=double()
        )

        ast = get_parser(".45E-2").number()
        assert isinstance(ast, SqlBaseParser.DoubleLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            pytest.approx(-0.45e-2), data_type=double()
        )

    def test_string(self):
        ast = get_parser("'abc'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.StringLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            "abc", data_type=varchar(max_chars=3)
        )

        ast = get_parser("U&'chilly snowman \2603'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.StringLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            "chilly snowman \u2603", data_type=varchar(max_chars=16)
        )

        ast = get_parser(
            "U&'chilly snowman #2603' UESCAPE '#'"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.StringLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            "chilly snowman \u2603", data_type=varchar(max_chars=16)
        )

    def test_boolean(self):
        ast = get_parser("TRUE").primaryExpression()
        assert isinstance(ast, SqlBaseParser.BooleanLiteralContext)
        assert self.visitor.visit(ast) == Literal(True, data_type=boolean())

        ast = get_parser("FALSE").primaryExpression()
        assert isinstance(ast, SqlBaseParser.BooleanLiteralContext)
        assert self.visitor.visit(ast) == Literal(False, data_type=boolean())
