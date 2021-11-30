import unittest
import pytest
from treeno.builder.convert import ConvertVisitor
from treeno.grammar.gen.SqlBaseLexer import SqlBaseLexer
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.expression import Literal, Field, AliasedValue
from antlr4 import CommonTokenStream
from antlr4.InputStream import InputStream


def get_parser(sql: str) -> SqlBaseParser:
    lexer = SqlBaseLexer(InputStream(data=sql))
    stream = CommonTokenStream(lexer)
    parser = SqlBaseParser(stream)
    return parser


class VisitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.visitor = ConvertVisitor()


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


class TestExpression(VisitorTest):
    """This is to test nested expressions, which is more of
    an integration test for chaining literals, identifiers, and functions
    together.
    """

    def test_compound_expressions(self):
        pass


class TestLiterals(VisitorTest):
    def test_integer(self):
        ast = get_parser("123").number()
        assert isinstance(ast, SqlBaseParser.IntegerLiteralContext)
        assert self.visitor.visit(ast) == Literal(123)

        ast = get_parser("-123").number()
        assert isinstance(ast, SqlBaseParser.IntegerLiteralContext)
        assert self.visitor.visit(ast) == Literal(123)

    def test_decimal(self):
        ast = get_parser("123.45").number()
        assert isinstance(ast, SqlBaseParser.DecimalLiteralContext)
        assert self.visitor.visit(ast) == Literal(123.45)

        ast = get_parser("-123.45").number()
        assert isinstance(ast, SqlBaseParser.DecimalLiteralContext)
        assert self.visitor.visit(ast) == Literal(-123.45)

    def test_double(self):
        ast = get_parser("-1.23E+2").number()
        assert isinstance(ast, SqlBaseParser.DoubleLiteralContext)
        assert self.visitor.visit(ast) == Literal(pytest.approx(-1.23e2))

        ast = get_parser(".45E-2").number()
        assert isinstance(ast, SqlBaseParser.DoubleLiteralContext)
        assert self.visitor.visit(ast) == Literal(pytest.approx(-0.45e-2))

    def test_string(self):
        ast = get_parser("'abc'").string()
        assert isinstance(ast, SqlBaseParser.BasicStringLiteralContext)
        assert self.visitor.visit(ast) == Literal(pytest.approx(-1.23e2))

        ast = get_parser("U&'chilly snowman \2603'").string()
        assert isinstance(ast, SqlBaseParser.UnicodeStringLiteralContext)
        assert self.visitor.visit(ast) == Literal("chilly snowman \u2603")

        ast = get_parser("U&'chilly snowman #2603' UESCAPE '#'").string()
        assert isinstance(ast, SqlBaseParser.UnicodeStringLiteralContext)
        assert self.visitor.visit(ast) == Literal("chilly snowman \u2603")


class TestRelation(VisitorTest):
    """Note that we leave select to its own test suite (TestSelect)
    since it's much more complicated
    """

    def test_table(self):
        pass

    def test_subquery(self):
        pass

    def test_unnest(self):
        pass

    def test_lateral(self):
        pass

    def test_join(self):
        pass

    def test_alias(self):
        pass


class TestSelect(VisitorTest):
    def test_select_star(self):
        pass

    def test_select_single(self):
        pass

    def test_query_specification(self):
        pass


class TestOperators(VisitorTest):
    def test_logical_binary(self):
        pass

    def test_logical_not(self):
        pass

    def test_comparison(self):
        pass

    def test_between(self):
        pass

    def test_in_list(self):
        pass

    def test_like(self):
        pass

    def test_isnull(self):
        pass

    def test_distinct_from(self):
        pass

    def test_arithmetic_binary(self):
        pass

    def test_arithmetic_unary(self):
        pass

    def test_cast(self):
        pass


class TestDataTypes(VisitorTest):
    def test_generic_type(self):
        pass

    def test_row_type(self):
        pass

    def test_interval_type(self):
        pass

    def test_array_type(self):
        pass

    def test_datetime_type(self):
        pass

    def test_double_precision_type(self):
        pass


if __name__ == "__main__":
    unittest.main()
