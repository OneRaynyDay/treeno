import unittest
import pytest
from treeno.builder.convert import ConvertVisitor
from treeno.grammar.gen.SqlBaseLexer import SqlBaseLexer
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.expression import Literal, Field, AliasedValue, Array
from treeno.relation import (
    Table,
    SelectQuery,
    Unnest,
    Lateral,
    Join,
    JoinType,
    JoinConfig,
    JoinOnCriteria,
    JoinUsingCriteria,
)
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
        ast = get_parser("foo.bar.baz").relationPrimary()
        assert isinstance(ast, SqlBaseParser.TableNameContext)
        assert self.visitor.visit(ast) == Table(
            "baz", schema="bar", catalog="foo"
        )

        ast = get_parser("bar.baz").relationPrimary()
        assert isinstance(ast, SqlBaseParser.TableNameContext)
        assert self.visitor.visit(ast) == Table("baz", schema="bar")

        ast = get_parser("baz").relationPrimary()
        assert isinstance(ast, SqlBaseParser.TableNameContext)
        assert self.visitor.visit(ast) == Table("baz")

    def test_subquery(self):
        ast = get_parser("(SELECT 1)").relationPrimary()
        assert isinstance(ast, SqlBaseParser.SubqueryRelationContext)
        assert self.visitor.visit(ast) == SelectQuery(
            select_values=[Literal(1)]
        )

    def test_unnest(self):
        ast = get_parser(
            "UNNEST(ARRAY [1,2,3], ARRAY['a','b','c'])"
        ).relationPrimary()
        assert isinstance(ast, SqlBaseParser.UnnestContext)
        assert self.visitor.visit(ast) == Unnest(
            array_values=[
                Array.from_values(Literal(1), Literal(2), Literal(3)),
                Array.from_values(Literal("a"), Literal("b"), Literal("c")),
            ],
            with_ordinality=False,
        )

        ast = get_parser(
            "UNNEST(some_column) WITH ORDINALITY"
        ).relationPrimary()
        assert isinstance(ast, SqlBaseParser.UnnestContext)
        assert self.visitor.visit(ast) == Unnest(
            array_values=[Field("some_column")], with_ordinality=True
        )

    def test_lateral(self):
        ast = get_parser("LATERAL (SELECT 1)").relationPrimary()
        assert isinstance(ast, SqlBaseParser.LateralContext)
        assert self.visitor.visit(ast) == Lateral(
            subquery=SelectQuery(select_values=[Literal(1)])
        )

    def test_cross_join(self):
        ast = get_parser("a.b.c CROSS JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(join_type=JoinType.CROSS),
        )

    def test_cross_join(self):
        ast = get_parser("a.b.c CROSS JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(join_type=JoinType.CROSS),
        )

    def test_inner_join(self):
        ast = get_parser("a.b.c INNER JOIN x.y.z ON c.foo = z.bar").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(
                join_type=JoinType.INNER,
                criteria=JoinOnCriteria(Field("foo", "c") == Field("bar", "z")),
                natural=False,
            ),
        )

    def test_outer_left_join(self):
        ast = get_parser(
            "a.b.c LEFT OUTER JOIN x.y.z USING (foo, bar)"
        ).relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(
                join_type=JoinType.LEFT,
                criteria=JoinUsingCriteria(["foo", "bar"]),
                natural=False,
            ),
        )

    def test_outer_full_join(self):
        ast = get_parser("a.b.c NATURAL FULL JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(join_type=JoinType.OUTER, natural=True),
        )

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
