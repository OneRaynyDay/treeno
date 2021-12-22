import unittest
import pytest
from treeno.builder.convert import ConvertVisitor
from treeno.grammar.gen.SqlBaseLexer import SqlBaseLexer
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.expression import (
    Literal,
    Field,
    AliasedValue,
    Array,
    Star,
    AliasedStar,
    Add,
    Positive,
    Minus,
    Negative,
    Multiply,
    Divide,
    Modulus,
    And,
    Or,
    Not,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    NotEqual,
    Equal,
    Between,
    Array,
    InList,
    Like,
    Cast,
    TryCast,
    IsNull,
    DistinctFrom,
)
from treeno.relation import (
    AliasedRelation,
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
from treeno.functions import Sum
from treeno.groupby import GroupBy, GroupingSet, GroupingSetList, Cube, Rollup
from treeno.datatypes.builder import (
    boolean,
    integer,
    bigint,
    decimal,
    varchar,
    interval,
    row,
    map_,
    double,
    timestamp,
    array,
    unknown,
)
from decimal import Decimal
from treeno.datatypes.types import DataType
from treeno.orderby import OrderType, NullOrder, OrderTerm
from treeno.window import (
    Window,
    UnboundedFrameBound,
    BoundedFrameBound,
    CurrentFrameBound,
    BoundType,
    FrameType,
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


class TestLiterals(VisitorTest):
    def test_null(self):
        ast = get_parser("NULL").primaryExpression()
        assert isinstance(ast, SqlBaseParser.NullLiteralContext)
        assert self.visitor.visit(ast) == Literal(None, unknown())

    def test_integer(self):
        ast = get_parser("123").number()
        assert isinstance(ast, SqlBaseParser.IntegerLiteralContext)
        assert self.visitor.visit(ast) == Literal(123, integer())

        ast = get_parser("-123").number()
        assert isinstance(ast, SqlBaseParser.IntegerLiteralContext)
        assert self.visitor.visit(ast) == Literal(123, integer())

    def test_decimal(self):
        ast = get_parser("123.45").number()
        assert isinstance(ast, SqlBaseParser.DecimalLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            Decimal("123.45"), decimal(precision=5, scale=2)
        )

        ast = get_parser("-123.45").number()
        assert isinstance(ast, SqlBaseParser.DecimalLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            Decimal("-123.45"), decimal(precision=5, scale=2)
        )

    def test_double(self):
        ast = get_parser("-1.23E+2").number()
        assert isinstance(ast, SqlBaseParser.DoubleLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            pytest.approx(-1.23e2), double()
        )

        ast = get_parser(".45E-2").number()
        assert isinstance(ast, SqlBaseParser.DoubleLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            pytest.approx(-0.45e-2), double()
        )

    def test_string(self):
        ast = get_parser("'abc'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.StringLiteralContext)
        assert self.visitor.visit(ast) == Literal("abc", varchar(max_chars=3))

        ast = get_parser("U&'chilly snowman \2603'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.StringLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            "chilly snowman \u2603", varchar(max_chars=16)
        )

        ast = get_parser(
            "U&'chilly snowman #2603' UESCAPE '#'"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.StringLiteralContext)
        assert self.visitor.visit(ast) == Literal(
            "chilly snowman \u2603", varchar(max_chars=16)
        )

    def test_boolean(self):
        ast = get_parser("TRUE").primaryExpression()
        assert isinstance(ast, SqlBaseParser.BooleanLiteralContext)
        assert self.visitor.visit(ast) == Literal(True, boolean())

        ast = get_parser("FALSE").primaryExpression()
        assert isinstance(ast, SqlBaseParser.BooleanLiteralContext)
        assert self.visitor.visit(ast) == Literal(False, boolean())


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
            select=[Literal(1, integer())]
        )

    def test_unnest(self):
        ast = get_parser(
            "UNNEST(ARRAY [1,2,3], ARRAY['a','b','c'])"
        ).relationPrimary()
        assert isinstance(ast, SqlBaseParser.UnnestContext)
        assert self.visitor.visit(ast) == Unnest(
            array=[
                Array.from_values(
                    Literal(1, integer()),
                    Literal(2, integer()),
                    Literal(3, integer()),
                ),
                Array.from_values(
                    Literal("a", varchar(max_chars=1)),
                    Literal("b", varchar(max_chars=1)),
                    Literal("c", varchar(max_chars=1)),
                ),
            ],
            with_ordinality=False,
        )

        ast = get_parser(
            "UNNEST(some_column) WITH ORDINALITY"
        ).relationPrimary()
        assert isinstance(ast, SqlBaseParser.UnnestContext)
        assert self.visitor.visit(ast) == Unnest(
            array=[Field("some_column")], with_ordinality=True
        )

    def test_lateral(self):
        ast = get_parser("LATERAL (SELECT 1)").relationPrimary()
        assert isinstance(ast, SqlBaseParser.LateralContext)
        assert self.visitor.visit(ast) == Lateral(
            subquery=SelectQuery(select=[Literal(1, integer())])
        )

    def test_cross_join(self):
        ast = get_parser("a.b.c CROSS JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(join_type=JoinType.CROSS),
        )

    def test_inner_on_join(self):
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

    def test_outer_left_using_join(self):
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

    def test_natural_full_join(self):
        ast = get_parser("a.b.c NATURAL FULL JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        assert self.visitor.visit(ast) == Join(
            left_relation=Table("c", "b", "a"),
            right_relation=Table("z", "y", "x"),
            config=JoinConfig(join_type=JoinType.OUTER, natural=True),
        )

    def test_alias(self):
        ast = get_parser("a.b.c AS foo (x,y,z)").aliasedRelation()
        assert isinstance(ast, SqlBaseParser.AliasedRelationContext)
        assert self.visitor.visit(ast) == AliasedRelation(
            relation=Table("c", "b", "a"),
            alias="foo",
            column_aliases=["x", "y", "z"],
        )


class TestSelect(VisitorTest):
    def test_select_star(self):
        ast = get_parser("*").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectAllContext)
        assert self.visitor.visit(ast) == Star()

        ast = get_parser("a.*").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectAllContext)
        assert self.visitor.visit(ast) == Star(table="a")

        ast = get_parser("a.* AS (x,y,z)").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectAllContext)
        assert self.visitor.visit(ast) == AliasedStar(
            table="a", aliases=["x", "y", "z"]
        )

    def test_select_single(self):
        ast = get_parser("1+2+3").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectSingleContext)
        assert self.visitor.visit(ast) == Add(
            left=Literal(1, integer()),
            right=Add(left=Literal(2, integer()), right=Literal(3, integer())),
        )

        ast = get_parser("a").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectSingleContext)
        assert self.visitor.visit(ast) == Field(name="a")

        ast = get_parser("a AS foo").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectSingleContext)
        assert self.visitor.visit(ast) == AliasedValue(
            value=Field(name="a"), alias="foo"
        )

    def test_query_specification(self):
        # TODO: Groupby with the different options like CUBE, orderby and the such
        pass


class TestBooleanExpressions(VisitorTest):
    def test_logical_binary(self):
        ast = get_parser("TRUE AND FALSE").booleanExpression()
        assert isinstance(ast, SqlBaseParser.LogicalBinaryContext)
        assert self.visitor.visit(ast) == And(
            left=Literal(True, boolean()), right=Literal(False, boolean())
        )

        ast = get_parser("TRUE OR FALSE").booleanExpression()
        assert isinstance(ast, SqlBaseParser.LogicalBinaryContext)
        assert self.visitor.visit(ast) == Or(
            left=Literal(True, boolean()), right=Literal(False, boolean())
        )

    def test_logical_not(self):
        ast = get_parser("NOT TRUE").booleanExpression()
        assert isinstance(ast, SqlBaseParser.LogicalNotContext)
        assert self.visitor.visit(ast) == Not(value=Literal(True, boolean()))

    def test_comparison(self):
        ast = get_parser("3 < 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == LessThan(
            left=Literal(3, integer()), right=Literal(1, integer())
        )

        ast = get_parser("3 <= 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == LessThanOrEqual(
            left=Literal(3, integer()), right=Literal(1, integer())
        )

        ast = get_parser("3 > 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == GreaterThan(
            left=Literal(3, integer()), right=Literal(1, integer())
        )

        ast = get_parser("3 >= 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == GreaterThanOrEqual(
            left=Literal(3, integer()), right=Literal(1, integer())
        )

        ast = get_parser("3 = 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Equal(
            left=Literal(3, integer()), right=Literal(1, integer())
        )

        ast = get_parser("3 <> 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == NotEqual(
            left=Literal(3, integer()), right=Literal(1, integer())
        )

    def test_between(self):
        ast = get_parser("3 BETWEEN 1 AND 5").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        between_expr = Between(
            value=Literal(3, integer()),
            lower=Literal(1, integer()),
            upper=Literal(5, integer()),
        )
        assert self.visitor.visit(ast) == between_expr

        ast = get_parser("3 NOT BETWEEN 1 AND 5").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Not(between_expr)

    def test_in_list(self):
        ast = get_parser("3 IN (1+2, 5)").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        in_list_expr = InList(
            value=Literal(3, integer()),
            exprs=[
                Add(left=Literal(1, integer()), right=Literal(2, integer())),
                Literal(5, integer()),
            ],
        )
        assert self.visitor.visit(ast) == in_list_expr

        ast = get_parser("3 NOT IN (1+2, 5)").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Not(in_list_expr)

    def test_like(self):
        ast = get_parser("'abc' LIKE '%b%'").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        like_expr = Like(
            value=Literal("abc", varchar(max_chars=3)),
            pattern=Literal("%b%", varchar(max_chars=3)),
        )
        assert self.visitor.visit(ast) == like_expr

        ast = get_parser("'abc' NOT LIKE '%b%'").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Not(like_expr)

        ast = get_parser("'ab%c' LIKE '%b/%%' ESCAPE '/'").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Like(
            value=Literal("ab%c", varchar(max_chars=3)),
            pattern=Literal("%b/%%", varchar(max_chars=5)),
            escape=Literal("/", varchar(max_chars=1)),
        )

    def test_isnull(self):
        ast = get_parser("3 IS NULL").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        is_null_expr = IsNull(value=Literal(3, integer()))
        assert self.visitor.visit(ast) == is_null_expr

        ast = get_parser("3 IS NOT NULL").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Not(is_null_expr)

    def test_distinct_from(self):
        ast = get_parser("1 IS DISTINCT FROM 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        distinct_expr = DistinctFrom(
            left=Literal(1, integer()), right=Literal(1, integer())
        )
        assert self.visitor.visit(ast) == distinct_expr

        ast = get_parser("1 IS NOT DISTINCT FROM 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        assert self.visitor.visit(ast) == Not(distinct_expr)


class TestFunctions(VisitorTest):
    """For now, we classify arithmetic operations as functions"""

    def test_arithmetic_binary(self):
        ast = get_parser("1 + 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        add_expr = Add(left=Literal(1, integer()), right=Literal(1, integer()))
        assert self.visitor.visit(ast) == add_expr

        ast = get_parser("1 - 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        minus_expr = Minus(
            left=Literal(1, integer()), right=Literal(1, integer())
        )
        assert self.visitor.visit(ast) == minus_expr

        ast = get_parser("1 * 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        mult_expr = Multiply(
            left=Literal(1, integer()), right=Literal(1, integer())
        )
        assert self.visitor.visit(ast) == mult_expr

        ast = get_parser("1 / 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        div_expr = Divide(
            left=Literal(1, integer()), right=Literal(1, integer())
        )
        assert self.visitor.visit(ast) == div_expr

        ast = get_parser("1 % 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        mod_expr = Modulus(
            left=Literal(1, integer()), right=Literal(1, integer())
        )
        assert self.visitor.visit(ast) == mod_expr

    def test_arithmetic_unary(self):
        ast = get_parser("+1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticUnaryContext)
        pos_expr = Positive(value=Literal(1, integer()))
        assert self.visitor.visit(ast) == pos_expr

        # This one's actually interesting - because valueExpression expands into
        # primaryExpression, we read it as a literal -1 first before it gets evaluated as unary
        ast = get_parser("-1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ValueExpressionDefaultContext)
        literal_expr = Literal(-1, integer())
        assert self.visitor.visit(ast) == literal_expr

        ast = get_parser("-x").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticUnaryContext)
        neg_expr = Negative(value=Field("x"))
        assert self.visitor.visit(ast) == neg_expr

    def test_cast(self):
        ast = get_parser("CAST(1 AS BIGINT)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CastContext)
        cast_expr = Cast(Literal(1, integer()), bigint())
        assert self.visitor.visit(ast) == cast_expr

        ast = get_parser("TRY_CAST(1 AS BIGINT)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CastContext)
        try_cast_expr = TryCast(Literal(1, integer()), bigint())
        assert self.visitor.visit(ast) == try_cast_expr


class TestGroupBy(VisitorTest):
    def test_grouping_set(self):
        ast = get_parser("(a+b,c)").groupingSet()
        assert isinstance(ast, SqlBaseParser.GroupingSetContext)
        group_expr = GroupingSet(
            [Add(left=Field("a"), right=Field("b")), Field("c")]
        )
        assert self.visitor.visit(ast) == group_expr

    def test_multiple_grouping_sets(self):
        ast = get_parser("GROUPING SETS ((a,b), c)").groupingElement()
        assert isinstance(ast, SqlBaseParser.MultipleGroupingSetsContext)
        groups_expr = GroupingSetList(
            [GroupingSet([Field("a"), Field("b")]), GroupingSet([Field("c")])]
        )
        assert self.visitor.visit(ast) == groups_expr

    def test_cube(self):
        ast = get_parser("CUBE (a,b)").groupingElement()
        assert isinstance(ast, SqlBaseParser.CubeContext)
        cube_expr = Cube([Field("a"), Field("b")])
        assert self.visitor.visit(ast) == cube_expr

    def test_rollup(self):
        ast = get_parser("ROLLUP (a,b)").groupingElement()
        assert isinstance(ast, SqlBaseParser.RollupContext)
        rollup_expr = Rollup([Field("a"), Field("b")])
        assert self.visitor.visit(ast) == rollup_expr


class TestOrderTerms(VisitorTest):
    def test_order_term_default(self):
        ast = get_parser("a").sortItem()
        assert isinstance(ast, SqlBaseParser.SortItemContext)
        order_term = OrderTerm(
            value=Field("a"),
            order_type=OrderType.ASC,
            null_order=NullOrder.LAST,
        )
        assert self.visitor.visit(ast) == order_term

    def test_order_term_specified(self):
        ast = get_parser("a ASC").sortItem()
        assert isinstance(ast, SqlBaseParser.SortItemContext)
        order_term = OrderTerm(
            value=Field("a"),
            order_type=OrderType.ASC,
            null_order=NullOrder.LAST,
        )
        assert self.visitor.visit(ast) == order_term

        ast = get_parser("a DESC").sortItem()
        assert isinstance(ast, SqlBaseParser.SortItemContext)
        order_term = OrderTerm(
            value=Field("a"),
            order_type=OrderType.DESC,
            null_order=NullOrder.LAST,
        )
        assert self.visitor.visit(ast) == order_term

    def test_order_term_nulls(self):
        ast = get_parser("a NULLS FIRST").sortItem()
        assert isinstance(ast, SqlBaseParser.SortItemContext)
        order_term = OrderTerm(
            value=Field("a"),
            order_type=OrderType.ASC,
            null_order=NullOrder.FIRST,
        )
        assert self.visitor.visit(ast) == order_term

        ast = get_parser("a DESC NULLS LAST").sortItem()
        assert isinstance(ast, SqlBaseParser.SortItemContext)
        order_term = OrderTerm(
            value=Field("a"),
            order_type=OrderType.DESC,
            null_order=NullOrder.LAST,
        )
        assert self.visitor.visit(ast) == order_term


class TestWindow(VisitorTest):
    def test_window_definition(self):
        ast = get_parser("w AS ()").windowDefinition()
        assert isinstance(ast, SqlBaseParser.WindowDefinitionContext)
        assert self.visitor.visit(ast) == ("w", Window())

    def test_window_specification(self):
        ast = get_parser(
            "w PARTITION BY a,b ORDER BY x ASC,y DESC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING"
        ).windowSpecification()
        assert isinstance(ast, SqlBaseParser.WindowSpecificationContext)
        assert self.visitor.visit(ast) == Window(
            parent_window="w",
            partitions=[Field("a"), Field("b")],
            orderby=[
                OrderTerm(Field("x"), order_type=OrderType.ASC),
                OrderTerm(
                    Field("y"),
                    order_type=OrderType.DESC,
                    null_order=NullOrder.FIRST,
                ),
            ],
            frame_type=FrameType.GROUPS,
            end_bound=BoundedFrameBound(
                offset=Literal(5, integer()), bound_type=BoundType.FOLLOWING
            ),
        )

    def test_window_frame(self):
        ast = get_parser("ROWS CURRENT ROW").windowFrame()
        assert isinstance(ast, SqlBaseParser.WindowFrameContext)
        assert self.visitor.visit(ast) == Window(
            frame_type=FrameType.ROWS, start_bound=CurrentFrameBound()
        )

        ast = get_parser(
            "RANGE BETWEEN 5 PRECEDING AND UNBOUNDED FOLLOWING"
        ).windowFrame()
        assert isinstance(ast, SqlBaseParser.WindowFrameContext)
        assert self.visitor.visit(ast) == Window(
            frame_type=FrameType.RANGE,
            start_bound=BoundedFrameBound(
                bound_type=BoundType.PRECEDING, offset=5
            ),
            end_bound=UnboundedFrameBound(bound_type=BoundType.FOLLOWING),
        )


class TestFunction(VisitorTest):
    def test_aggregate_functions(self):
        ast = get_parser("SUM(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Sum(Field("a"))


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


if __name__ == "__main__":
    unittest.main()
