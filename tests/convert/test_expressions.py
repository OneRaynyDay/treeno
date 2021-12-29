import pytest

from treeno.datatypes.builder import (
    bigint,
    boolean,
    decimal,
    double,
    integer,
    timestamp,
    varchar,
)
from treeno.expression import (
    Add,
    And,
    Between,
    Case,
    Cast,
    DistinctFrom,
    Divide,
    Else,
    Equal,
    Field,
    GreaterThan,
    GreaterThanOrEqual,
    InList,
    Interval,
    IsNull,
    Lambda,
    LessThan,
    LessThanOrEqual,
    Like,
    Literal,
    Minus,
    Modulus,
    Multiply,
    Negative,
    Not,
    NotEqual,
    Or,
    Positive,
    RowConstructor,
    Subscript,
    TryCast,
    TypeConstructor,
    When,
    wrap_literal,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestFunctions(VisitorTest):
    """For now, we classify arithmetic operations as functions"""

    def test_arithmetic_binary(self):
        ast = get_parser("1 + 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        add_expr = Add(
            left=Literal(1, data_type=integer()),
            right=Literal(1, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(add_expr)

        ast = get_parser("1 - 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        minus_expr = Minus(
            left=Literal(1, data_type=integer()),
            right=Literal(1, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(minus_expr)

        ast = get_parser("1 * 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        mult_expr = Multiply(
            left=Literal(1, data_type=integer()),
            right=Literal(1, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(mult_expr)

        ast = get_parser("1 / 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        div_expr = Divide(
            left=Literal(1, data_type=integer()),
            right=Literal(1, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(div_expr)

        ast = get_parser("1 % 1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticBinaryContext)
        mod_expr = Modulus(
            left=Literal(1, data_type=integer()),
            right=Literal(1, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(mod_expr)

    def test_arithmetic_unary(self):
        ast = get_parser("+1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticUnaryContext)
        pos_expr = Positive(value=Literal(1, data_type=integer()))
        self.visitor.visit(ast).assert_equals(pos_expr)

        # This one's actually interesting - because valueExpression expands into
        # primaryExpression, we read it as a literal -1 first before it gets evaluated as unary
        ast = get_parser("-1").valueExpression()
        assert isinstance(ast, SqlBaseParser.ValueExpressionDefaultContext)
        literal_expr = Literal(-1, data_type=integer())
        self.visitor.visit(ast).assert_equals(literal_expr)

        ast = get_parser("-x").valueExpression()
        assert isinstance(ast, SqlBaseParser.ArithmeticUnaryContext)
        neg_expr = Negative(value=Field("x"))
        self.visitor.visit(ast).assert_equals(neg_expr)

    def test_cast(self):
        ast = get_parser("CAST(1 AS BIGINT)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CastContext)
        cast_expr = Cast(Literal(1, data_type=integer()), data_type=bigint())
        self.visitor.visit(ast).assert_equals(cast_expr)

        ast = get_parser("TRY_CAST(1 AS BIGINT)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CastContext)
        try_cast_expr = TryCast(
            Literal(1, data_type=integer()), data_type=bigint()
        )
        self.visitor.visit(ast).assert_equals(try_cast_expr)

    def test_subscript(self):
        ast = get_parser("arr[3]").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SubscriptContext)
        self.visitor.visit(ast).assert_equals(
            Subscript(value=Field("arr"), index=wrap_literal(3))
        )


class TestBooleanExpressions(VisitorTest):
    def test_logical_binary(self):
        ast = get_parser("TRUE AND FALSE").booleanExpression()
        assert isinstance(ast, SqlBaseParser.And_Context)
        self.visitor.visit(ast).assert_equals(
            And(
                left=Literal(True, data_type=boolean()),
                right=Literal(False, data_type=boolean()),
            )
        )

        ast = get_parser("TRUE OR FALSE").booleanExpression()
        assert isinstance(ast, SqlBaseParser.Or_Context)
        self.visitor.visit(ast).assert_equals(
            Or(
                left=Literal(True, data_type=boolean()),
                right=Literal(False, data_type=boolean()),
            )
        )

    def test_logical_not(self):
        ast = get_parser("NOT TRUE").booleanExpression()
        assert isinstance(ast, SqlBaseParser.LogicalNotContext)
        self.visitor.visit(ast).assert_equals(
            Not(value=Literal(True, data_type=boolean()))
        )

    def test_comparison(self):
        ast = get_parser("3 < 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            LessThan(
                left=Literal(3, data_type=integer()),
                right=Literal(1, data_type=integer()),
            )
        )

        ast = get_parser("3 <= 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            LessThanOrEqual(
                left=Literal(3, data_type=integer()),
                right=Literal(1, data_type=integer()),
            )
        )

        ast = get_parser("3 > 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            GreaterThan(
                left=Literal(3, data_type=integer()),
                right=Literal(1, data_type=integer()),
            )
        )

        ast = get_parser("3 >= 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            GreaterThanOrEqual(
                left=Literal(3, data_type=integer()),
                right=Literal(1, data_type=integer()),
            )
        )

        ast = get_parser("3 = 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            Equal(
                left=Literal(3, data_type=integer()),
                right=Literal(1, data_type=integer()),
            )
        )

        ast = get_parser("3 <> 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            NotEqual(
                left=Literal(3, data_type=integer()),
                right=Literal(1, data_type=integer()),
            )
        )

    def test_between(self):
        ast = get_parser("3 BETWEEN 1 AND 5").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        between_expr = Between(
            value=Literal(3, data_type=integer()),
            lower=Literal(1, data_type=integer()),
            upper=Literal(5, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(between_expr)

        ast = get_parser("3 NOT BETWEEN 1 AND 5").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(Not(between_expr))

    def test_in_list(self):
        ast = get_parser("3 IN (1+2, 5)").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        in_list_expr = InList(
            value=Literal(3, data_type=integer()),
            exprs=[
                Add(
                    left=Literal(1, data_type=integer()),
                    right=Literal(2, data_type=integer()),
                ),
                Literal(5, data_type=integer()),
            ],
        )
        self.visitor.visit(ast).assert_equals(in_list_expr)

        ast = get_parser("3 NOT IN (1+2, 5)").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(Not(in_list_expr))

    def test_like(self):
        ast = get_parser("'abc' LIKE '%b%'").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        like_expr = Like(
            value=Literal("abc", data_type=varchar(max_chars=3)),
            pattern=Literal("%b%", data_type=varchar(max_chars=3)),
        )
        self.visitor.visit(ast).assert_equals(like_expr)

        ast = get_parser("'abc' NOT LIKE '%b%'").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(Not(like_expr))

        ast = get_parser("'ab%c' LIKE '%b/%%' ESCAPE '/'").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(
            Like(
                value=Literal("ab%c", data_type=varchar(max_chars=4)),
                pattern=Literal("%b/%%", data_type=varchar(max_chars=5)),
                escape=Literal("/", data_type=varchar(max_chars=1)),
            )
        )

    def test_isnull(self):
        ast = get_parser("3 IS NULL").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        is_null_expr = IsNull(value=Literal(3, data_type=integer()))
        self.visitor.visit(ast).assert_equals(is_null_expr)

        ast = get_parser("3 IS NOT NULL").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(Not(is_null_expr))

    def test_distinct_from(self):
        ast = get_parser("1 IS DISTINCT FROM 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        distinct_expr = DistinctFrom(
            left=Literal(1, data_type=integer()),
            right=Literal(1, data_type=integer()),
        )
        self.visitor.visit(ast).assert_equals(distinct_expr)

        ast = get_parser("1 IS NOT DISTINCT FROM 1").booleanExpression()
        assert isinstance(ast, SqlBaseParser.PredicatedContext)
        self.visitor.visit(ast).assert_equals(Not(distinct_expr))


class TestConstructorExprs(VisitorTest):
    def test_type_constructor(self):
        ast = get_parser("DECIMAL '3.0'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.TypeConstructorContext)
        self.visitor.visit(ast).assert_equals(
            TypeConstructor("3.0", type_name="DECIMAL")
        )

        ast = get_parser("TIMESTAMP '2021-01-01 00:00:01'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.TypeConstructorContext)
        self.visitor.visit(ast).assert_equals(
            TypeConstructor("2021-01-01 00:00:01", type_name="TIMESTAMP")
        )

        ast = get_parser("BIGINT '3'").primaryExpression()
        assert isinstance(ast, SqlBaseParser.TypeConstructorContext)
        self.visitor.visit(ast).assert_equals(
            TypeConstructor("3", type_name="BIGINT")
        )

        ast_double = get_parser("DOUBLE '3'").primaryExpression()
        assert isinstance(ast_double, SqlBaseParser.TypeConstructorContext)
        ast_double_precision = get_parser(
            "DOUBLE PRECISION '3'"
        ).primaryExpression()
        assert isinstance(
            ast_double_precision, SqlBaseParser.TypeConstructorContext
        )
        self.visitor.visit(ast_double).assert_equals(
            self.visitor.visit(ast_double)
        )
        self.visitor.visit(ast_double).assert_equals(
            TypeConstructor("3", type_name="DOUBLE")
        )

    def test_row_constructor(self):
        ast_row_explicit = get_parser("ROW (1,2,3)").primaryExpression()
        assert isinstance(ast_row_explicit, SqlBaseParser.RowConstructorContext)
        ast_row_implicit = get_parser("(1,2,3)").primaryExpression()
        assert isinstance(ast_row_implicit, SqlBaseParser.RowConstructorContext)
        self.visitor.visit(ast_row_explicit).assert_equals(
            self.visitor.visit(ast_row_implicit)
        )
        self.visitor.visit(ast_row_explicit).assert_equals(
            RowConstructor([wrap_literal(1), wrap_literal(2), wrap_literal(3)])
        )

    def test_interval_constructor(self):
        ast = get_parser("INTERVAL '3' YEAR").primaryExpression()
        assert isinstance(ast, SqlBaseParser.IntervalLiteralContext)
        self.visitor.visit(ast).assert_equals(
            Interval("3", from_interval="YEAR")
        )

        ast = get_parser("INTERVAL -'3' YEAR").primaryExpression()
        assert isinstance(ast, SqlBaseParser.IntervalLiteralContext)
        self.visitor.visit(ast).assert_equals(
            Interval("-3", from_interval="YEAR")
        )

        ast = get_parser("INTERVAL -'-3' YEAR").primaryExpression()
        assert isinstance(ast, SqlBaseParser.IntervalLiteralContext)
        self.visitor.visit(ast).assert_equals(
            Interval("3", from_interval="YEAR")
        )

        ast = get_parser("INTERVAL -'+3' YEAR").primaryExpression()
        assert isinstance(ast, SqlBaseParser.IntervalLiteralContext)
        self.visitor.visit(ast).assert_equals(
            Interval("-3", from_interval="YEAR")
        )

        # This is 3 years and 100 months, which is simplified to 11-4 in Trino, but we don't take care of that.
        ast = get_parser("INTERVAL '3-100' YEAR TO MONTH").primaryExpression()
        assert isinstance(ast, SqlBaseParser.IntervalLiteralContext)
        self.visitor.visit(ast).assert_equals(
            Interval("3-100", from_interval="YEAR", to_interval="MONTH")
        )

    def test_lambda(self):
        ast = get_parser("a -> a + b - 5").primaryExpression()
        assert isinstance(ast, SqlBaseParser.Lambda_Context)
        self.visitor.visit(ast).assert_equals(
            Lambda(
                inputs=[Lambda.Variable("a")],
                expr=Lambda.Variable("a") + Field("b") - 5,
            )
        )

        # Just to be safe, this should fail if we replaced it properly:
        with pytest.raises(AssertionError):
            self.visitor.visit(ast).assert_equals(
                Lambda(
                    inputs=[Lambda.Variable("a")],
                    expr=Field("a") + Field("b") - 5,
                )
            )

        ast = get_parser("(b,c) -> b <> c").primaryExpression()
        assert isinstance(ast, SqlBaseParser.Lambda_Context)
        self.visitor.visit(ast).assert_equals(
            Lambda(
                inputs=[Lambda.Variable("b"), Lambda.Variable("c")],
                expr=Lambda.Variable("b") != Lambda.Variable("c"),
            )
        )


class TestConditionalFunctions(VisitorTest):
    def test_case(self):
        ast = get_parser(
            "CASE 1 WHEN 1 THEN 2 WHEN a THEN b ELSE c END"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.SimpleCaseContext)
        self.visitor.visit(ast).assert_equals(
            Case(
                value=1,
                branches=[When(1, 2), When(Field("a"), Field("b"))],
                else_=Else(Field("c")),
            )
        )

        ast = get_parser(
            "CASE WHEN 1 THEN 2 WHEN a THEN b ELSE c END"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.SearchedCaseContext)
        self.visitor.visit(ast).assert_equals(
            Case(
                value=None,
                branches=[When(1, 2), When(Field("a"), Field("b"))],
                else_=Else(Field("c")),
            )
        )
