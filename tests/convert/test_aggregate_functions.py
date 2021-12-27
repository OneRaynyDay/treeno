from treeno.datatypes.builder import (
    array,
    bigint,
    boolean,
    decimal,
    double,
    integer,
    interval,
    map_,
    row,
    timestamp,
    unknown,
    varchar,
)
from treeno.expression import (
    Add,
    AliasedStar,
    AliasedValue,
    And,
    Array,
    Between,
    Cast,
    DistinctFrom,
    Divide,
    Equal,
    Field,
    GreaterThan,
    GreaterThanOrEqual,
    InList,
    Interval,
    IsNull,
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
    Star,
    Subscript,
    TryCast,
    TypeConstructor,
    wrap_literal,
)
from treeno.functions.aggregate import (
    Arbitrary,
    ArrayAgg,
    Avg,
    BitwiseAndAgg,
    BitwiseOrAgg,
    BoolAnd,
    BoolOr,
    Checksum,
    CountIndication,
    Every,
    GeometricMean,
    ListAgg,
    Max,
    MaxBy,
    Min,
    MinBy,
    OverflowFiller,
    Sum,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.orderby import OrderTerm, OrderType
from treeno.relation import (
    AliasedRelation,
    Join,
    JoinConfig,
    JoinOnCriteria,
    JoinType,
    JoinUsingCriteria,
    Lateral,
    SelectQuery,
    Table,
    TableQuery,
    Unnest,
    ValuesQuery,
)
from treeno.window import (
    BoundedFrameBound,
    BoundType,
    CurrentFrameBound,
    FrameType,
    NullTreatment,
    UnboundedFrameBound,
    Window,
)

from .helpers import VisitorTest, get_parser


class TestFunction(VisitorTest):
    def test_complex_aggregate_expression(self):
        ast = get_parser(
            "SUM(a ORDER BY b ASC) FILTER (WHERE a <> b) IGNORE NULLS OVER (w PARTITION BY date ORDER BY a,b GROUPS 5 PRECEDING AND CURRENT ROW"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        window = Window(
            parent_window="w",
            orderby=[OrderTerm(Field("a")), OrderTerm(Field("b"))],
            partitions=[Field("date")],
            frame_type=FrameType.GROUPS,
            start_bound=BoundedFrameBound(
                bound_type=BoundType.PRECEDING, offset=5
            ),
            end_bound=CurrentFrameBound(),
        )
        assert self.visitor.visit(ast) == Sum(
            Field("a"),
            orderby=[OrderTerm(Field("b"), order_type=OrderType.ASC)],
            filter_=Field("a") != Field("b"),
            null_treatment=NullTreatment.IGNORE,
            window=window,
        )

    def test_aggregate_functions(self):
        # Check lowercase as an example
        ast = get_parser("Sum(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Sum(Field("a"))

        ast = get_parser("ARBITRARY(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Arbitrary(Field("a"))

        ast = get_parser("ARRAY_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ArrayAgg(Field("a"))

        ast = get_parser("AVG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Avg(Field("a"))

        ast = get_parser("BOOL_AND(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == BoolAnd(Field("a"))

        ast = get_parser("BOOL_OR(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == BoolOr(Field("a"))

        ast = get_parser("CHECKSUM(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Checksum(Field("a"))

        ast = get_parser("EVERY(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Every(Field("a"))

        ast = get_parser("GEOMETRIC_MEAN(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == GeometricMean(Field("a"))

    def test_min_max(self):
        ast = get_parser("MAX(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Max(Field("a"))

        ast = get_parser("MAX(a, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Max(Field("a"), Field("n"))

        ast = get_parser("MAX_BY(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MaxBy(Field("a"), Field("b"))

        ast = get_parser("MAX_BY(a, b, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MaxBy(
            Field("a"), Field("b"), Field("n")
        )

        ast = get_parser("MIN(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Min(Field("a"))

        ast = get_parser("MIN(a, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Min(Field("a"), Field("n"))

        ast = get_parser("MIN_BY(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MinBy(Field("a"), Field("b"))

        ast = get_parser("MIN_BY(a, b, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MinBy(
            Field("a"), Field("b"), Field("n")
        )

    def test_list_agg(self):
        ast = get_parser(
            "LISTAGG(a) WITHIN GROUP (ORDER BY a)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.ListaggContext)
        assert self.visitor.visit(ast) == ListAgg(
            Field("a"), orderby=[OrderTerm(value=Field("a"))]
        )

        ast = get_parser(
            "LISTAGG(a, 'abc' ON OVERFLOW TRUNCATE 'xyz' WITH COUNT) WITHIN GROUP (ORDER BY a)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.ListaggContext)
        assert self.visitor.visit(ast) == ListAgg(
            Field("a"),
            separator="abc",
            overflow_filler=OverflowFiller(
                count_indication=CountIndication.WITH_COUNT, filler="xyz"
            ),
            orderby=[OrderTerm(value=Field("a"))],
        )
