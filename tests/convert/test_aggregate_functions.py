from treeno.expression import Field
from treeno.functions.aggregate import (
    ApproxDistinct,
    ApproxMostFrequent,
    ApproxPercentile,
    ApproxSet,
    Arbitrary,
    ArrayAgg,
    Avg,
    BitwiseAndAgg,
    BitwiseOrAgg,
    BoolAnd,
    BoolOr,
    Checksum,
    Corr,
    CountIndication,
    CovarPop,
    CovarSamp,
    Every,
    GeometricMean,
    Histogram,
    Kurtosis,
    ListAgg,
    MapAgg,
    MapUnion,
    Max,
    MaxBy,
    Merge,
    Min,
    MinBy,
    MultiMapAgg,
    NumericHistogram,
    OverflowFiller,
    QDigestAgg,
    RegrIntercept,
    RegrSlope,
    Skewness,
    StdDev,
    StdDevPop,
    StdDevSamp,
    Sum,
    TDigestAgg,
    Variance,
    VarPop,
    VarSamp,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.orderby import OrderTerm, OrderType
from treeno.window import (
    BoundedFrameBound,
    BoundType,
    CurrentFrameBound,
    FrameType,
    NullTreatment,
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
        self.visitor.visit(ast).assert_equals(
            Sum(
                Field("a"),
                orderby=[OrderTerm(Field("b"), order_type=OrderType.ASC)],
                filter_=Field("a") != Field("b"),
                null_treatment=NullTreatment.IGNORE,
                window=window,
            )
        )

    def test_aggregate_functions(self):
        # Check lowercase as an example
        ast = get_parser("Sum(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Sum(Field("a")))

        ast = get_parser("ARBITRARY(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Arbitrary(Field("a")))

        ast = get_parser("ARRAY_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ArrayAgg(Field("a")))

        ast = get_parser("AVG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Avg(Field("a")))

        ast = get_parser("BOOL_AND(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(BoolAnd(Field("a")))

        ast = get_parser("BOOL_OR(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(BoolOr(Field("a")))

        ast = get_parser("CHECKSUM(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Checksum(Field("a")))

        ast = get_parser("EVERY(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Every(Field("a")))

        ast = get_parser("GEOMETRIC_MEAN(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(GeometricMean(Field("a")))

        ast = get_parser("BITWISE_AND_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(BitwiseAndAgg(Field("a")))

        ast = get_parser("BITWISE_OR_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(BitwiseOrAgg(Field("a")))

        ast = get_parser("HISTOGRAM(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Histogram(Field("a")))

        ast = get_parser("MAP_AGG(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(MapAgg(Field("a"), Field("b")))

        ast = get_parser("MAP_UNION(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(MapUnion(Field("a")))

        ast = get_parser("MULTIMAP_AGG(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            MultiMapAgg(Field("a"), Field("b"))
        )

        ast = get_parser("APPROX_DISTINCT(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ApproxDistinct(Field("a")))

        ast = get_parser("APPROX_DISTINCT(a, eps)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            ApproxDistinct(Field("a"), Field("eps"))
        )

        ast = get_parser(
            "APPROX_MOST_FREQUENT(buckets, value, capacity)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            ApproxMostFrequent(
                Field("buckets"), Field("value"), Field("capacity")
            )
        )

        ast = get_parser("APPROX_SET(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ApproxSet(Field("a")))

        ast = get_parser("MERGE(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Merge(Field("a")))

        ast = get_parser("NUMERIC_HISTOGRAM(buckets, a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            NumericHistogram(Field("buckets"), Field("a"))
        )

        ast = get_parser(
            "NUMERIC_HISTOGRAM(buckets, a, weight)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            NumericHistogram(Field("buckets"), Field("a"), Field("weight"))
        )

        ast = get_parser("QDIGEST_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(QDigestAgg(Field("a")))

        ast = get_parser("QDIGEST_AGG(a, weight)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            QDigestAgg(Field("a"), Field("weight"))
        )

        ast = get_parser("QDIGEST_AGG(a, weight, accuracy)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            QDigestAgg(Field("a"), Field("weight"), Field("accuracy"))
        )

        ast = get_parser("TDIGEST_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(TDigestAgg(Field("a")))

        ast = get_parser("TDIGEST_AGG(a, weight)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            TDigestAgg(Field("a"), Field("weight"))
        )

    def test_approx_percentile(self):
        ast = get_parser("APPROX_PERCENTILE(a, percent)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            ApproxPercentile(Field("a"), Field("percent"))
        )

        ast = get_parser(
            "APPROX_PERCENTILE(a, percent, weight)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            ApproxPercentile(Field("a"), Field("percent"), Field("weight"))
        )

    def test_min_max(self):
        ast = get_parser("MAX(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Max(Field("a")))

        ast = get_parser("MAX(a, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Max(Field("a"), Field("n")))

        ast = get_parser("MAX_BY(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(MaxBy(Field("a"), Field("b")))

        ast = get_parser("MAX_BY(a, b, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            MaxBy(Field("a"), Field("b"), Field("n"))
        )

        ast = get_parser("MIN(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Min(Field("a")))

        ast = get_parser("MIN(a, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Min(Field("a"), Field("n")))

        ast = get_parser("MIN_BY(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(MinBy(Field("a"), Field("b")))

        ast = get_parser("MIN_BY(a, b, n)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            MinBy(Field("a"), Field("b"), Field("n"))
        )

    def test_list_agg(self):
        ast = get_parser(
            "LISTAGG(a) WITHIN GROUP (ORDER BY a)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.ListaggContext)
        self.visitor.visit(ast).assert_equals(
            ListAgg(Field("a"), orderby=[OrderTerm(value=Field("a"))])
        )

        ast = get_parser(
            "LISTAGG(a, 'abc' ON OVERFLOW TRUNCATE 'xyz' WITH COUNT) WITHIN GROUP (ORDER BY a)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.ListaggContext)
        self.visitor.visit(ast).assert_equals(
            ListAgg(
                Field("a"),
                separator="abc",
                overflow_filler=OverflowFiller(
                    count_indication=CountIndication.WITH_COUNT, filler="xyz"
                ),
                orderby=[OrderTerm(value=Field("a"))],
            )
        )

    def test_statistics(self):
        ast = get_parser("CORR(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Corr(Field("y"), Field("x")))

        ast = get_parser("COVAR_POP(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(CovarPop(Field("y"), Field("x")))

        ast = get_parser("COVAR_SAMP(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(CovarSamp(Field("y"), Field("x")))

        ast = get_parser("KURTOSIS(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Kurtosis(Field("y"), Field("x")))

        ast = get_parser("REGR_INTERCEPT(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            RegrIntercept(Field("y"), Field("x"))
        )

        ast = get_parser("REGR_SLOPE(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(RegrSlope(Field("y"), Field("x")))

        ast = get_parser("SKEWNESS(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Skewness(Field("x")))

        ast = get_parser("STDDEV(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(StdDev(Field("x")))

        ast = get_parser("STDDEV_POP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(StdDevPop(Field("x")))

        ast = get_parser("STDDEV_SAMP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(StdDevSamp(Field("x")))

        ast = get_parser("VARIANCE(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Variance(Field("x")))

        ast = get_parser("VAR_POP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(VarPop(Field("x")))

        ast = get_parser("VAR_SAMP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(VarSamp(Field("x")))
