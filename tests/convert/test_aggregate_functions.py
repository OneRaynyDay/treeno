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

        ast = get_parser("BITWISE_AND_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == BitwiseAndAgg(Field("a"))

        ast = get_parser("BITWISE_OR_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == BitwiseOrAgg(Field("a"))

        ast = get_parser("HISTOGRAM(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Histogram(Field("a"))

        ast = get_parser("MAP_AGG(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MapAgg(Field("a"), Field("b"))

        ast = get_parser("MAP_UNION(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MapUnion(Field("a"))

        ast = get_parser("MULTIMAP_AGG(a, b)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == MultiMapAgg(Field("a"), Field("b"))

        ast = get_parser("APPROX_DISTINCT(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ApproxDistinct(Field("a"))

        ast = get_parser("APPROX_DISTINCT(a, eps)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ApproxDistinct(
            Field("a"), Field("eps")
        )

        ast = get_parser(
            "APPROX_MOST_FREQUENT(buckets, value, capacity)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ApproxMostFrequent(
            Field("buckets"), Field("value"), Field("capacity")
        )

        ast = get_parser("APPROX_SET(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ApproxSet(Field("a"))

        ast = get_parser("MERGE(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Merge(Field("a"))

        ast = get_parser("NUMERIC_HISTOGRAM(buckets, a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == NumericHistogram(
            Field("buckets"), Field("a")
        )

        ast = get_parser(
            "NUMERIC_HISTOGRAM(buckets, a, weight)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == NumericHistogram(
            Field("buckets"), Field("a"), Field("weight")
        )

        ast = get_parser("QDIGEST_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == QDigestAgg(Field("a"))

        ast = get_parser("QDIGEST_AGG(a, weight)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == QDigestAgg(
            Field("a"), Field("weight")
        )

        ast = get_parser("QDIGEST_AGG(a, weight, accuracy)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == QDigestAgg(
            Field("a"), Field("weight"), Field("accuracy")
        )

        ast = get_parser("TDIGEST_AGG(a)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == TDigestAgg(Field("a"))

        ast = get_parser("TDIGEST_AGG(a, weight)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == TDigestAgg(
            Field("a"), Field("weight")
        )

    def test_approx_percentile(self):
        ast = get_parser("APPROX_PERCENTILE(a, percent)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ApproxPercentile(
            Field("a"), Field("percent")
        )

        ast = get_parser(
            "APPROX_PERCENTILE(a, percent, weight)"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == ApproxPercentile(
            Field("a"), Field("percent"), Field("weight")
        )

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

    def test_statistics(self):
        ast = get_parser("CORR(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Corr(Field("y"), Field("x"))

        ast = get_parser("COVAR_POP(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == CovarPop(Field("y"), Field("x"))

        ast = get_parser("COVAR_SAMP(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == CovarSamp(Field("y"), Field("x"))

        ast = get_parser("KURTOSIS(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Kurtosis(Field("y"), Field("x"))

        ast = get_parser("REGR_INTERCEPT(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == RegrIntercept(Field("y"), Field("x"))

        ast = get_parser("REGR_SLOPE(y, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == RegrSlope(Field("y"), Field("x"))

        ast = get_parser("SKEWNESS(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Skewness(Field("x"))

        ast = get_parser("STDDEV(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Skewness(Field("x"))

        ast = get_parser("STDDEV_POP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == StdDevPop(Field("x"))

        ast = get_parser("STDDEV_SAMP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == StdDevSamp(Field("x"))

        ast = get_parser("VARIANCE(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == Variance(Field("x"))

        ast = get_parser("VAR_POP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == VarPop(Field("x"))

        ast = get_parser("VAR_SAMP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        assert self.visitor.visit(ast) == VarSamp(Field("x"))
