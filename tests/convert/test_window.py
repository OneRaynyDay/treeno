import unittest

from helpers import VisitorTest, get_parser

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
from treeno.orderby import NullOrder, OrderTerm, OrderType
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
                offset=Literal(5, data_type=integer()),
                bound_type=BoundType.FOLLOWING,
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


if __name__ == "__main__":
    unittest.main()
