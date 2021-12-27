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
