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
from treeno.groupby import Cube, GroupingSet, GroupingSetList, Rollup
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
