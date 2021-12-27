import unittest

from antlr4 import CommonTokenStream
from antlr4.InputStream import InputStream

from treeno.builder.convert import ConvertVisitor
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
from treeno.grammar.gen.SqlBaseLexer import SqlBaseLexer
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
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


def get_parser(sql: str) -> SqlBaseParser:
    lexer = SqlBaseLexer(InputStream(data=sql))
    stream = CommonTokenStream(lexer)
    parser = SqlBaseParser(stream)
    return parser


class VisitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.visitor = ConvertVisitor()
