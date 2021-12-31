import pytest

from treeno.datatypes.builder import varchar
from treeno.expression import Field, Literal, wrap_literal
from treeno.functions.datetime import (
    DEFAULT_DATETIME_PRECISION,
    AtTimezone,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Date,
    DateAdd,
    DateDiff,
    DateTrunc,
    FromISO8601Date,
    FromISO8601Timestamp,
    FromISO8601TimestampNanos,
    FromUnixtime,
    FromUnixtimeNanos,
    HumanReadableSeconds,
    LastDayOfMonth,
    LocalTime,
    LocalTimestamp,
    Now,
    ParseDuration,
    ToISO8601,
    ToMilliseconds,
    ToUnixtime,
    WithTimezone,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestFunction(VisitorTest):
    def test_special_datetime(self):
        ast = get_parser("CURRENT_DATE").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(CurrentDate())

        ast = get_parser("CURRENT_TIMESTAMP").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            CurrentTimestamp(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("CURRENT_TIMESTAMP(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(CurrentTimestamp(9))

        ast = get_parser("CURRENT_TIMESTAMP(13)").primaryExpression()
        with pytest.raises(
            AssertionError, match="Precision of 13 is not supported"
        ):
            self.visitor.visit(ast)

        ast = get_parser("CURRENT_TIME").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            CurrentTime(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("CURRENT_TIME(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(CurrentTime(9))

        ast = get_parser("LOCALTIME").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            LocalTime(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("LOCALTIME(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(LocalTime(9))

        ast = get_parser("LOCALTIMESTAMP").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(
            LocalTimestamp(DEFAULT_DATETIME_PRECISION)
        )

        ast = get_parser("LOCALTIMESTAMP(9)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.SpecialDateTimeFunctionContext)
        self.visitor.visit(ast).assert_equals(LocalTimestamp(9))

    def test_from_unix_time(self):
        ast = get_parser("FROM_UNIXTIME(a, zone)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            FromUnixtime(value=Field("a"), zone=Field("zone"))
        )

        ast = get_parser("FROM_UNIXTIME(a, hrs, mins)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            FromUnixtime(
                value=Field("a"), hours=Field("hrs"), minutes=Field("mins")
            )
        )

    def test_functions(self):
        ast = get_parser("DATE(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Date(value=Field("x")))
        ast = get_parser("LAST_DAY_OF_MONTH(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(LastDayOfMonth(value=Field("x")))
        ast = get_parser("FROM_ISO8601_TIMESTAMP(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            FromISO8601Timestamp(value=Field("x"))
        )
        ast = get_parser("FROM_ISO8601_TIMESTAMP_NANOS(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            FromISO8601TimestampNanos(value=Field("x"))
        )
        ast = get_parser("FROM_ISO8601_DATE(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(FromISO8601Date(value=Field("x")))
        ast = get_parser(
            "AT_TIMEZONE(x, 'America/New_York')"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            AtTimezone(
                value=Field("x"),
                zone=Literal(
                    "America/New_York", data_type=varchar(max_chars=16)
                ),
            )
        )
        ast = get_parser(
            "WITH_TIMEZONE(x, 'America/New_York'"
        ).primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            WithTimezone(
                value=Field("x"),
                zone=Literal(
                    "America/New_York", data_type=varchar(max_chars=16)
                ),
            )
        )
        ast = get_parser("FROM_UNIXTIME_NANOS(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            FromUnixtimeNanos(value=Field("x"))
        )
        ast = get_parser("NOW()").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Now())
        ast = get_parser("TO_ISO8601(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ToISO8601(value=Field("x")))
        ast = get_parser("TO_MILLISECONDS(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ToMilliseconds(value=Field("x")))
        ast = get_parser("TO_UNIXTIME(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ToUnixtime(value=Field("x")))
        ast = get_parser("DATE_TRUNC('quarter', x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            DateTrunc(
                unit=Literal("quarter", data_type=varchar(max_chars=7)),
                value=Field("x"),
            )
        )
        ast = get_parser("DATE_ADD('second', 12, x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            DateAdd(
                unit=Literal("second", data_type=varchar(max_chars=6)),
                value=wrap_literal(12),
                timestamp=Field("x"),
            )
        )
        ast = get_parser("DATE_DIFF('second', x, y)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            DateDiff(
                unit=Literal("second", data_type=varchar(max_chars=6)),
                timestamp1=Field("x"),
                timestamp2=Field("y"),
            )
        )

        ast = get_parser("PARSE_DURATION(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(ParseDuration(value=Field("x")))
        ast = get_parser("HUMAN_READABLE_SECONDS(x)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            HumanReadableSeconds(value=Field("x"))
        )
