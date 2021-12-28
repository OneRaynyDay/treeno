from treeno.datatypes.builder import integer, varchar
from treeno.expression import (
    Add,
    AliasedStar,
    AliasedValue,
    Array,
    Field,
    Literal,
    Star,
    wrap_literal,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.groupby import GroupBy, GroupingSet
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

from .helpers import VisitorTest, get_parser


class TestRelation(VisitorTest):
    """Note that we leave select to its own test suite (TestSelect)
    since it's much more complicated
    """

    def test_table(self):
        ast = get_parser("foo.bar.baz").relationPrimary()
        assert isinstance(ast, SqlBaseParser.TableNameContext)
        self.visitor.visit(ast).assert_equals(
            Table("baz", schema="bar", catalog="foo")
        )

        ast = get_parser("bar.baz").relationPrimary()
        assert isinstance(ast, SqlBaseParser.TableNameContext)
        self.visitor.visit(ast).assert_equals(Table("baz", schema="bar"))

        ast = get_parser("baz").relationPrimary()
        assert isinstance(ast, SqlBaseParser.TableNameContext)
        self.visitor.visit(ast).assert_equals(Table("baz"))

    def test_subquery(self):
        ast = get_parser("(SELECT 1)").relationPrimary()
        assert isinstance(ast, SqlBaseParser.SubqueryRelationContext)
        self.visitor.visit(ast).assert_equals(
            SelectQuery(select=[Literal(1, data_type=integer())])
        )

    def test_unnest(self):
        ast = get_parser(
            "UNNEST(ARRAY [1,2,3], ARRAY['a','b','c'])"
        ).relationPrimary()
        assert isinstance(ast, SqlBaseParser.UnnestContext)
        self.visitor.visit(ast).assert_equals(
            Unnest(
                array=[
                    Array.from_values(
                        Literal(1, data_type=integer()),
                        Literal(2, data_type=integer()),
                        Literal(3, data_type=integer()),
                    ),
                    Array.from_values(
                        Literal("a", data_type=varchar(max_chars=1)),
                        Literal("b", data_type=varchar(max_chars=1)),
                        Literal("c", data_type=varchar(max_chars=1)),
                    ),
                ],
                with_ordinality=False,
            )
        )

        ast = get_parser(
            "UNNEST(some_column) WITH ORDINALITY"
        ).relationPrimary()
        assert isinstance(ast, SqlBaseParser.UnnestContext)
        self.visitor.visit(ast).assert_equals(
            Unnest(array=[Field("some_column")], with_ordinality=True)
        )

    def test_lateral(self):
        ast = get_parser("LATERAL (SELECT 1)").relationPrimary()
        assert isinstance(ast, SqlBaseParser.LateralContext)
        self.visitor.visit(ast).assert_equals(
            Lateral(
                subquery=SelectQuery(select=[Literal(1, data_type=integer())])
            )
        )

    def test_cross_join(self):
        ast = get_parser("a.b.c CROSS JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        self.visitor.visit(ast).assert_equals(
            Join(
                left_relation=Table("c", "b", "a"),
                right_relation=Table("z", "y", "x"),
                config=JoinConfig(join_type=JoinType.CROSS),
            )
        )

    def test_inner_on_join(self):
        ast = get_parser("a.b.c INNER JOIN x.y.z ON c.foo = z.bar").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        self.visitor.visit(ast).assert_equals(
            Join(
                left_relation=Table("c", "b", "a"),
                right_relation=Table("z", "y", "x"),
                config=JoinConfig(
                    join_type=JoinType.INNER,
                    criteria=JoinOnCriteria(
                        Field("foo", "c") == Field("bar", "z")
                    ),
                    natural=False,
                ),
            )
        )

    def test_outer_left_using_join(self):
        ast = get_parser(
            "a.b.c LEFT OUTER JOIN x.y.z USING (foo, bar)"
        ).relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        self.visitor.visit(ast).assert_equals(
            Join(
                left_relation=Table("c", "b", "a"),
                right_relation=Table("z", "y", "x"),
                config=JoinConfig(
                    join_type=JoinType.LEFT,
                    criteria=JoinUsingCriteria(["foo", "bar"]),
                    natural=False,
                ),
            )
        )

    def test_natural_full_join(self):
        ast = get_parser("a.b.c NATURAL FULL JOIN x.y.z").relation()
        assert isinstance(ast, SqlBaseParser.JoinRelationContext)
        self.visitor.visit(ast).assert_equals(
            Join(
                left_relation=Table("c", "b", "a"),
                right_relation=Table("z", "y", "x"),
                config=JoinConfig(join_type=JoinType.OUTER, natural=True),
            )
        )

    def test_alias(self):
        ast = get_parser("a.b.c AS foo (x,y,z)").aliasedRelation()
        assert isinstance(ast, SqlBaseParser.AliasedRelationContext)
        self.visitor.visit(ast).assert_equals(
            AliasedRelation(
                relation=Table("c", "b", "a"),
                alias="foo",
                column_aliases=["x", "y", "z"],
            )
        )


class TestSelectItems(VisitorTest):
    def test_select_star(self):
        ast = get_parser("*").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectAllContext)
        self.visitor.visit(ast).assert_equals(Star())

        ast = get_parser("a.*").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectAllContext)
        self.visitor.visit(ast).assert_equals(Star(table="a"))

        ast = get_parser("a.* AS (x,y,z)").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectAllContext)
        self.visitor.visit(ast).assert_equals(
            AliasedStar(star=Star("a"), aliases=["x", "y", "z"])
        )

    def test_select_single(self):
        ast = get_parser("1+2+3").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectSingleContext)
        self.visitor.visit(ast).assert_equals(
            Add(
                left=Add(
                    left=Literal(1, data_type=integer()),
                    right=Literal(2, data_type=integer()),
                ),
                right=Literal(3, data_type=integer()),
            )
        )

        ast = get_parser("a").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectSingleContext)
        self.visitor.visit(ast).assert_equals(Field(name="a"))

        ast = get_parser("a AS foo").selectItem()
        assert isinstance(ast, SqlBaseParser.SelectSingleContext)
        self.visitor.visit(ast).assert_equals(
            AliasedValue(value=Field(name="a"), alias="foo")
        )


class TestSelect(VisitorTest):
    def test_simple_selects(self):
        ast = get_parser("SELECT tbl.a").query()
        assert isinstance(ast, SqlBaseParser.QueryContext)
        field = Field(name="a", table="tbl")
        query = SelectQuery(select=[field])
        self.visitor.visit(ast).assert_equals(query)

        ast = get_parser("SELECT tbl.a FROM tbl").query()
        assert isinstance(ast, SqlBaseParser.QueryContext)
        query.from_ = Table(name="tbl")
        self.visitor.visit(ast).assert_equals(query)

        ast = get_parser("SELECT tbl.a FROM tbl WHERE tbl.a > 5").query()
        assert isinstance(ast, SqlBaseParser.QueryContext)
        query.where = field > 5
        self.visitor.visit(ast).assert_equals(query)

        ast = get_parser(
            "SELECT tbl.a FROM tbl WHERE tbl.a > 5 GROUP BY date"
        ).query()
        date_field = Field(name="date")
        assert isinstance(ast, SqlBaseParser.QueryContext)
        query.groupby = GroupBy([GroupingSet([date_field])])
        self.visitor.visit(ast).assert_equals(query)

        ast = get_parser(
            "SELECT tbl.a FROM tbl WHERE tbl.a > 5 GROUP BY date ORDER BY tbl.a ASC"
        ).query()
        assert isinstance(ast, SqlBaseParser.QueryContext)
        query.orderby = [OrderTerm(field, order_type=OrderType.ASC)]
        self.visitor.visit(ast).assert_equals(query)


class TestQueryPrimary(VisitorTest):
    def test_table(self):
        ast = get_parser("TABLE foo.bar").queryPrimary()
        assert isinstance(ast, SqlBaseParser.TableContext)
        self.visitor.visit(ast).assert_equals(
            TableQuery(Table(name="bar", schema="foo"))
        )

    def test_values(self):
        ast = get_parser("VALUES 1,2,3").queryPrimary()
        assert isinstance(ast, SqlBaseParser.InlineTableContext)
        self.visitor.visit(ast).assert_equals(
            ValuesQuery(
                exprs=[wrap_literal(1), wrap_literal(2), wrap_literal(3)]
            )
        )

    def test_select(self):
        ast = get_parser("SELECT 1, foo").queryPrimary()
        assert isinstance(ast, SqlBaseParser.QueryPrimaryDefaultContext)
        self.visitor.visit(ast).assert_equals(
            SelectQuery(select=[wrap_literal(1), Field("foo")])
        )
