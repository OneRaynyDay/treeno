from treeno.expression import Add, Field
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.groupby import Cube, GroupingSet, GroupingSetList, Rollup

from .helpers import VisitorTest, get_parser


class TestGroupBy(VisitorTest):
    def test_grouping_set(self):
        ast = get_parser("(a+b,c)").groupingSet()
        assert isinstance(ast, SqlBaseParser.GroupingSetContext)
        group_expr = GroupingSet(
            [Add(left=Field("a"), right=Field("b")), Field("c")]
        )
        self.visitor.visit(ast).assert_equals(group_expr)

    def test_multiple_grouping_sets(self):
        ast = get_parser("GROUPING SETS ((a,b), c)").groupingElement()
        assert isinstance(ast, SqlBaseParser.MultipleGroupingSetsContext)
        groups_expr = GroupingSetList(
            [GroupingSet([Field("a"), Field("b")]), GroupingSet([Field("c")])]
        )
        self.visitor.visit(ast).assert_equals(groups_expr)

    def test_cube(self):
        ast = get_parser("CUBE (a,b)").groupingElement()
        assert isinstance(ast, SqlBaseParser.CubeContext)
        cube_expr = Cube([Field("a"), Field("b")])
        self.visitor.visit(ast).assert_equals(cube_expr)

    def test_rollup(self):
        ast = get_parser("ROLLUP (a,b)").groupingElement()
        assert isinstance(ast, SqlBaseParser.RollupContext)
        rollup_expr = Rollup([Field("a"), Field("b")])
        self.visitor.visit(ast).assert_equals(rollup_expr)
