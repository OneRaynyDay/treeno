from treeno.expression import NULL, wrap_literal
from treeno.functions.conditional import Coalesce, If, NullIf, Try
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestFunction(VisitorTest):
    def test_functions(self):
        ast = get_parser("IF(TRUE, 1, 2)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            If(wrap_literal(True), wrap_literal(1), wrap_literal(2))
        )

        ast = get_parser("IF(TRUE, 1)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            If(wrap_literal(True), wrap_literal(1))
        )

        ast = get_parser("COALESCE(NULL, 0, 1)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            Coalesce([NULL, wrap_literal(0), wrap_literal(1)])
        )

        ast = get_parser("NULLIF(0, 1)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            NullIf(wrap_literal(0), wrap_literal(1))
        )

        ast = get_parser("NULLIF(0, 1)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(
            NullIf(wrap_literal(0), wrap_literal(1))
        )

        ast = get_parser("TRY(NULL + 1)").primaryExpression()
        assert isinstance(ast, SqlBaseParser.FunctionCallContext)
        self.visitor.visit(ast).assert_equals(Try(NULL + wrap_literal(1)))
