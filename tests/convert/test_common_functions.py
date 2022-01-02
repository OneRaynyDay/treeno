from treeno.expression import Array, wrap_literal
from treeno.functions.common import Concatenate
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser

from .helpers import VisitorTest, get_parser


class TestFunction(VisitorTest):
    def test_concatenate(self):
        ast = get_parser("'2' || '3'").valueExpression()
        assert isinstance(ast, SqlBaseParser.ConcatenationContext)
        result = self.visitor.visit(ast)
        alternate_result = self.visitor.visit(
            get_parser("CONCAT('2', '3')").primaryExpression()
        )
        result.assert_equals(
            Concatenate(values=[wrap_literal("2"), wrap_literal("3")])
        )
        result.assert_equals(alternate_result)

        ast = get_parser("ARRAY[2,3,4] || 3").valueExpression()
        assert isinstance(ast, SqlBaseParser.ConcatenationContext)
        result = self.visitor.visit(ast)
        alternate_result = self.visitor.visit(
            get_parser("CONCAT(ARRAY[2,3,4], 3)").primaryExpression()
        )
        result.assert_equals(
            Concatenate(values=[Array([2, 3, 4]), wrap_literal(3)])
        )
        result.assert_equals(alternate_result)

        ast = get_parser("ARRAY[2,3,4] || ARRAY[3]").valueExpression()
        assert isinstance(ast, SqlBaseParser.ConcatenationContext)
        result = self.visitor.visit(ast)
        alternate_result = self.visitor.visit(
            get_parser("CONCAT(ARRAY[2,3,4], ARRAY[3])").primaryExpression()
        )
        result.assert_equals(Concatenate(values=[Array([2, 3, 4]), Array([3])]))
        result.assert_equals(alternate_result)

        ast = get_parser("ARRAY[ARRAY[2,3,4]] || ARRAY[3]").valueExpression()
        assert isinstance(ast, SqlBaseParser.ConcatenationContext)
        result = self.visitor.visit(ast)
        alternate_result = self.visitor.visit(
            get_parser(
                "CONCAT(ARRAY[ARRAY[2,3,4]], ARRAY[3])"
            ).primaryExpression()
        )
        result.assert_equals(
            Concatenate(values=[Array([Array([2, 3, 4])]), Array([3])])
        )
        result.assert_equals(alternate_result)
