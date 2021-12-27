from treeno.expression import Field
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.orderby import NullOrder, OrderTerm, OrderType

from .helpers import VisitorTest, get_parser


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
