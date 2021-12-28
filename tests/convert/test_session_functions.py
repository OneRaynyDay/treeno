from treeno.functions.session import (
    CurrentCatalog,
    CurrentPath,
    CurrentSchema,
    CurrentUser,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.window import (
    BoundedFrameBound,
    BoundType,
    CurrentFrameBound,
    FrameType,
    NullTreatment,
    Window,
)

from .helpers import VisitorTest, get_parser


class TestFunction(VisitorTest):
    def test_special_datetime(self):
        ast = get_parser("CURRENT_USER").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CurrentUserContext)
        self.visitor.visit(ast).assert_equals(CurrentUser())

        ast = get_parser("CURRENT_CATALOG").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CurrentCatalogContext)
        self.visitor.visit(ast).assert_equals(CurrentCatalog())

        ast = get_parser("CURRENT_SCHEMA").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CurrentSchemaContext)
        self.visitor.visit(ast).assert_equals(CurrentSchema())

        ast = get_parser("CURRENT_PATH").primaryExpression()
        assert isinstance(ast, SqlBaseParser.CurrentPathContext)
        self.visitor.visit(ast).assert_equals(CurrentPath())
