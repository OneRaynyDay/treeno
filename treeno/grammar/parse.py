from io import StringIO

import nltk
from antlr4 import CommonTokenStream
from antlr4.InputStream import InputStream
from antlr4.ParserRuleContext import ParserRuleContext
from antlr4.tree.Trees import Trees

try:
    from treeno.grammar.gen.SqlBaseLexer import SqlBaseLexer  # type: ignore
    from treeno.grammar.gen.SqlBaseParser import SqlBaseParser  # type: ignore
except ImportError:
    print(
        "SqlBaseLexer and SqlBaseParser not found. Did you run `python setup.py install` or `python setup.py "
        "develop` first?"
    )
    raise


class AST:
    """Basic building block for SQL parse tree. This is not meant to be used directly, but is rather an intermediate
    layer to operate on to create a pypika interface."""

    def __init__(self, sql):
        self._sql = sql

    def parser(self) -> SqlBaseParser:
        lexer = SqlBaseLexer(InputStream(data=self._sql))
        stream = CommonTokenStream(lexer)
        return SqlBaseParser(stream)

    @property
    def text(self):
        return self._sql

    def query(self):
        return self.parser().query()

    def expression(self):
        return self.parser().standaloneExpression()

    def type(self):
        return self.parser().standaloneType()


def tree(ast: AST, node: ParserRuleContext) -> str:
    parenthetical_tree = nltk.Tree.fromstring(
        Trees.toStringTree(node, None, ast.parser())
    )
    sio = StringIO()
    parenthetical_tree.pretty_print(stream=sio)
    return sio.getvalue()
