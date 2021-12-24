from antlr4 import CommonTokenStream
from antlr4.InputStream import InputStream
from antlr4.tree.Trees import Trees
from antlr4.ParserRuleContext import ParserRuleContext
import nltk

try:
    from treeno.grammar.gen.SqlBaseLexer import SqlBaseLexer
    from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
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
        self.lexer = SqlBaseLexer(InputStream(data=self._sql))
        self.stream = CommonTokenStream(self.lexer)
        self.parser = SqlBaseParser(self.stream)

    @property
    def text(self):
        return self._sql

    def query(self):
        return self.parser.query()

    def expression(self):
        return self.parser.standaloneExpression()

    def type(self):
        return self.parser.standaloneType()

    def __str__(self):
        return Trees.toStringTree(self.root, None, self.parser)


def tree(ast: AST, node: ParserRuleContext) -> str:
    parenthetical_tree = nltk.Tree.fromstring(
        Trees.toStringTree(node, None, ast.parser)
    )
    return parenthetical_tree.pretty_print()
