from enum import Enum
from io import StringIO
from typing import Any

import typer
from nltk.tree import Tree

from treeno.base import PrintMode, PrintOptions, Sql
from treeno.builder.convert import (
    expression_from_sql,
    query_from_sql,
    type_from_sql,
)
from treeno.datatypes.types import DataType
from treeno.grammar.parse import AST
from treeno.grammar.parse import tree as print_tree
from treeno.util import children, is_dictlike, is_listlike

app = typer.Typer()


class SqlConstruct(str, Enum):
    QUERY = "query"
    EXPRESSION = "expression"
    TYPE = "type"


def get_sql_object(construct_type: SqlConstruct, sql: str) -> Sql:
    sql_object: Sql
    if construct_type == SqlConstruct.QUERY:
        sql_object = query_from_sql(sql)
    elif construct_type == SqlConstruct.EXPRESSION:
        sql_object = expression_from_sql(sql)
    elif construct_type == SqlConstruct.TYPE:
        sql_object = type_from_sql(sql)
    else:
        raise TypeError(f"Unexpected sql construct type {construct_type.value}")
    return sql_object


@app.command()
def format(construct_type: SqlConstruct, sql: str) -> None:
    sql_obj = get_sql_object(construct_type, sql)
    typer.echo(sql_obj.sql(PrintOptions(PrintMode.PRETTY)))


@app.command()
def antlr_tree(construct_type: SqlConstruct, sql: str) -> None:
    ast = AST(sql)
    if construct_type == SqlConstruct.QUERY:
        node = ast.query()
    elif construct_type == SqlConstruct.EXPRESSION:
        node = ast.expression()
    elif construct_type == SqlConstruct.TYPE:
        node = ast.type()
    else:
        raise TypeError(f"Unexpected sql construct type {construct_type.value}")
    typer.echo(print_tree(ast, node))


def treeify(node: Any) -> Any:
    if is_listlike(node):
        return Tree("list", [treeify(item) for item in node])
    if is_dictlike(node):
        return Tree("dict", [Tree(k, treeify(v)) for k, v in node.items()])
    if not isinstance(node, Sql):
        return Tree("terminal", [str(node)])
    # Data types are terminal and we don't want to print out the parameters when there is none
    if isinstance(node, DataType):
        return Tree("type", [str(node)])
    child_nodes = [
        Tree(k, treeify(v)) for k, v in children(node).items() if v is not None
    ]
    return Tree(node.__class__.__name__, child_nodes)


@app.command()
def tree(construct_type: SqlConstruct, sql: str, draw: bool = False) -> None:
    sql_obj = get_sql_object(construct_type, sql)
    tree = treeify(sql_obj)
    sio = StringIO()
    if not draw:
        tree.pretty_print(stream=sio)
        typer.echo(sio.getvalue())
    else:
        tree.draw()
