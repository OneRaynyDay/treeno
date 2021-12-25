from enum import Enum

import typer

from treeno.base import PrintMode, PrintOptions, Sql
from treeno.builder.convert import (
    expression_from_sql,
    query_from_sql,
    type_from_sql,
)
from treeno.grammar.parse import AST
from treeno.grammar.parse import tree as print_tree

app = typer.Typer()


class SqlConstruct(str, Enum):
    QUERY = "query"
    EXPRESSION = "expression"
    TYPE = "type"


@app.command()
def format(construct_type: SqlConstruct, sql: str) -> None:
    sql_object: Sql
    if construct_type == SqlConstruct.QUERY:
        sql_object = query_from_sql(sql)
    elif construct_type == SqlConstruct.EXPRESSION:
        sql_object = expression_from_sql(sql)
    elif construct_type == SqlConstruct.TYPE:
        sql_object = type_from_sql(sql)
    else:
        raise TypeError(f"Unexpected sql construct type {construct_type.value}")
    typer.echo(sql_object.sql(PrintOptions(PrintMode.PRETTY)))


@app.command()
def tree(construct_type: SqlConstruct, sql: str) -> None:
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
