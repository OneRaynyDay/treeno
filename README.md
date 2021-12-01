# Treeno

A user friendly query tree building library for Trino.

Crafting SQL commands is often a duplicated art.
A standard library of common SQL functions is necessary to reduce common boilerplate.
Treeno is a way for standard library authors to build on top of arbitrary queries to
aggregate, filter, join, etc on top of Trino SQL.

Treeno is interoperable with raw SQL, so users don't need to be aware of `treeno.relation.Query` objects (`SelectQuery`, `TableQuery`, etc):

```python
from treeno.builder.convert import query_from_ast
from treeno.grammar.parse import AST

ast = AST("SELECT * FROM table")
select_query = query_from_ast(ast)
```

In addition, Treeno is able to parse standalone types:

```python
from treeno.builder.convert import type_from_ast
from treeno.grammar.parse import AST

ast = AST("TIMESTAMP(9) WITH TIME ZONE")
data_type = type_from_ast(ast)
```

... and arbitrary expressions:

```python
from treeno.builder.convert import type_from_ast
from treeno.grammar.parse import AST

ast = AST("(3 + 5) / 7 ")
data_type = type_from_ast(ast)
```
