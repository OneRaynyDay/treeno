# Treeno

A user friendly query tree building library for Trino.

Crafting SQL commands is often a duplicated art.
A standard library of common SQL functions is necessary to reduce common boilerplate.
Treeno is a way for standard library authors to build on top of arbitrary queries to
aggregate, filter, join, etc on top of Trino SQL.

Treeno is interoperable with raw SQL, so users don't need to be aware of `treeno.relation.Query` objects (`SelectQuery`, `TableQuery`, etc):

```python
from treeno.builder.convert import query_from_sql
select_query = query_from_sql("SELECT * FROM table")
```

In addition, Treeno is able to parse standalone types:

```python
from treeno.builder.convert import type_from_sql
data_type = type_from_sql("TIMESTAMP(9) WITH TIME ZONE")
```

... and arbitrary expressions:

```python
from treeno.builder.convert import expression_from_sql
data_type = expression_from_sql("(3 + 5) / 7 ")
```
