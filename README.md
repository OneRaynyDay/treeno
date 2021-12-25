# Treeno

![Tests](https://github.com/OneRaynyDay/treeno/actions/workflows/python-tests.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>


A user friendly query tree building library for Trino, a distributed SQL engine.
It supports Python>=3.6.

Crafting SQL commands is often a duplicated art.
A standard library of common SQL functions is necessary to reduce common boilerplate.
Treeno is a way for standard library authors to build on top of arbitrary queries to
aggregate, filter, join, etc on top of Trino SQL.

# Install

```
$ pip install trino
```

# Quick Start

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
