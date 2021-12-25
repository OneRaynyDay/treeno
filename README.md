# Treeno

![Tests](https://github.com/OneRaynyDay/treeno/actions/workflows/python-tests.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>


A user friendly query tree building library for [Trino](https://trino.io/), a distributed SQL query engine.
Treeno supports `python>=3.6`, and aims to support Trino `v367` grammar. **This project is currently in development and new features are added frequently.**


# Install

```
$ pip install trino
```

# Quick Start

## CLI
You can use `treeno` to format your SQL queries:

```shell
❯ treeno format query "SELECT a,b FROM (SELECT a, b, c FROM t WHERE c > 5 AND b = 2 ORDER BY a) LIMIT 3;"
SELECT "a","b"
  FROM (SELECT "a","b","c"
          FROM "t"
         WHERE "c" > 5
               AND "b" = 2
         ORDER BY "a")
 LIMIT 3
```

`treeno tree` allows you to view the SQL syntax as a tree to better understand your query structure:

```shell
❯ treeno tree expression "1+2"
          standaloneExpres
                sion
   ______________|________________
  |                           expression
  |                               |
  |                        booleanExpressio
  |                               n
  |                               |
  |                        valueExpression
  |     __________________________|________________
  |    |                   valueExpression  valueExpression
  |    |                          |                |
  |    |                   primaryExpressio primaryExpressio
  |    |                          n                n
  |    |                          |                |
  |    |                        number           number
  |    |                          |                |
<EOF>  +                          1                2
```

## Library

`treeno.builder.convert` supplies three useful functions `query_from_sql`, `expression_from_sql` and `type_from_sql`,
which allows us to parse Trino SQL into python data types.

Underneath the hood, every expression is a `Value`, which has a `sql()` function which gives us the string
representation of the query.

### Support for joins:

```python
>>> query = query_from_sql("SELECT foo.a, t.b FROM t INNER JOIN foo ON foo.a = t.b")
>>> print(query.sql(PrintOptions(PrintMode.PRETTY)))
SELECT "foo"."a","t"."b"
  FROM "t" INNER JOIN "foo" ON "foo"."a" = "t"."b"
>>> print(str(query))
SELECT "foo"."a","t"."b" FROM "t" INNER JOIN "foo" ON "foo"."a" = "t"."b"
```

### Support for basic window functions:

```python
>>> query = query_from_sql("SELECT SUM(a) OVER (PARTITION BY date ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), x, y, z FROM t")
>>> print(query.sql(PrintOptions(PrintMode.PRETTY)))
SELECT SUM("a") OVER (
       PARTITION BY "date"
           ORDER BY "timestamp"
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
       "x","y","z"
  FROM "t"
>>> print(query)
SELECT SUM("a") OVER (PARTITION BY "date" ORDER BY "timestamp" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),"x","y","z" FROM "t"
```

### Support for subqueries:

```python
>>> query = query_from_sql("SELECT a FROM (SELECT a,b FROM (SELECT a,b,c FROM t))")
>>> print(str(query))
SELECT "a" FROM (SELECT "a","b" FROM (SELECT "a","b","c" FROM "t"))
>>> print(query.sql(PrintOptions(PrintMode.PRETTY)))
SELECT "a"
  FROM (SELECT "a","b"
          FROM (SELECT "a","b","c"
                  FROM "t"))
```

### Support for CTE's:

```python
>>> query = query_from_sql("WITH foo AS (SELECT a,b FROM t) SELECT foo.a")
>>> print(query.sql(PrintOptions(PrintMode.PRETTY)))
  WITH "foo" AS (
       SELECT "a","b"
         FROM "t")
SELECT "foo"."a"
>>> print(str(query))
WITH "foo" AS (SELECT "a","b" FROM "t") SELECT "foo"."a"
```

### Support for all pemdas:

```python
>>> query = query_from_sql("SELECT (((1+2)*3)/4)-10")
>>> print(str(query)) # only + needs parenthesizing
SELECT (1 + 2) * 3 / 4 - 10
>> query = query_from_sql("SELECT (TRUE OR TRUE) AND FALSE")
>>> print(str(query)) # AND has precedence, so we preserve the parentheses
SELECT (TRUE OR TRUE) AND FALSE
>>> query = query_from_sql("SELECT TRUE OR (TRUE AND FALSE)")
>>> print(str(query)) # AND has precedence so parentheses are redundant
SELECT TRUE OR TRUE AND FALSE
```

### `treeno` is type-aware:

```python
>>> query = query_from_sql("SELECT CAST(3 AS DECIMAL(29,1))")
>>> print(str(query))
SELECT CAST(3 AS DECIMAL(29,1))
```

The above decimal type can be constructed in python using the `treeno.datatypes.builder` module:

```python
>>> from treeno.datatypes.builder import decimal
>>> decimal(precision=29, scale=1)
DataType(type_name='DECIMAL', parameters={'precision': 29, 'scale': 1})
```

## Links

PyPI Releases: https://pypi.org/project/treeno/
