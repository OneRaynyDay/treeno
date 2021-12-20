"""A printer that follows a partial set of SQL formatting standards.
StatementPrinter complies with sqlstyle.guide.
"""
import attr
from treeno.base import Sql, PrintOptions
from typing import List, Tuple


@attr.s
class StatementPrinter:
    """Statement printer is responsible for formatting large hierarchical statements that usually consist of consecutive
    lines of <keyword> <sql expression>. For example:

    SELECT model_num
      FROM phones AS p
     WHERE p.release_date > '2014-09-30';

    Is a SelectQuery where the keywords are SELECT, FROM, and WHERE.

    TODO: Before we can use this, we have to add all the groupby functionalities
    """

    print_options: PrintOptions = attr.ib()
    # Buffer is a logical per-statement buffer which tries to right-adjust the keywords and left-adjust the sql entities
    buffer: List[Tuple[str, Sql]] = attr.ib(init=False)

    def add_statement(self, statement: str, sql: Sql) -> None:
        self.buffer.append((statement, sql))

    def to_string(self) -> str:
        rpad = max(len(line[0]) for line in self.buffer)
        string = ""
        for line in self.buffer:
            keyword, sql_expr = line
            with self.print_options as opts:
                sql_str = sql_expr.sql(opts)
            string += self.pad() + keyword.rjust(rpad) + " " + sql_str
