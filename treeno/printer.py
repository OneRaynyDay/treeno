"""A printer that follows a partial set of SQL formatting standards.
StatementPrinter complies with sqlstyle.guide.

Note that none of these printers are explicitly responsible for calling sql(). They should work only with the
primitive str representation.
"""
import attr
from treeno.base import PrintOptions, PrintMode
from typing import Dict


def pad(input: str, spaces: int) -> str:
    lines = input.splitlines()
    pad = " " * spaces
    return f"\n{pad}".join(lines)


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

    # Buffer is a logical per-statement buffer which tries to right-adjust the keywords and left-adjust the sql entities
    stmt_mapping: Dict[str, str] = attr.ib(factory=dict)

    def add_entry(self, key: str, value: str) -> None:
        assert (
            key not in self.stmt_mapping
        ), f"Key {key} already exists in statement printer"
        self.stmt_mapping[key] = value

    def update(self, new_mapping: Dict[str, str]) -> None:
        existing_keys = new_mapping.keys() & self.stmt_mapping.keys()
        assert not existing_keys, f"Keys {existing_keys} already exist"
        for key, value in new_mapping.items():
            self.add_entry(key, value)

    def to_string(self, opts: PrintOptions) -> str:
        rpad = max(len(line) for line in self.stmt_mapping)
        lines = []
        for keyword, sql_str in self.stmt_mapping.items():
            if opts.mode == PrintMode.PRETTY:
                padded_str = pad(sql_str, rpad + 1)
                lines.append(keyword.rjust(rpad) + " " + padded_str)
            elif opts.mode == PrintMode.DEFAULT:
                lines.append(keyword + " " + sql_str)
            else:
                raise NotImplementedError(
                    f"to_string not implemented for mode {opts.mode}"
                )

        # TODO: Consider appropriate join_char for other print modes
        join_char = "\n" if opts.mode == PrintMode.PRETTY else " "
        return join_char.join(lines)
