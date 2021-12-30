"""A printer that follows a partial set of SQL formatting standards.
StatementPrinter complies with sqlstyle.guide.

Note that none of these printers are explicitly responsible for calling sql(). They should work only with the
primitive str representation.
"""
from typing import Dict, List

import attr

from treeno.base import PrintMode, PrintOptions


def pad(input: str, spaces: int) -> str:
    """Pads the input lines with spaces except for the first line.
    We don't need opts here because the default mode will always be a single line so we'll never invoke this.
    """
    lines = input.splitlines()
    pad = " " * spaces
    return f"\n{pad}".join(lines)


def join_stmts(
    stmts: List[str], opts: PrintOptions, delimiter: str = ","
) -> str:
    """A convenience method for joining statements when the entire stmt list is already known"""
    return JoinPrinter(delimiter=delimiter, stmt_list=stmts).to_string(opts)


@attr.s
class JoinPrinter:
    """JoinPrinter is responsible for formatting a sequence of strings. For example:

    SELECT abc, 123, -- gets wrapped over
           def, ghij
    """

    delimiter: str = attr.ib()
    stmt_list: List[str] = attr.ib(factory=list)
    max_length: int = attr.ib(default=80)

    def add_entry(self, value: str) -> None:
        self.stmt_list.append(value)

    def update(self, values: List[str]) -> None:
        self.stmt_list.extend(values)

    def to_string(self, opts: PrintOptions) -> str:
        assert opts.spaces < self.max_length
        remaining_length = self.max_length - opts.spaces
        lines = []
        current_length = 0
        for idx, line in enumerate(self.stmt_list):
            # If we're at the last element, we don't need to add a comma
            needs_comma = int(idx < len(self.stmt_list) - 1)
            line_length = len(line) + needs_comma
            # If a line is simply too long, we can't do anything about it but to include it in its own line.
            if (
                opts.mode == PrintMode.PRETTY
                and current_length + line_length > remaining_length
            ):
                current_length = line_length
                # If we're at the beginning of the line, we shouldn't add a newline
                newline_if_not_beginning = "\n" if idx != 0 else ""
                lines.append(newline_if_not_beginning + line)
            else:
                # Add 1 because of commas
                current_length += line_length
                lines.append(line)
        return self.delimiter.join(lines)


@attr.s
class StatementPrinter:
    """StatementPrinter is responsible for formatting large hierarchical statements that usually consist of consecutive
    lines of <keyword> <sql expression>. For example:

    SELECT model_num
      FROM phones AS p
     WHERE p.release_date > '2014-09-30';

    The keywords are right adjusted to form a "river" for better readability
    """

    # Buffer is a logical per-statement buffer which tries to right-adjust the keywords and left-adjust the sql entities
    stmt_mapping: Dict[str, str] = attr.ib(factory=dict)
    river: bool = attr.ib(default=True)

    def add_entry(self, key: str, value: str) -> None:
        """Adds an entry to the statement.
        Note that if value is an empty string, we will omit it from the formatting later on.
        """
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
        if not self.stmt_mapping:
            return ""
        rpad = max(len(line) for line in self.stmt_mapping)
        lines = []
        for keyword, sql_str in self.stmt_mapping.items():
            if opts.mode == PrintMode.PRETTY:
                # If we don't care about the river, then don't pad anything.
                if self.river:
                    keyword = keyword.rjust(rpad)
                    # If sql_str is empty, then no need to pad.
                    if sql_str:
                        sql_str = pad(sql_str, rpad + 1)
            elif opts.mode != PrintMode.DEFAULT:
                raise NotImplementedError(
                    f"to_string not implemented for mode {opts.mode}"
                )

            if sql_str:
                lines.append(keyword + " " + sql_str)
            else:
                lines.append(keyword)

        join_char = "\n" if opts.mode == PrintMode.PRETTY else " "
        return join_char.join(lines)
