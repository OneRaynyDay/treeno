#!/usr/bin/env python
import difflib
import os
import re
import sys

import click
import requests
import typer

# Refer to https://github.com/antlr/antlr4/blob/master/tool/src/org/antlr/v4/codegen/target/Python3Target.java
KEYWORDS_TO_REPLACE = [
    "abs",
    "all",
    "and",
    "any",
    "apply",
    "as",
    "assert",
    "bin",
    "bool",
    "break",
    "buffer",
    "bytearray",
    "callable",
    "chr",
    "class",
    "classmethod",
    "coerce",
    "compile",
    "complex",
    "continue",
    "def",
    "del",
    "delattr",
    "dict",
    "dir",
    "divmod",
    "elif",
    "else",
    "enumerate",
    "eval",
    "execfile",
    "except",
    "file",
    "filter",
    "finally",
    "float",
    "for",
    "format",
    "from",
    "frozenset",
    "getattr",
    "global",
    "globals",
    "hasattr",
    "hash",
    "help",
    "hex",
    "id",
    "if",
    "import",
    "in",
    "input",
    "int",
    "intern",
    "is",
    "isinstance",
    "issubclass",
    "iter",
    "lambda",
    "len",
    "list",
    "locals",
    "map",
    "max",
    "min",
    "memoryview",
    "next",
    "nonlocal",
    "not",
    "object",
    "oct",
    "open",
    "or",
    "ord",
    "pass",
    "pow",
    "print",
    "property",
    "raise",
    "range",
    "raw_input",
    "reduce",
    "reload",
    "repr",
    "return",
    "reversed",
    "round",
    "set",
    "setattr",
    "slice",
    "sorted",
    "staticmethod",
    "str",
    "sum",
    "super",
    "try",
    "tuple",
    "type",
    "unichr",
    "unicode",
    "vars",
    "with",
    "while",
    "yield",
    "zip",
    "__import__",
    "True",
    "False",
    "None",
]


# This is the raw file that gets updated in Trino's github trunk.
def get_grammar_url(commit_id):
    return f"https://raw.githubusercontent.com/trinodb/trino/{commit_id}/core/trino-parser/src/main/antlr4/io/trino/sql/parser/SqlBase.g4"


def escape_keywords(content_lines, fromfile, tofile):
    union_string = "|".join(KEYWORDS_TO_REPLACE)
    escaped_content_lines = []
    for line in content_lines:
        if re.match(r"\s*([\/\/]|\*)", line):
            escaped_content_lines.append(line)
        else:
            # We accept any group non-alphanumeric group behind and after the current string optionally because
            # there doesn't need to be any characters at the beginning/end of the line for a rule definition
            escaped_content_lines.append(
                re.sub(
                    rf"(^|[^a-zA-Z])({union_string})(?=$|[^a-zA-Z])",
                    r"\1\2_",
                    line,
                )
            )

    diffs = difflib.context_diff(
        content_lines, escaped_content_lines, fromfile=fromfile, tofile=tofile
    )
    diff_output = "".join(diffs)
    if not click.confirm(
        f"After fetching SqlBase.g4 from Trino, applied the following changes to escape keywords in grammar:\n{diff_output}\nContinue?"
    ):
        sys.exit(1)
    return escaped_content_lines


def monkeypatch_letter(content_lines):
    # Hacky way to allow us to recognize lowercased letters correctly.
    for idx, line in enumerate(content_lines):
        if "fragment LETTER" in line:
            difflib.context_diff
            next_line = content_lines[idx + 1]
            patched_line = re.sub(r"A-Z", r"a-zA-Z", next_line)
            before_line = "\n".join([line, next_line])
            after_line = "\n".join([line, patched_line])
            if not click.confirm(
                f"Applied the following changes to allow lower case letters in grammar:\nBefore: {before_line}\nAfter: {after_line}\nContinue?"
            ):
                sys.exit(1)
            content_lines[idx + 1] = patched_line
            return content_lines
    raise ValueError("Expected grammar rule LETTER. Not found.")


def allow_lowercase_keywords(content_lines, fromfile, tofile):
    lines = []
    for line in content_lines:
        match = re.match(r"^([A-Z]*): '\1';$", line)
        if not match:
            lines.append(line)
        else:
            rule_name = match.group(1)
            lines.append(
                f"{rule_name}: '{rule_name}' | '{rule_name.lower()}';\n"
            )

    diffs = difflib.context_diff(
        content_lines, lines, fromfile=fromfile, tofile=tofile
    )
    diff_output = "".join(diffs)
    if not click.confirm(
        f"After fetching SqlBase.g4 from Trino, applied the following changes to allow lowercase keywords in grammar:\n{diff_output}\nContinue?"
    ):
        sys.exit(1)
    return lines


def download_file(
    file_name: str, commit_id: str = "master", replace: bool = False
):
    file_exists = os.path.exists(file_name)
    if not replace:
        assert (
            not file_exists
        ), f"File already exists at {file_name}. Move/delete it before generating a new grammar file."
    elif file_exists:
        print(f"File {file_name} found with replace=True. Deleting the file.")
        os.remove(file_name)
    grammar_url = get_grammar_url(commit_id)
    r = requests.get(grammar_url)
    # Before we write it to the file, we have to replace python keywords with keyword + _
    # TODO: Ideally we would do this substitution by parsing the ANTLR grammar file with an ANTLR parser instead of
    # using regex.
    content = r.content.decode()
    content_lines = content.splitlines(keepends=True)
    content_lines = escape_keywords(
        content_lines, fromfile=grammar_url, tofile=file_name
    )
    content_lines = monkeypatch_letter(content_lines)
    content_lines = allow_lowercase_keywords(
        content_lines, fromfile=grammar_url, tofile=file_name
    )

    with open(file_name, "w") as f:
        f.writelines(content_lines)


if __name__ == "__main__":
    typer.run(download_file)
