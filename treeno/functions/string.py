from enum import Enum
from typing import ClassVar, Optional

import attr

from treeno.base import PrintOptions
from treeno.datatypes.builder import (
    bigint,
    boolean,
    integer,
    varbinary,
    varchar,
)
from treeno.expression import Value, value_attr, wrap_literal
from treeno.functions.base import Function
from treeno.printer import join_stmts


class NormalForm(Enum):
    NFD = "NFD"
    NFC = "NFC"
    NFKD = "NFKD"
    NFKC = "NFKC"


@value_attr
class Chr(Function):
    FN_NAME: ClassVar[str] = "CHR"
    number: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.number], opts)


@value_attr
class CodePoint(Function):
    FN_NAME: ClassVar[str] = "CODEPOINT"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = integer()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class HammingDistance(Function):
    FN_NAME: ClassVar[str] = "HAMMING_DISTANCE"
    string1: Value = attr.ib(converter=wrap_literal)
    string2: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string1, self.string2], opts)


@value_attr
class Length(Function):
    FN_NAME: ClassVar[str] = "LENGTH"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class LevenshteinDistance(Function):
    FN_NAME: ClassVar[str] = "LEVENSHTEIN_DISTANCE"
    string1: Value = attr.ib(converter=wrap_literal)
    string2: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = bigint()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string1, self.string2], opts)


@value_attr
class Lower(Function):
    FN_NAME: ClassVar[str] = "LOWER"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class LPad(Function):
    FN_NAME: ClassVar[str] = "LPAD"
    string: Value = attr.ib(converter=wrap_literal)
    size: Value = attr.ib(converter=wrap_literal)
    pad_string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string, self.size, self.pad_string], opts)


@value_attr
class LTrim(Function):
    FN_NAME: ClassVar[str] = "LTRIM"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class LuhnCheck(Function):
    FN_NAME: ClassVar[str] = "LUHN_CHECK"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = boolean()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class Replace(Function):
    FN_NAME: ClassVar[str] = "REPLACE"
    string: Value = attr.ib(converter=wrap_literal)
    search: Value = attr.ib(converter=wrap_literal)
    replace: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        components = [self.string, self.search]
        if self.replace:
            components.append(self.replace)
        return self.to_string(components, opts)


@value_attr
class Reverse(Function):
    FN_NAME: ClassVar[str] = "REVERSE"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class RPad(Function):
    FN_NAME: ClassVar[str] = "RPAD"
    string: Value = attr.ib(converter=wrap_literal)
    size: Value = attr.ib(converter=wrap_literal)
    pad_string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string, self.size, self.pad_string], opts)


@value_attr
class RTrim(Function):
    FN_NAME: ClassVar[str] = "RTRIM"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class Soundex(Function):
    FN_NAME: ClassVar[str] = "SOUNDEX"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class Normalize(Function):
    FN_NAME: ClassVar[str] = "NORMALIZE"
    string: Value = attr.ib(converter=wrap_literal)
    normal_form: Optional[NormalForm] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        # Note: The reason we don't use self.to_string here is because
        # normal_form is not an arbitrary Sql construct here but rather
        # an enum.
        components = [self.string.to_sql(opts)]
        if self.normal_form:
            components.append(self.normal_form.value)
        arg_string = join_stmts(components, opts)
        return f"{self.FN_NAME}({arg_string})"


@value_attr
class ToUTF8(Function):
    FN_NAME: ClassVar[str] = "TO_UTF8"
    string: Value = attr.ib(converter=wrap_literal)

    def __attrs_post_init__(self) -> None:
        self.data_type = varbinary()

    def sql(self, opts: PrintOptions) -> str:
        return self.to_string([self.string], opts)


@value_attr
class FromUTF8(Function):
    FN_NAME: ClassVar[str] = "FROM_UTF8"
    binary: Value = attr.ib(converter=wrap_literal)
    replace: Optional[Value] = attr.ib(
        default=None, converter=attr.converters.optional(wrap_literal)
    )

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        components = [self.binary]
        if self.replace:
            components.append(self.replace)
        return self.to_string(components, opts)
