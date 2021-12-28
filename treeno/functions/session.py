from typing import ClassVar

from treeno.base import PrintOptions
from treeno.datatypes.builder import varchar
from treeno.expression import value_attr
from treeno.functions.base import Function


@value_attr
class CurrentUser(Function):
    FN_NAME: ClassVar[str] = "CURRENT_USER"

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class CurrentCatalog(Function):
    FN_NAME: ClassVar[str] = "CURRENT_CATALOG"

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class CurrentSchema(Function):
    FN_NAME: ClassVar[str] = "CURRENT_SCHEMA"

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class CurrentPath(Function):
    FN_NAME: ClassVar[str] = "CURRENT_PATH"

    def __attrs_post_init__(self) -> None:
        self.data_type = varchar()

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME
