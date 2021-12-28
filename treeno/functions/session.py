from typing import ClassVar

from treeno.base import PrintOptions
from treeno.expression import value_attr
from treeno.functions.base import Function


@value_attr
class CurrentUser(Function):
    FN_NAME: ClassVar[str] = "CURRENT_USER"

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class CurrentCatalog(Function):
    FN_NAME: ClassVar[str] = "CURRENT_CATALOG"

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class CurrentSchema(Function):
    FN_NAME: ClassVar[str] = "CURRENT_SCHEMA"

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME


@value_attr
class CurrentPath(Function):
    FN_NAME: ClassVar[str] = "CURRENT_PATH"

    def sql(self, opts: PrintOptions) -> str:
        return self.FN_NAME
