"""Runtime for dependency-isolated generated package barrels.

The barrel generator owns the target table.  This module only implements the
runtime lookup and, crucially, keeps a same-named child module from shadowing
the public class/function exported under that name.
"""

from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType


class _LazyExportsModule(ModuleType):
    def __getattribute__(self, name: str):
        namespace = ModuleType.__getattribute__(self, "__dict__")
        targets = namespace.get("_LAZY_EXPORTS", {})
        target = targets.get(name)
        if target is not None:
            # An explicit non-module assignment is an override seam (runtime
            # injection, test fake, feature replacement) and must win. Only a
            # same-named child module is Python's implicit shadow binding; that
            # is the value the canonical public export must replace.
            if name in namespace and not isinstance(namespace[name], ModuleType):
                return namespace[name]
            module_name, attribute = target
            package_name = ModuleType.__getattribute__(self, "__name__")
            value = getattr(import_module(module_name, package_name), attribute)
            ModuleType.__setattr__(self, name, value)
            return value
        return ModuleType.__getattribute__(self, name)

    def __dir__(self) -> list[str]:
        namespace = ModuleType.__getattribute__(self, "__dict__")
        return sorted(set(namespace) | set(namespace.get("_LAZY_EXPORTS", {})))


def _install_lazy_exports(module_name: str, targets: dict[str, tuple[str, str]]) -> None:
    module = sys.modules[module_name]
    module.__dict__["_LAZY_EXPORTS"] = targets
    if module.__class__ is not _LazyExportsModule:
        module.__class__ = _LazyExportsModule
