"""Generated lazy barrels preserve both public identity and injection seams."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

from cara._LazyExports import _install_lazy_exports


def test_explicit_non_module_override_wins_over_lazy_target() -> None:
    name = "_cara_test_lazy_override"
    package = ModuleType(name)
    package.__path__ = []
    sys.modules[name] = package
    try:
        _install_lazy_exports(name, {"service": ("builtins", "str")})
        fake = SimpleNamespace(call=lambda: "fake")
        package.service = fake

        assert package.service is fake
    finally:
        sys.modules.pop(name, None)


def test_same_named_module_shadow_is_replaced_by_public_target() -> None:
    name = "_cara_test_lazy_shadow"
    package = ModuleType(name)
    package.__path__ = []
    sys.modules[name] = package
    try:
        _install_lazy_exports(name, {"service": ("builtins", "str")})
        package.service = ModuleType(f"{name}.service")

        assert package.service is str
    finally:
        sys.modules.pop(name, None)
