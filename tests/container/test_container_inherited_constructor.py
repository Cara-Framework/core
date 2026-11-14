"""A class that inherits its constructor is built like the base.

Under ``from __future__ import annotations`` a constructor's parameter types
are strings, read from a module's names. The container read them from the
module of the class being BUILT, so a subclass defined in a module that never
imports its base constructor's parameter types could not be built ("Not
enough dependencies passed"), while the base resolved fine. The strings are
the defining function's, so they are read where that function is defined.
"""

from __future__ import annotations

import sys
import types

from cara.container import Container


class _Dependency:
    pass


class _Base:
    def __init__(self, dependency: _Dependency) -> None:
        self.dependency = dependency


def test_a_subclass_in_another_module_resolves_the_inherited_constructor():
    elsewhere = types.ModuleType("_cara_test_elsewhere")
    sys.modules[elsewhere.__name__] = elsewhere
    try:
        subclass = type("_Sub", (_Base,), {"__module__": elsewhere.__name__})
        elsewhere._Sub = subclass
        container = Container()
        container.bind(_Dependency, _Dependency)

        built = container.make(subclass)

        assert isinstance(built, subclass)
        assert isinstance(built.dependency, _Dependency)
    finally:
        sys.modules.pop(elsewhere.__name__, None)
