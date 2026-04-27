"""Direct-file module loader for tests.

The services package's ``__init__.py`` chains imports through
``packages.amazon.*`` which transitively triggers ``bootstrap.py``,
which boots the entire framework — too heavy and brittle for unit
tests of a single service.

:func:`load_service` and :func:`load_contract` use
``importlib.util.spec_from_file_location`` to load a *single* module
file without running the parent package's ``__init__.py``. The loaded
module is registered in ``sys.modules`` under its dotted path so
intra-module references (``isinstance`` checks against the contract
class loaded the same way) keep working.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Any, Type

def _find_services_root() -> Path:
    """Locate the services project root.

    ``cara`` is a symlink (``services/cara -> ../commons/cara/cara``)
    so ``Path(__file__).resolve()`` lands inside ``commons/`` and we'd
    look there instead. Walk up from the unresolved path until we hit
    a directory containing both ``pytest.ini`` and ``app``.
    """
    here = Path(__file__).parent
    for candidate in [here, *here.parents]:
        if (candidate / "pytest.ini").exists() and (candidate / "app").is_dir():
            return candidate
    raise RuntimeError(
        "Could not locate services root from "
        f"{Path(__file__)!s}; expected pytest.ini + app/ on an ancestor."
    )


_SERVICES_ROOT = _find_services_root()


def _load_file(dotted: str, file_path: Path) -> Any:
    """Load ``file_path`` and register it as ``dotted`` in sys.modules."""
    if dotted in sys.modules:
        return sys.modules[dotted]
    spec = importlib.util.spec_from_file_location(dotted, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not build spec for {dotted} at {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[dotted] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        # Don't leave a half-initialised module behind.
        sys.modules.pop(dotted, None)
        raise
    return module


def load_service(name: str) -> Type[Any]:
    """Load ``app.services.<name>`` and return the class of the same name.

    Example::

        PriceValidationService = load_service("PriceValidationService")
    """
    dotted = f"app.services.{name}"
    path = _SERVICES_ROOT / "app" / "services" / f"{name}.py"
    if not path.exists():
        raise FileNotFoundError(f"No service file at {path}")
    module = _load_file(dotted, path)
    if not hasattr(module, name):
        raise AttributeError(f"Module {dotted} has no class named {name!r}")
    return getattr(module, name)


def load_contract(name: str) -> Type[Any]:
    """Load ``app.contracts.<name>`` and return the class of the same name."""
    dotted = f"app.contracts.{name}"
    path = _SERVICES_ROOT / "app" / "contracts" / f"{name}.py"
    if not path.exists():
        raise FileNotFoundError(f"No contract file at {path}")
    module = _load_file(dotted, path)
    if not hasattr(module, name):
        raise AttributeError(f"Module {dotted} has no class named {name!r}")
    return getattr(module, name)


def stub_modules(*dotted_names: str) -> None:
    """Pre-register empty stub modules in ``sys.modules`` (no auto-cleanup).

    Useful when a service does ``from heavy.package import X`` and the
    package's ``__init__.py`` chains imports we don't want to execute
    (e.g. ``app.support`` triggers config/Currency bootstrap).

    Prefer :func:`stub_modules_scoped` in tests — that one cleans up
    after itself so test order doesn't matter.
    """
    import types

    for name in dotted_names:
        if name in sys.modules:
            continue
        sys.modules[name] = types.ModuleType(name)


from contextlib import contextmanager  # noqa: E402
from typing import Iterator  # noqa: E402


@contextmanager
def stub_modules_scoped(**stubs: Any) -> Iterator[dict]:
    """Install module stubs for the duration of the ``with`` block.

    Each kwarg is ``dotted_name=fake_module``. On exit, the previous
    ``sys.modules`` entry (or absence of one) is restored — so tests
    that stub the same module independently don't pollute each other.

    Example::

        my_support = types.ModuleType("app.support")
        my_support.SeasonalCalendar = FakeCalendar
        with stub_modules_scoped(**{"app.support": my_support}):
            mod = load_module("app.services.MyService")
            ...
    """
    previous: dict = {}
    for name, fake in stubs.items():
        previous[name] = sys.modules.get(name, _MISSING)
        sys.modules[name] = fake
    try:
        yield stubs
    finally:
        for name, prev in previous.items():
            if prev is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prev


# Sentinel for "no previous module" used by the scoped stub.
_MISSING: Any = object()


def load_module(dotted: str) -> Any:
    """Load an arbitrary project module by dotted path, without
    triggering parent-package ``__init__.py`` chains.

    Pass ``"app.support.SeasonalCalendar"`` to load that exact file.
    """
    rel_path = dotted.replace(".", "/") + ".py"
    path = _SERVICES_ROOT / rel_path
    if not path.exists():
        raise FileNotFoundError(f"No module file at {path}")
    return _load_file(dotted, path)
