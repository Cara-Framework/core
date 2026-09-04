"""Dev-only kernel-barrel plumbing: one ``sys.modules`` mirroring helper.

Each deployable's thin kernel barrels (``app/models``, ``app/contracts``,
``app/gates``, ``app/shared`` — DOCTRINE §2/§3) mirror the kernel package's
submodules under their own ``app.*`` name so a deep import resolves to the
SAME module object. That mirroring loop was copy-pasted into every barrel
that needed it — three files per deployable, six across the two that share
this framework — so a fix to the single-identity rule had to be made six
times or not at all. It lives here once.

Production never runs this: ``build:vendor-commons`` replaces each dev
barrel with the kernel package's own ``__init__.py``, so the import below
disappears from the image along with the block that calls it.
"""

from __future__ import annotations

import importlib
import pkgutil
import sys
from typing import Any

__all__ = ["alias_kernel_submodules"]


def alias_kernel_submodules(
    package: Any, alias_name: str, *, skip: tuple[str, ...] = ()
) -> None:
    """Mirror *package*'s submodules into ``sys.modules`` under *alias_name*.

    Registers the SAME module objects under both the kernel name and the
    ``app.*`` name, so deep imports (``from app.gates.ProductPredicates
    import ProductPredicates``) resolve without creating a second module
    instance — exception classes, registries and module state stay shared.
    Recurses into subpackages; names in *skip* (and their subtrees) are left
    unaliased, which is how kernel-internal packages such as
    ``commons.gates.persistence`` stay unreachable from an app tree.
    """
    for info in pkgutil.iter_modules(package.__path__):
        if info.name in skip:
            continue
        module = importlib.import_module(f"{package.__name__}.{info.name}")
        child = f"{alias_name}.{info.name}"
        sys.modules.setdefault(child, module)
        if info.ispkg:
            alias_kernel_submodules(module, child)
