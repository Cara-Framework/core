"""Shared source walk for the AST audits in this package.

The three audits (cache keys, discarded dispatch coroutines, numeric
truthiness) each walked the tree with the same eight lines. This is that
walk, once. ``__init__.py`` files are scanned deliberately — a barrel can
carry the same defects as any other module, which is why this does not
reuse ``cara.architecture._ast_utils.iter_modules`` (that one skips
barrels, and reusing it would also point ``cara.testing`` at
``cara.architecture``).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path


def _iter_sources(
    root: Path, directories: Sequence[str] | None = None
) -> Iterator[tuple[str, str]]:
    """Yield ``(source, relative_path)`` for every module under ``root``.

    ``directories=None`` means the whole tree; a named directory that does
    not exist is skipped, so one declaration can serve deployables that do
    not all carry the same layers.

    ``root`` itself must exist. A root that is not a directory (a renamed
    ``app/``, a ``parents[n]`` off by one) used to yield nothing, so every
    audit over it returned no findings and its guard test stayed green over
    a tree it never read.
    """
    if not root.is_dir():
        raise NotADirectoryError(
            f"audit root {root} is not a directory — nothing would be scanned "
            f"and the audit would pass without reading a single file"
        )
    targets = [root] if directories is None else [root / name for name in directories]
    for target in targets:
        if not target.is_dir():
            continue
        for path in sorted(target.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            yield path.read_text(encoding="utf-8"), path.relative_to(root).as_posix()
