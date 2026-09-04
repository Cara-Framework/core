"""Shared tmp-tree builder for the model-first migration tests.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).

``ModelDiscoverer`` reads model SOURCE, so every discovery test writes a
throwaway model file first. Three copies of the writer meant three places
for the dedent/encoding behaviour to drift; this is the one owner.
"""

from __future__ import annotations

import textwrap
from pathlib import Path


def write_model(tmp_path: Path, filename: str, source: str) -> Path:
    path = tmp_path / filename
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path
