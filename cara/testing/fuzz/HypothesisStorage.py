"""Keep Hypothesis's example database out of the source tree."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


class HypothesisStorage:
    """Point ``HYPOTHESIS_STORAGE_DIRECTORY`` at the temp dir.

    Hypothesis writes constants and failing-example patches under
    ``./.hypothesis`` unless told otherwise, which leaves scratch state in the
    repository the way a pytest cache would. Call :meth:`redirect` at the very
    top of a ``conftest.py`` (Hypothesis reads the variable lazily, on its first
    write). ``setdefault`` keeps a CI-provided location.
    """

    ENV = "HYPOTHESIS_STORAGE_DIRECTORY"

    @classmethod
    def redirect(cls, name: str) -> Path:
        """Set the storage directory to ``<tmp>/<name>-hypothesis``; return it."""
        if not name or os.sep in name:
            raise ValueError(f"storage name must be a plain token, got {name!r}")
        default = Path(tempfile.gettempdir()) / f"{name}-hypothesis"
        return Path(os.environ.setdefault(cls.ENV, str(default)))
