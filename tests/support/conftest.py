"""Make the ``commons`` namespace package importable from the framework suite.

The Cara framework test suite (``commons/cara/tests``) normally only imports
``cara.*`` — the framework is self-contained. ``test_feature_gate.py`` is the
exception: ``FeatureGate`` lives in ``commons/support`` and reads the
``commons.models.core.FeatureFlag`` model, so the test needs the ``commons``
namespace package on ``sys.path``.

``commons`` is a namespace package whose parent is the repo root
(``.../code``). Running pytest from ``commons/cara`` puts ``cara`` on the path
but not ``commons``; this conftest prepends the repo root so both resolve.
Scoped to ``tests/support/`` so it only affects the one test that needs it.
"""

from __future__ import annotations

import sys
from pathlib import Path

# this file: commons/cara/tests/support/conftest.py → repo root is 4 up.
_REPO_ROOT = Path(__file__).resolve().parents[4]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
