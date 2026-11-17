"""Re-import a module under a patched environment WITHOUT leaking it.

``sys.modules.pop(name)`` + ``importlib.import_module(name)`` is how a test
re-executes a ``config/*.py`` module under a different ``APP_ENV``. Done bare,
the fresh module leaks in two places a later test cannot see:

* ``sys.modules[name]`` now holds the module executed under the TEST's
  environment, so a later ``from config.app import IS_PRODUCTION`` (or an
  ``importlib.reload`` of a consumer) reads a verdict the process environment
  no longer carries. Popping it again after the test does not help: the next
  importer re-executes it, and every module that bound the ORIGINAL object
  keeps pointing at a module no longer registered;
* the parent package's attribute (``config.app``) points at that fresh object
  too — ``import config.app as cfg`` binds THROUGH the attribute, while
  ``from config.app import KEY`` resolves through ``sys.modules``. Restoring
  only one of the two leaves the pair on DIFFERENT module objects, so a test
  that patches ``cfg.KEY`` and reloads a consumer patches an object the
  consumer never reads.

``drop_modules`` records both through ``monkeypatch`` so teardown puts the
ORIGINAL objects back in both places. It surfaced as an order-dependent red
under pytest-randomly: an encryption config reloaded after a production CORS
case read ``ENV="production"`` and refused the empty keyring the test expected.
"""

from __future__ import annotations

import sys

import pytest


def drop_modules(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    """Drop ``names`` from ``sys.modules`` and from their parent packages so
    the next import re-executes them; ``monkeypatch`` teardown restores the
    original module objects in both places."""
    for name in names:
        parent_name, _, child = name.rpartition(".")
        parent = sys.modules.get(parent_name) if parent_name else None
        if parent is not None and hasattr(parent, child):
            monkeypatch.setattr(parent, child, getattr(parent, child))
        monkeypatch.delitem(sys.modules, name, raising=False)
