"""Model discovery must stay inside the project it is asked about.

Found 2026-11-14 (overnight schema-deploy c1) in the production API image:
``ModelDiscoverer.discover_models`` scans ``Path(paths("")).parent`` five
levels deep, and the image's base path is ``/app`` — so every ``schema:check``
/ ``schema:plan`` / ``schema:apply`` in production walks ``/`` (``/proc``,
``/sys``, ``/usr/local/lib/python3.14``...). It works by luck in the parent
process; ``schema:verify`` re-runs ``schema:check`` as a child, the walk hits
another process's ``/proc/<pid>/net`` and dies with ``OSError: [Errno 22]
Invalid argument``, which verify then reports as "The migrated-from-zero
schema differs from the models" — a crash dressed up as drift. Any ``.py``
anywhere on the host filesystem that looks like a model would also join
the plan.
"""

from __future__ import annotations

import importlib
import textwrap

_DISCOVERER = importlib.import_module("cara.eloquent.migrations.ModelDiscoverer")

_MODEL = textwrap.dedent(
    """
    from cara.eloquent.models import Model
    from cara.eloquent.schema import Schema


    class {name}(Model):
        __table__ = "{table}"

        @property
        def fields(self):
            return Schema.build(lambda field: (field.big_increments("id"),))
    """
)


def test_discovery_does_not_read_models_outside_the_project(monkeypatch, tmp_path):
    project = tmp_path / "app"
    (project / "models").mkdir(parents=True)
    (project / "models" / "Owned.py").write_text(
        _MODEL.format(name="Owned", table="owned")
    )
    stranger = tmp_path / "somebody_else"
    stranger.mkdir()
    (stranger / "Foreign.py").write_text(_MODEL.format(name="Foreign", table="foreign"))
    monkeypatch.setattr(_DISCOVERER, "paths", lambda *_: str(project))

    tables = {model["table"] for model in _DISCOVERER.ModelDiscoverer().discover_models()}

    assert "owned" in tables
    assert "foreign" not in tables, (
        "discovery scanned the base path's PARENT: in the image that is '/', so "
        "schema:verify dies in /proc and any model-shaped file on the host joins the plan"
    )
