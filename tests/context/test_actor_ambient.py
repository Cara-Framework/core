"""The acting actor rides the queue hop like the tenant does."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from cara.context import Actor, Ambient, acting_as, current_actor, set_actor


def test_actor_is_captured_and_restored_across_the_hop() -> None:
    with acting_as("admin", 42):
        captured = Ambient.capture()
    assert captured["actor"] == {"kind": "admin", "id": 42}
    assert current_actor() is None
    with Ambient.restored(captured):
        assert current_actor() == Actor("admin", 42)
    assert current_actor() is None


def test_set_actor_names_the_rest_of_the_context() -> None:
    import contextvars

    def _run() -> Actor | None:
        set_actor(Actor("system"))
        return current_actor()

    assert contextvars.copy_context().run(_run) == Actor("system")
    assert current_actor() is None


@pytest.mark.parametrize(
    ("kind", "id_"),
    [("Admin", 1), ("", None), ("a b", None), ("admin", 0), ("admin", True)],
)
def test_malformed_actor_is_refused(kind, id_) -> None:
    with pytest.raises(ValueError):
        Actor(kind, id_)


def test_garbled_envelope_value_is_skipped_not_fatal() -> None:
    with Ambient.restored({"actor": {"kind": "DROP TABLE", "id": -1}}):
        assert current_actor() is None


def test_a_fresh_worker_restores_the_actor_before_importing_its_module() -> None:
    """The worker restores ambient context before the job body imports
    whatever reads the actor; the framework carrier must already be known."""
    root = Path(__file__).resolve().parents[2]
    code = (
        "from cara.context.Ambient import Ambient\n"
        "import sys\n"
        "assert 'cara.context.Actor' not in sys.modules\n"
        "with Ambient.restored({'actor': {'kind': 'admin', 'id': 7}}):\n"
        "    from cara.context import current_actor\n"
        "    print(current_actor())\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=root, capture_output=True, text=True, check=True
    )
    assert "Actor(kind='admin', id=7)" in out.stdout
