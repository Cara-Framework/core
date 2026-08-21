"""Gate: the queue reference may never ship a prose apology instead of content.

The page this covers spent a month, in BOTH products, with a body consisting
entirely of "_Could not parse QueueTopology.py — check the file by hand._". The
file parsed perfectly. The extractor was an ``ast.literal_eval`` over the
elements of one hard-coded NAME, so a topology whose table is COMPUTED
(``TABLE = _lane_bindings()``) has no elements to walk, and a product that
names its table something else was never looked at at all.

Two failures compounded: the reader was too narrow, and its fallback was PROSE.
A page that apologises is indistinguishable from a page that works — writes are
idempotent, so "(unchanged)" is reported forever. So the reader now loads the
contract module (it is pure vocabulary; DOCTRINE §5 keeps ``env()`` out of it)
and finds the table by SHAPE, and an unrenderable topology RAISES.
"""

from __future__ import annotations

import time

import pytest

from cara.docs.Queues import (
    binding_tables,
    gen_queues,
    load_contract,
    worker_pool_rows,
)

from ._fixtures import make_checkout, manifest_for, write

# A table built by a helper call — the shape that rendered as nothing before.
COMPUTED_TOPOLOGY = '''
"""Fixture topology whose table is computed, not spelled out."""

from __future__ import annotations

_LANES: tuple[str, ...] = ("control", "bulk")

# Private, and shaped exactly like the live table: it must NOT be picked up.
_RETIRED_BINDINGS: list[tuple[str, str]] = [("old", "old.*")]

# A public string list that is not a binding table: it must NOT be picked up.
JOB_ALLOWED_PREFIXES: list[str] = ["app.jobs"]


def _bindings() -> list[tuple[str, str]]:
    return [(lane, f"{lane}.*.*") for lane in _LANES]


LANE_BINDINGS: list[tuple[str, str]] = _bindings()
'''

NO_TABLE_TOPOLOGY = '''
"""Fixture topology that declares no queue/pattern pairs at all."""

JOB_ALLOWED_PREFIXES: list[str] = ["app.jobs"]
'''

# Pools built by a helper, plus one appended after the literal — the append is
# what the old ``\\n}``-terminated regex could never see.
POOL_CONFIG = """
from cara.environment import env


def _pool(lane: str, *, concurrency: int) -> dict:
    return {"queues": [lane], "concurrency": int(env("C", concurrency)), "timeout": 5}


WORKER_POOLS: dict[str, dict] = {
    "control": _pool("control", concurrency=2),
    "bulk": {"queues": ["bulk"], "concurrency": 1, "timeout": 7},
}

WORKER_POOLS["all"] = {"queues": ["control", "bulk"], "timeout": 5}
"""


def _checkout(tmp_path, topology: str = COMPUTED_TOPOLOGY):
    root = make_checkout(tmp_path, "alpha")
    write(root / "commons" / "contracts" / "QueueTopology.py", topology)
    write(root / "services" / "config" / "queue.py", POOL_CONFIG)
    return root


def test_computed_binding_table_is_read(tmp_path):
    """A table produced by a helper call still reaches the page."""
    root = _checkout(tmp_path)
    module = load_contract(root / "commons" / "contracts" / "QueueTopology.py")

    assert binding_tables(module) == [
        ("LANE_BINDINGS", [("control", "control.*.*"), ("bulk", "bulk.*.*")])
    ]


def test_generated_page_carries_the_topology(tmp_path):
    """The rendered page states the bindings and every declared pool."""
    root = _checkout(tmp_path)
    manifest = manifest_for(root, "alpha")

    gen_queues(manifest, time.strftime("%Y-%m-%d %H:%M"), lambda _line: None)

    page = (manifest.reference / "queues.md").read_text(encoding="utf-8")
    assert "Could not parse" not in page
    assert "| `control` | `control.*.*` |" in page
    assert "_Total: 2 bindings._" in page
    # Every pool is named even though concurrency is env-derived, and the pool
    # appended after the literal is present.
    for pool in ("control", "bulk", "all"):
        assert f"| {pool} |" in page
    # A literal field is reported as the literal, not as boot-resolved.
    assert "| bulk | `bulk` | 7s |" in page


def test_unrenderable_topology_raises_instead_of_apologising(tmp_path):
    """No table means a bug in the contract or in this reader — never a page."""
    root = _checkout(tmp_path, topology=NO_TABLE_TOPOLOGY)
    manifest = manifest_for(root, "alpha")

    with pytest.raises(RuntimeError, match="no queue"):
        gen_queues(manifest, time.strftime("%Y-%m-%d %H:%M"), lambda _line: None)

    assert not (manifest.reference / "queues.md").exists()


def test_product_without_a_queue_contract_is_a_no_op(tmp_path):
    """Probing for an absent subject is not the same as failing to read one."""
    root = make_checkout(tmp_path, "beta")
    manifest = manifest_for(root, "beta")

    gen_queues(manifest, time.strftime("%Y-%m-%d %H:%M"), lambda _line: None)

    assert not (manifest.reference / "queues.md").exists()


def test_appended_pool_entries_are_not_lost(tmp_path):
    """``WORKER_POOLS[x] = {...}`` after the literal is a pool like any other."""
    rows = worker_pool_rows(POOL_CONFIG)

    assert [name for name, _queues, _backoff in rows] == ["control", "bulk", "all"]
