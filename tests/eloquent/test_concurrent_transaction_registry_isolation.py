"""Pins the keystone concurrency invariant of the transaction layer:
per-context / per-thread isolation of the active-connection registry.

The production queue worker runs many jobs as concurrent asyncio tasks AND real
OS threads. The transaction registry (``_ACTIVE_CONNECTIONS``) is a ContextVar
holding a MUTABLE dict — and a task/thread spawned via ``copy_context()``
inherits the parent's dict BY REFERENCE. If two concurrent units then shared one
registry, one's ``commit`` would pop the connection the other is mid-transaction
on ("No active transaction found for connection: app" → dead-letter). That was
the dominant queue error before ``reset_registry()`` was added at every spawn
boundary (``BaseJob.handle``, ``ExecutionContext.run_in_thread``).

``ConnectionResolver.py`` references a ``test_concurrent_transactions.py`` that
never existed, so this single most-important property had no CI guard. These
tests are hermetic (pure ContextVar logic — no DB) and fail if the isolation
regresses.
"""

from __future__ import annotations

import threading
from contextvars import copy_context

from cara.eloquent.connections.ConnectionResolver import (
    _ACTIVE_CONNECTIONS,
    _get_registry,
    reset_registry,
)


def _clear() -> None:
    _ACTIVE_CONNECTIONS.set(None)


def test_get_registry_is_lazy_and_stable_within_a_context() -> None:
    _clear()
    r1 = _get_registry()
    r2 = _get_registry()
    assert r1 is r2, "repeated calls in one context must return the SAME dict"
    assert r1 == {}


def test_reset_registry_binds_a_fresh_dict() -> None:
    _clear()
    r1 = _get_registry()
    r1["app"] = object()
    reset_registry()
    r2 = _get_registry()
    assert r2 is not r1, "reset_registry must rebind a NEW dict"
    assert r2 == {}, "the fresh registry must be empty"


def test_threads_do_not_share_a_registry() -> None:
    """Each OS thread that touches the registry must get its own — a peer
    thread's transaction bookkeeping must be invisible."""
    _clear()
    seen: dict[str, object] = {}
    barrier = threading.Barrier(4)

    def worker(name: str) -> None:
        # Mirror BaseJob.handle: isolate this unit's registry up front.
        reset_registry()
        reg = _get_registry()
        reg[name] = f"conn-{name}"
        barrier.wait(timeout=10)  # ensure all mutate before any reads
        # This thread must see ONLY its own entry — never a peer's.
        seen[name] = dict(reg)

    threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    for name, reg in seen.items():
        assert reg == {name: f"conn-{name}"}, (
            f"thread {name} saw a shared/leaked registry {reg} — per-thread "
            f"isolation is broken"
        )


def test_copy_context_without_reset_shares_the_dict_but_reset_isolates() -> None:
    """Documents the exact hazard reset_registry defends against: a
    ``copy_context()`` child shares the parent's dict BY REFERENCE, so a plain
    child mutation leaks back to the parent — UNLESS the child resets first."""
    _clear()
    parent = _get_registry()
    parent["app"] = "parent-conn"

    # Child WITHOUT reset: shares the same dict object → leak.
    def _child_no_reset() -> None:
        _get_registry()["leaked"] = "child-conn"

    copy_context().run(_child_no_reset)
    assert "leaked" in parent, (
        "sanity: a copy_context child that does NOT reset shares the parent "
        "dict by reference (this is the hazard)"
    )

    # Child WITH reset: rebinds its own dict in the copied context only.
    parent_snapshot_before = dict(parent)

    def _child_with_reset() -> None:
        reset_registry()
        _get_registry()["isolated"] = "child-conn"

    copy_context().run(_child_with_reset)
    assert parent == parent_snapshot_before, (
        "a copy_context child that resets its registry must NOT mutate the "
        "parent's — this is what BaseJob.handle / run_in_thread rely on"
    )
    _clear()
