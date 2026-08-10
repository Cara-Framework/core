"""``migrate:reset`` idempotency flush — the namespace vocabulary has ONE owner.

The flusher used to restate the cache-key prefixes as a literal tuple
``("job_result:*", "job_lock:*")``. ``MakesIdempotentBase`` later minted a
third namespace, ``job_fence:``, and the copy had no way to learn: fence
counters survived every reset while the command still printed
"🧹 Flushed N job idempotency cache entries", so the operator believed the
idempotency state was clean.

These tests pin the SSOT direction — the command reads the mixin's own
``RESET_FLUSHABLE_KEY_PREFIXES`` — and pin the deliberate exclusion of the
fence, which must stay unflushed because it is a monotonic counter.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import cara.facades
from cara.commands.core.MigrateResetCommand import MigrateResetCommand
from cara.queues.idempotency.MakesIdempotentBase import MakesIdempotentBase


def _make_command() -> MigrateResetCommand:
    cmd = MigrateResetCommand(application=None)
    cmd.set_parsed_options({})
    cmd.console = MagicMock()
    return cmd


def _flush_and_capture(monkeypatch) -> list[str]:
    """Run the flusher against a recording Cache double."""
    prefixes: list[str] = []
    monkeypatch.setattr(
        cara.facades,
        "Cache",
        SimpleNamespace(
            forget_by_prefix=lambda prefix: prefixes.append(prefix) or 1,
        ),
    )
    _make_command()._flush_job_idempotency_cache()
    return prefixes


def test_reset_flushes_exactly_the_namespaces_the_mixin_declares(monkeypatch):
    """The flushed set is derived, not restated."""
    prefixes = _flush_and_capture(monkeypatch)

    assert prefixes == list(MakesIdempotentBase.RESET_FLUSHABLE_KEY_PREFIXES)


def test_a_namespace_added_to_the_mixin_is_flushed_without_touching_the_command(
    monkeypatch,
):
    """Pins the drift itself.

    Against the pre-fix literal tuple this fails: the command flushed
    ``job_result:*`` / ``job_lock:*`` and nothing else, so a namespace
    added to ``MakesIdempotentBase`` (exactly what happened to
    ``job_fence:``) survived the reset silently.
    """
    monkeypatch.setattr(
        MakesIdempotentBase,
        "RESET_FLUSHABLE_KEY_PREFIXES",
        (*MakesIdempotentBase.RESET_FLUSHABLE_KEY_PREFIXES, "job_future_namespace:"),
    )

    assert "job_future_namespace:" in _flush_and_capture(monkeypatch)


def test_the_monotonic_fence_is_never_flushed(monkeypatch):
    """A reset may not restart the owner fence.

    ``forget_pattern`` really does scan the Redis counter namespace, so
    including the fence would zero it and re-issue owner tokens that have
    already been handed out — fence reuse is the precise failure the fence
    exists to prevent. A too-high fence after a reset is harmless.
    """
    assert (
        MakesIdempotentBase.FENCE_KEY_PREFIX
        not in MakesIdempotentBase.RESET_FLUSHABLE_KEY_PREFIXES
    )
    assert MakesIdempotentBase.FENCE_KEY_PREFIX == "job_fence:"

    assert "job_fence:" not in _flush_and_capture(monkeypatch)


def test_every_minted_key_namespace_is_classified():
    """Census guard: a new ``*_KEY_PREFIX`` must be classified deliberately.

    The reset blind spot was created by ADDING a namespace, so the guard
    that matters fires on addition. Extending this census is the moment to
    decide whether the new namespace belongs in
    ``RESET_FLUSHABLE_KEY_PREFIXES``.
    """
    minted = {
        name: getattr(MakesIdempotentBase, name)
        for name in dir(MakesIdempotentBase)
        if name.endswith("_KEY_PREFIX")
    }

    assert minted == {
        "COOLDOWN_KEY_PREFIX": "collection_cooldown:",
        "FENCE_KEY_PREFIX": "job_fence:",
        "LOCK_KEY_PREFIX": "job_lock:",
        "RESULT_KEY_PREFIX": "job_result:",
    }


def test_key_helpers_compose_from_the_declared_prefixes():
    """The mixin's own eight call sites read the constants too (§5)."""
    job = MakesIdempotentBase()
    job._idempotency_key = "abc123"

    assert job._result_key() == "job_result:abc123"
    assert job._lock_key() == "job_lock:abc123"
    assert job._fence_key() == "job_fence:abc123"
