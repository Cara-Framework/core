"""The cache fake must answer every call the facade answers."""

from __future__ import annotations

import inspect

from cara.cache.Cache import Cache
from cara.testing.fakes import CacheFake

#: Facade methods a fake is not expected to carry: driver plumbing and
#: lifecycle, not the read/write surface a test exercises.
INFRASTRUCTURE = {
    "driver",
    "add_driver",
    "extend",
    "purge",
    "set_application",
    "get_application",
    "resolve",
    "register",
    "boot",
    "make",
    "flush_drivers",
}

#: Facade calls the fake does NOT answer yet, each with the reason it is still
#: outstanding. SHRINK-ONLY: adding a name here is a deliberate decision to
#: leave a hole a test can fall through, and the census exists so that decision
#: is made once, in the open, instead of being discovered as a Redis
#: connection error in CI.
KNOWN_GAPS = {
    # Distributed locks are the one part of the facade whose whole purpose is
    # cross-process coordination; a single-process dict fake can model the
    # happy path but not contention, and a lock that never blocks is a worse
    # answer than no lock at all.
    "lock",
    "exact_lock",
    # Tag sets need their own index to invalidate by tag; nothing in either
    # product tags a cache entry yet.
    "tags",
}


def _public(obj) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(obj, callable)
        if not name.startswith("_") and name not in INFRASTRUCTURE
    }


def test_the_fake_answers_every_call_the_facade_does() -> None:
    """A fake narrower than the facade is a test that cannot run.

    `remember_with_negative` was missing, so any suite reaching for it fell
    through to the live Redis: it needed a server on 127.0.0.1, and under
    `pytest --disable-socket` every write silently no-opped and the assertion
    failed for a reason that had nothing to do with the code under test.
    """
    gap = _public(Cache) - _public(CacheFake)
    surprises = sorted(gap - KNOWN_GAPS)
    assert not surprises, (
        "CacheFake cannot answer these Cache calls, so any test using them "
        f"silently falls back to a real cache: {surprises}. Implement them on "
        "the fake, or add them to KNOWN_GAPS with the reason."
    )
    repaid = sorted(KNOWN_GAPS - gap)
    assert not repaid, (
        f"the fake now answers {repaid} — remove the stale KNOWN_GAPS entry "
        "so the census keeps shrinking"
    )
