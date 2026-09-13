"""The ``Cache`` manager behind the facade exposes every driver contract operation.

``CacheContract.throttle`` shipped on both drivers and was missing here, so the
facade could not resolve it and every throttled route answered 503 in a live
app — while every limiter test stayed green, because each faked the facade with
an object that had the method. A fake more permissive than the real manager
hides exactly this, so the parity is pinned against the real class.
"""

from __future__ import annotations

import inspect

from cara.cache.Cache import Cache
from cara.cache.contracts import CacheContract


def _operations(cls: type) -> set[str]:
    return {
        name
        for name, _member in inspect.getmembers(cls, inspect.isfunction)
        if not name.startswith("_")
    }


def test_every_contract_operation_is_reachable_through_the_manager() -> None:
    missing = _operations(CacheContract) - _operations(Cache)

    assert missing == set(), f"the Cache manager does not expose: {sorted(missing)}"
