"""Cycle-2 Cara hunt: ``Cache.remember`` releases only its own regen slot.

The stampede slot (``stampede:remember:<key>``) lives ``stampede_lock_seconds``.
When the winner's callback outlives it, a second caller legitimately claims
the slot and starts computing. The winner's ``finally`` then runs a bare
``forget(lock_key)`` and deletes the SECOND caller's slot, so a third caller
claims it too: the heavy callback the lock exists to serialise now runs in
parallel against the same struggling downstream. ``CacheLock`` and
``WithoutOverlapping`` already release with the owner-fenced ``forget_if``;
``remember`` must too.
"""

from __future__ import annotations

from cara.cache.Cache import Cache

LOCK = "stampede:remember:report"


class _Store:
    """Dict-backed driver with the real add / forget / forget_if contract."""

    def __init__(self):
        self.data: dict[str, object] = {}

    def get(self, key, default=None, *, strict=True):
        return self.data.get(key, default)

    def put(self, key, value, ttl=None, *, strict=True):
        self.data[key] = value

    def add(self, key, value, ttl=None):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def forget(self, key):
        return self.data.pop(key, None) is not None

    def forget_if(self, key, expected):
        if self.data.get(key) == expected:
            del self.data[key]
            return True
        return False


def test_winner_does_not_release_a_slot_another_caller_now_holds():
    store = _Store()
    cache = Cache(application=None, default_driver="memory")
    cache.add_driver("memory", store)

    def slow_report():
        # The winner's slot expired mid-callback and a peer claimed it.
        store.data.pop(LOCK, None)
        assert store.add(LOCK, "peer-owner", 30)
        return {"rows": 3}

    assert cache.remember("report", 60, slow_report) == {"rows": 3}

    assert store.data.get(LOCK) == "peer-owner", "the peer's regen slot was deleted"


def test_second_chance_winner_does_not_release_a_slot_a_peer_now_holds():
    # The first winner crashed and its slot lapsed; this caller re-claims it
    # from the wait loop, then outlives it too. Its release must be fenced.
    store = _Store()
    store.data[LOCK] = "crashed-winner"
    cache = Cache(application=None, default_driver="memory")
    cache.add_driver("memory", store)
    real_get = store.get
    reads = []

    def get(key, default=None, *, strict=True):
        reads.append(key)
        if len(reads) == 2:  # first poll: the crashed winner's slot lapses
            store.data.pop(LOCK, None)
        return real_get(key, default, strict=strict)

    store.get = get

    def slow_report():
        store.data.pop(LOCK, None)
        assert store.add(LOCK, "peer-owner", 30)
        return {"rows": 3}

    assert cache.remember("report", 60, slow_report) == {"rows": 3}

    assert store.data.get(LOCK) == "peer-owner", "the peer's regen slot was deleted"


def test_each_remember_call_claims_with_its_own_owner_token():
    # The peer that re-claims a lapsed slot is normally another remember
    # call; a shared token would let the fence delete that peer's slot.
    store = _Store()
    cache = Cache(application=None, default_driver="memory")
    cache.add_driver("memory", store)
    tokens = []
    real_add = store.add

    def add(key, value, ttl=None):
        tokens.append(value)
        return real_add(key, value, ttl)

    store.add = add

    cache.remember("report", 60, lambda: 1)
    store.data.pop("report")
    cache.remember("report", 60, lambda: 2)

    assert len(tokens) == 2 and tokens[0] != tokens[1]
