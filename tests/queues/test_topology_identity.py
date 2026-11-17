"""Topology commands check identity and inventory before the first declare."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cara.exceptions import QueueException
from cara.queues import require_canonical_inventory, require_topology_identity


@pytest.mark.parametrize("access", ["topology", "full", " Topology ", None])
def test_topology_capable_identities_pass(access) -> None:
    require_topology_identity(SimpleNamespace(options={"broker_access": access}))


@pytest.mark.parametrize("access", ["consume", "publish", "none", "bogus"])
def test_other_identities_are_refused(access) -> None:
    with pytest.raises(QueueException, match="topology identity"):
        require_topology_identity(SimpleNamespace(options={"broker_access": access}))


def test_matching_inventory_passes() -> None:
    driver = SimpleNamespace(canonical_queues=frozenset({"a", "b"}))
    require_canonical_inventory(driver, ["b", "a"])


def test_drifted_inventory_names_both_sides() -> None:
    driver = SimpleNamespace(canonical_queues=frozenset({"a", "stale"}))
    with pytest.raises(QueueException) as error:
        require_canonical_inventory(driver, ["a", "new"])
    assert "['new']" in str(error.value) and "['stale']" in str(error.value)
