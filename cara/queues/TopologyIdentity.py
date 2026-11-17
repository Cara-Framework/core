"""Preconditions every broker-topology command checks before declaring.

``queue:reconcile`` / ``queue:topology`` declare and delete queues. Two
checks belong before the first declaration, in every product:

* **identity** — the process holds a broker identity that may change
  topology: ``topology``, or ``full`` (which ``rabbit_broker_access`` already
  forbids in production). A ``consume``/``publish``/``none`` process must not
  even try: the broker would refuse half-way, after some queues changed.
* **inventory** — the driver's ``canonical_queues`` (what it accepts
  dispatches for) equals the inventory the command is about to declare. A
  drift means a lane the workers publish to is never declared, or the
  reverse.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cara.exceptions import QueueException

#: Broker identities allowed to declare or delete queues.
TOPOLOGY_ACCESS_LEVELS = frozenset({"topology", "full"})


def require_topology_identity(driver: Any) -> None:
    """Raise unless the driver's ``broker_access`` may change topology."""
    options = getattr(driver, "options", None) or {}
    access = str(options.get("broker_access") or "full").strip().lower()
    if access not in TOPOLOGY_ACCESS_LEVELS:
        raise QueueException(
            f"Broker topology changes need the topology identity; this process "
            f"holds broker_access={access!r}."
        )


def require_canonical_inventory(driver: Any, expected: Iterable[str]) -> None:
    """Raise unless the driver's ``canonical_queues`` equal *expected*."""
    configured = frozenset(str(name) for name in getattr(driver, "canonical_queues", ()))
    wanted = frozenset(str(name) for name in expected)
    if configured != wanted:
        missing = sorted(wanted - configured)
        extra = sorted(configured - wanted)
        raise QueueException(
            "AMQP driver canonical queue inventory differs from the declared "
            f"inventory (missing from driver: {missing}; extra in driver: {extra})."
        )


__all__ = [
    "TOPOLOGY_ACCESS_LEVELS",
    "require_canonical_inventory",
    "require_topology_identity",
]
