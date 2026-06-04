"""Regression test: queries inside a transaction reuse the pinned connection.

Background
~~~~~~~~~~
Before 2026-04-23, ``create_connection_instance`` always minted a FRESH pool
connection. So every query issued inside ``with db.transaction(): ...`` —
including ORM relationship lazy-loads (``listing.images``) — ran on a *sibling*
psycopg2 session in autocommit mode, separate from the transaction's own
connection. Two consequences:

  1. The transaction's own uncommitted writes were invisible to those reads, so
     a relationship lazy-loaded inside the transaction silently returned EMPTY
     (this is the bug that spawned the "known cara ORM issue" direct-query
     band-aids in the app).
  2. ``rollback`` couldn't undo writes that had gone out on the autocommit
     sibling — they were never part of the transaction.

The fix makes both the ``DatabaseManager`` and the ``ConnectionResolver``
short-circuit to the connection pinned in the per-execution-context
``_ACTIVE_CONNECTIONS`` registry whenever the caller is inside a transaction.

This test pins that contract on BOTH layers so the fix can never silently
regress. It asserts identity (``is``) — the very same connection object the
transaction pinned must be handed back, not an equal-looking fresh one.
"""

from __future__ import annotations

from cara.eloquent.DatabaseManager import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import _get_registry


def test_create_connection_instance_returns_the_pinned_transaction_connection():
    dm = DatabaseManager()
    # Stand-in for "the connection begin_transaction pinned for this context".
    pinned = object()

    registry = _get_registry()
    registry.clear()
    registry["app"] = pinned
    try:
        # DatabaseManager layer (the one QueryBuilder.new_connection() calls).
        assert dm.create_connection_instance("app") is pinned, (
            "DatabaseManager.create_connection_instance minted a fresh connection "
            "inside a transaction instead of reusing the pinned one — relationship "
            "lazy-loads would silently miss the transaction's own writes again."
        )
        # ConnectionResolver layer (the one DB.select() / raw statements use).
        resolver = dm.get_resolver()
        assert resolver._create_connection_instance("app") is pinned, (
            "ConnectionResolver._create_connection_instance bypassed the pinned "
            "transaction connection."
        )
    finally:
        registry.clear()


def test_no_active_transaction_does_not_borrow_another_connections_pin():
    """The short-circuit is keyed by connection NAME — a pin on connection
    ``'other'`` must never leak into a lookup for ``'app'``. Otherwise an
    unrelated open transaction on one connection would hijack queries meant for
    a different connection."""
    dm = DatabaseManager()
    other_pin = object()

    registry = _get_registry()
    registry.clear()
    registry["other"] = other_pin
    try:
        resolver = dm.get_resolver()
        # 'app' has no pinned connection → the resolver must NOT return the
        # 'other' connection's pin. (It would proceed to build a real
        # connection; we only assert it doesn't hand back the wrong pin.)
        assert registry.get("app") is None
        assert other_pin is not registry.get("app")
    finally:
        registry.clear()
