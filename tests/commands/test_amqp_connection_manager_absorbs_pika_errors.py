"""The worker's AMQP connection manager survives pika's own exception family.

Live defect: pika raises ``StreamLostError`` / ``ConnectionWrongStateError``
from ``close()`` on a connection whose stream died. They are ``AMQPError``
subclasses, not ``OSError`` or ``ConnectionError``, so the narrow suppress
lists let them through: ``close()`` raised out of the worker's teardown, and
``ensure_connection()`` reported "Failed to connect" (returned False) instead
of reconnecting in the same call. Before the fix (``git show
HEAD:cara/commands/core/AMQPConnectionManager.py``) the first test raised
``ConnectionWrongStateError`` and the second returned False.
"""

from __future__ import annotations

import pika

from cara.commands.core.AMQPConnectionManager import AMQPConnectionManager


class _HalfDead:
    """Reads open, but the stream is gone: every operation raises pika's errors."""

    is_closed = False

    def process_data_events(self, time_limit=0):
        raise pika.exceptions.StreamLostError("Transport indicated EOF")

    def close(self):
        raise pika.exceptions.ConnectionWrongStateError(
            "close() called on closed connection"
        )


class _Fresh:
    is_closed = False

    def process_data_events(self, time_limit=0):
        return None

    def close(self):
        self.is_closed = True


class _Manager(AMQPConnectionManager):
    def _create_connection(self):
        return _Fresh()


def test_close_absorbs_pikas_error_from_a_half_dead_connection() -> None:
    manager = _Manager(lambda *_a, **_k: None, driver=object())
    manager.connection = _HalfDead()

    manager.close()  # must not raise


def test_ensure_connection_reconnects_in_the_same_call_after_a_lost_stream() -> None:
    manager = _Manager(lambda *_a, **_k: None, driver=object())
    manager.connection = _HalfDead()

    assert manager.ensure_connection() is True
    assert isinstance(manager.connection, _Fresh)
