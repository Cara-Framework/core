"""The confirm channel's on-close callback never raises into pika's teardown.

When the TCP stream drops, pika closes the connection first and runs the
channel close callbacks second. Waking the waiter through
``add_callback_threadsafe`` then raises ``ConnectionWrongStateError`` from
inside the teardown, which hid the real transport error in the relay's log.
"""

from __future__ import annotations

from types import SimpleNamespace

import pika

from cara.queues.drivers import AMQPConfirmBatch


class _Impl:
    def __init__(self):
        self.is_closed = False
        self.on_select_ok = None
        self.on_close = None

    def confirm_delivery(self, *, ack_nack_callback, callback):
        self.on_select_ok = callback

    def add_on_return_callback(self, callback):
        pass

    def add_on_close_callback(self, callback):
        self.on_close = callback


class _Connection:
    def __init__(self):
        self.impl = _Impl()
        self.closed = False
        self.wakes = 0

    def channel(self):
        return SimpleNamespace(_impl=self.impl, is_open=True, close=lambda: None)

    def call_later(self, _delay, callback):
        return callback

    def remove_timeout(self, _timer):
        pass

    def add_callback_threadsafe(self, _callback):
        if self.closed:
            raise pika.exceptions.ConnectionWrongStateError(
                "add_callback_threadsafe() called on closed or closing connection."
            )
        self.wakes += 1

    def process_data_events(self, time_limit=0):
        self.impl.on_select_ok(None)


def test_a_close_on_an_open_connection_wakes_the_waiter() -> None:
    connection = _Connection()
    AMQPConfirmBatch(connection)
    before = connection.wakes

    connection.impl.on_close(None, None)

    assert connection.wakes == before + 1


def test_a_close_after_the_connection_died_does_not_raise() -> None:
    connection = _Connection()
    AMQPConfirmBatch(connection)
    connection.closed = True

    # Pre-fix: ConnectionWrongStateError out of pika's stream teardown.
    connection.impl.on_close(None, None)
