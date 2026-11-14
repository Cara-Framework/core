"""Publish a batch on one confirm-mode channel and wait for its confirms once.

``BlockingChannel.basic_publish`` in confirm mode waits for the broker's
``Basic.Ack`` of every message before it returns, so a relay batch of 64 rows
paid 64 confirm round trips — each one a quorum-queue log write. Measured on
a private RabbitMQ 4.2 node (2026-11-13, 1.4 KB persistent messages to a
quorum queue): 2.2k msg/s one by one, 10.8k msg/s when a batch of 64 is
published and its confirms are awaited together.

pika's blocking adapter has no batch-confirm call, so this channel is put in
confirm mode through its underlying ``pika.channel.Channel`` (``_impl``, a
stable attribute through pika 1.x; the products pin ``pika==1.4.4``) with a
callback that records every ``Basic.Ack``/``Basic.Nack`` (``multiple`` acks
included), and ``process_data_events`` drives the I/O loop until every tag of
the batch is settled. A mandatory message the broker cannot route comes back
as ``Basic.Return`` BEFORE its ack (RabbitMQ's documented order) and is
reported as ``UnroutableError``, exactly like the blocking path.

The channel belongs to the publishing thread's connection and dies with it:
any transport error, a closed channel or a confirm deadline raises, and the
driver discards the whole connection, so no tag bookkeeping outlives the
channel it numbered.
"""

from __future__ import annotations

import time
from typing import Any

try:
    import pika
except ImportError:  # pragma: no cover - exercised only without the extra
    pika = None  # type: ignore[assignment]

from cara.exceptions import QueueException


class AMQPConfirmBatch:
    """One publisher-confirm channel that settles a whole batch per wait."""

    #: Ceiling on one batch's confirm wait. The blocking path waits until the
    #: connection's heartbeat declares the broker gone; a deadline here turns
    #: a broker that stopped confirming into an ordinary publish failure.
    DEFAULT_CONFIRM_TIMEOUT_SECONDS = 30.0

    def __init__(self, connection: Any, *, confirm_timeout_seconds: float | None = None):
        self.connection = connection
        self.confirm_timeout_seconds = float(
            confirm_timeout_seconds or self.DEFAULT_CONFIRM_TIMEOUT_SECONDS
        )
        self.channel = connection.channel()
        self._impl = self.channel._impl
        self._next_tag = 1
        self._settled: dict[int, bool] = {}
        self._outstanding: set[int] = set()
        self._returned: dict[str, Any] = {}
        selected: list[bool] = []
        self._impl.confirm_delivery(
            ack_nack_callback=self._on_confirm,
            callback=lambda _frame: (selected.append(True), self._wake()),
        )
        self._impl.add_on_return_callback(self._on_return)
        self._impl.add_on_close_callback(lambda *_args: self._wake())
        self._wait_until(lambda: bool(selected), "Confirm.SelectOk")

    @property
    def is_open(self) -> bool:
        return bool(self.channel.is_open)

    def close(self) -> None:
        if self.channel.is_open:
            self.channel.close()

    def publish(self, messages: list[tuple[str, str, bytes, Any]]) -> list[Any]:
        """Publish ``(exchange, routing_key, body, properties)`` messages in order.

        Returns one outcome per message: ``None`` once the broker confirmed
        it, or the ``UnroutableError``/``NackError`` the blocking path would
        have raised for it. Raises (nothing confirmed is reported) when the
        transport fails or the confirms miss the deadline.
        """
        tags: list[int] = []
        for exchange, routing_key, body, properties in messages:
            self._impl.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=True,
            )
            tag = self._next_tag
            self._next_tag += 1
            self._outstanding.add(tag)
            tags.append(tag)
        self._wait_until(
            lambda: all(tag in self._settled for tag in tags),
            f"publisher confirms for {len(tags)} message(s)",
        )
        outcomes: list[Any] = []
        for (_exchange, _routing_key, _body, properties), tag in zip(
            messages, tags, strict=True
        ):
            acked = self._settled.pop(tag)
            returned = self._returned.pop(str(properties.message_id or ""), None)
            if not acked:
                outcomes.append(pika.exceptions.NackError([]))
            elif returned is not None:
                outcomes.append(pika.exceptions.UnroutableError([returned]))
            else:
                outcomes.append(None)
        return outcomes

    def _on_confirm(self, frame: Any) -> None:
        method = frame.method
        acked = isinstance(method, pika.spec.Basic.Ack)
        tag = int(method.delivery_tag)
        if method.multiple:
            for pending in [item for item in self._outstanding if item <= tag]:
                self._outstanding.discard(pending)
                self._settled[pending] = acked
        elif tag in self._outstanding:
            self._outstanding.discard(tag)
            self._settled[tag] = acked
        self._wake()

    def _wake(self) -> None:
        """Make the blocked ``process_data_events`` return to re-check.

        Frames handled on the underlying channel are not events of the
        blocking connection, so ``process_data_events(time_limit=None)``
        would keep polling past them; a no-op threadsafe callback is one.
        """
        self.connection.add_callback_threadsafe(_noop)

    def _on_return(
        self, _channel: Any, method: Any, properties: Any, body: bytes
    ) -> None:
        self._returned[str(properties.message_id or "")] = (
            pika.adapters.blocking_connection.ReturnedMessage(method, properties, body)
        )

    def _wait_until(self, done, what: str) -> None:
        """Drive the I/O loop until ``done()``; wake as soon as frames arrive.

        ``process_data_events(time_limit=t)`` with ``t > 0`` blocks for the
        WHOLE ``t`` even after the awaited confirm arrived (measured: every
        64-message batch took ~52 ms at ``t=0.05``), so the loop waits with
        ``time_limit=None`` — return on the next event of the blocking
        connection. Every confirm, the select-ok and a channel close post
        one (:meth:`_wake`), and a ``call_later`` timer is the event when the
        broker stays silent past the deadline.
        """
        deadline = time.monotonic() + self.confirm_timeout_seconds
        expired: list[bool] = []
        timer = self.connection.call_later(
            self.confirm_timeout_seconds, lambda: expired.append(True)
        )
        try:
            while not done():
                if self._impl.is_closed:
                    raise pika.exceptions.ChannelClosed(
                        0, f"confirm channel closed awaiting {what}"
                    )
                if expired or time.monotonic() >= deadline:
                    raise QueueException(
                        f"RabbitMQ did not confirm {what} within "
                        f"{self.confirm_timeout_seconds:g}s."
                    )
                self.connection.process_data_events(time_limit=None)
        finally:
            if not expired:
                self.connection.remove_timeout(timer)


def _noop() -> None:
    return None
