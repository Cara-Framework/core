"""Queue worker runtime collaborator."""

from __future__ import annotations

import logging
import threading

_logger = logging.getLogger("cara.queue.worker")


class ThreadSafeAMQPAckChannel:
    """Expose ACK/NACK to job threads without touching pika off its I/O thread."""

    def __init__(
        self,
        connection,
        channel,
        timeout_seconds: int = 30,
        on_settled=None,
    ):
        self._connection = connection
        self._channel = channel
        self._timeout_seconds = timeout_seconds
        self._on_settled = on_settled
        self._settled = False
        self._settled_lock = threading.Lock()

    def basic_ack(self, *, delivery_tag) -> None:
        self._schedule(
            lambda: self._channel.basic_ack(delivery_tag=delivery_tag),
            operation="ACK",
        )

    def basic_nack(self, *, delivery_tag, requeue: bool) -> None:
        self._schedule(
            lambda: self._channel.basic_nack(
                delivery_tag=delivery_tag,
                requeue=requeue,
            ),
            operation="NACK",
        )

    def _schedule(self, callback, *, operation: str) -> None:
        completed = threading.Event()
        errors: list[BaseException] = []

        def _run() -> None:
            try:
                # Refused BEFORE the frame goes out. The check used to follow
                # ``callback()``, so the second ACK/NACK of one delivery tag
                # was already on the wire when this raised: RabbitMQ answers
                # an unknown tag with a channel-level PRECONDITION_FAILED,
                # which closes the consumer's channel and returns every
                # delivery it still holds as a failed delivery, spending the
                # quorum queue's delivery limit. Every ``_run`` executes on
                # the connection's own I/O thread, one after another, so the
                # check and the frame cannot interleave with another settle;
                # a frame that fails leaves the delivery unsettled.
                with self._settled_lock:
                    if self._settled:
                        raise RuntimeError(
                            "RabbitMQ delivery was settled more than once."
                        )
                callback()
                with self._settled_lock:
                    self._settled = True
                if self._on_settled is not None:
                    self._on_settled()
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed.set()

        if self._connection is None or self._connection.is_closed:
            raise ConnectionError(f"RabbitMQ connection closed before {operation}")
        self._connection.add_callback_threadsafe(_run)
        if not completed.wait(self._timeout_seconds):
            raise TimeoutError(
                f"RabbitMQ {operation} was not processed within "
                f"{self._timeout_seconds} seconds"
            )
        if errors:
            raise errors[0]
