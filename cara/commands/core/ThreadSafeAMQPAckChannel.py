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
                callback()
                with self._settled_lock:
                    if self._settled:
                        raise RuntimeError(
                            "RabbitMQ delivery was settled more than once."
                        )
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
