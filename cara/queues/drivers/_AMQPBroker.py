"""AMQP dead-letter inspection, topology arguments and connections."""

from __future__ import annotations

import ssl
from typing import Any
from urllib.parse import quote_plus, urlencode

try:
    import pika
except ImportError:  # pragma: no cover - optional queue extra
    pika = None  # type: ignore[assignment]

from cara.exceptions import QueueDriverLibraryNotFoundException, QueueException
from cara.facades import Log
from cara.queues.QueueState import DEAD_LETTER_EXCHANGE, DEAD_LETTER_QUEUE
from cara.queues.serializers.SignedJsonJobSerializer import SignedJsonJobSerializer


def _amqp_broker_get_dead_letter_messages(
    self, queue_name: str = DEAD_LETTER_QUEUE, limit: int = 100
) -> list[dict[str, Any]]:
    """
    Peek at dead letter queue messages without consuming them.

    Args:
        queue_name: Dead letter queue name
        limit: Maximum messages to retrieve

    Returns:
        List of message details (headers, body, routing_key)
    """
    messages = []
    try:
        self._connect(self.options)

        # Use basic_get to peek at messages without consuming
        for _ in range(limit):
            method, properties, body = self.channel.basic_get(queue_name, auto_ack=False)

            if method is None:
                break

            # Verify the signed JSON envelope without importing the job
            # class. A forged/corrupt DLQ record remains visible as raw
            # metadata but is never dynamically imported.
            try:
                envelope = SignedJsonJobSerializer.inspect_envelope(
                    body,
                    signing_keys=self.options.get("signing_keys", {}),
                    clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
                    max_age_seconds=int(
                        self.options.get(
                            "envelope_max_age_seconds",
                            SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                        )
                    ),
                    allow_not_before=True,
                    allow_expired=True,
                )
                payload = envelope["payload"]
                signature_valid = True
                temporal_status = SignedJsonJobSerializer.temporal_status(
                    envelope,
                    clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
                )
            except QueueException as exc:
                payload = {
                    "error": str(exc),
                    "raw": body.decode("utf-8", errors="replace"),
                }
                signature_valid = False
                temporal_status = "invalid"

            messages.append(
                {
                    "delivery_tag": method.delivery_tag,
                    "routing_key": method.routing_key,
                    "redelivered": method.redelivered,
                    "exchange": method.exchange,
                    "headers": dict(properties.headers or {}),
                    "priority": properties.priority,
                    "signature_valid": signature_valid,
                    "temporal_status": temporal_status,
                    "timestamp": properties.timestamp,
                    "payload": payload,
                }
            )

            # Don't consume - requeue the message
            self.channel.basic_nack(method.delivery_tag, requeue=True)

    except Exception as e:
        Log.error("Failed to get dead letter messages: %s", e, exc_info=True)
        raise
    finally:
        try:
            if self.channel is not None:
                self.channel.close()
        except OSError, ConnectionError, RuntimeError, AttributeError:
            pass
        try:
            if self.connection is not None:
                self.connection.close()
        except OSError, ConnectionError, RuntimeError, AttributeError:
            pass
        self.channel = None
        self.connection = None

    return messages


def _amqp_broker_replay_delivery(
    self,
    job_id: str,
    *,
    operator: str,
    reason: str,
) -> str:
    """Replay one audited expired/dead delivery directly from PostgreSQL."""
    return self._delivery_store.replay_from_ledger(
        job_id,
        operator=operator,
        reason=reason,
    )


def _amqp_broker_canonical_queue_arguments(
    self,
    queue_name: str,
    options: dict[str, Any] | None = None,
) -> dict[str, object]:
    """Return the one canonical declaration contract for AMQP queues."""
    opts = {**self.options, **(options or {})}
    exchange_name = opts.get("exchange", "")
    arguments: dict[str, object] = {
        "x-queue-type": "quorum",
        "x-delivery-limit": self._bounded_queue_argument(
            opts.get("delivery_limit", 20),
            field="delivery_limit",
            minimum=1,
            maximum=1000,
        ),
        "x-dead-letter-exchange": (
            f"{exchange_name}.dlx" if exchange_name else DEAD_LETTER_EXCHANGE
        ),
        "x-dead-letter-routing-key": f"dead.{queue_name}",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }

    max_priority = opts.get("max_priority")
    if isinstance(max_priority, bool) or not isinstance(max_priority, int):
        raise QueueException("AMQP max_priority must be an integer.")
    if not 1 <= max_priority <= 31:
        raise QueueException("AMQP max_priority must be between 1 and 31.")
    # RabbitMQ 4.3 quorum queues provide strict 0-31 priorities without
    # x-max-priority (that argument applies only to classic queues).

    for field, argument, default in (
        ("max_length", "x-max-length", 100000),
        ("max_length_bytes", "x-max-length-bytes", 1073741824),
    ):
        value = opts.get(field, default)
        if isinstance(value, bool) or not isinstance(value, int):
            raise QueueException(f"AMQP {field} must be an integer.")
        if value <= 0:
            raise QueueException(f"AMQP {field} must be positive.")
        arguments[argument] = value

    message_ttl = opts.get("message_ttl")
    if message_ttl is not None:
        if isinstance(message_ttl, bool) or not isinstance(message_ttl, int):
            raise QueueException("AMQP message_ttl must be an integer.")
        if message_ttl <= 0:
            raise QueueException("AMQP message_ttl must be positive.")
        arguments["x-message-ttl"] = message_ttl
    return arguments


def _amqp_broker_dead_letter_queue_arguments(
    self,
    options: dict[str, Any] | None = None,
) -> dict[str, object]:
    """Return the bounded quorum contract for untrusted broker quarantine."""
    opts = {**self.options, **(options or {})}
    return {
        "x-queue-type": "quorum",
        "x-delivery-limit": self._bounded_queue_argument(
            opts.get("delivery_limit", 20),
            field="delivery_limit",
            minimum=1,
            maximum=1000,
        ),
        "x-overflow": "reject-publish",
        "x-max-length": self._bounded_queue_argument(
            opts.get("max_length", 100000),
            field="max_length",
            minimum=1,
            maximum=2_147_483_647,
        ),
        "x-max-length-bytes": self._bounded_queue_argument(
            opts.get("max_length_bytes", 1073741824),
            field="max_length_bytes",
            minimum=1,
            maximum=9_223_372_036_854_775_807,
        ),
    }


def _amqp_broker_bounded_queue_argument(
    value: Any,
    *,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise QueueException(f"AMQP {field} must be an integer.")
    if not minimum <= value <= maximum:
        raise QueueException(f"AMQP {field} must be between {minimum} and {maximum}.")
    return value


def _amqp_broker_priority_name(self, job: Any, options: dict[str, Any]) -> str:
    explicit = options.get("priority")
    job_priority = getattr(job, "priority", None)
    if not isinstance(job_priority, (str, int)):
        job_priority = getattr(job, "job_priority", None)
    value = explicit if explicit is not None else job_priority
    if value is None:
        value = "default"
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise QueueException(f"Invalid AMQP job priority: {value!r}")
    if isinstance(value, int):
        return str(value)

    levels = options.get("priority_levels") or {}
    if value not in levels:
        valid = ", ".join(sorted(str(level) for level in levels))
        raise QueueException(f"Unknown AMQP job priority {value!r}. Valid: {valid}")
    return value


def _amqp_broker_message_priority(self, job: Any, options: dict[str, Any]) -> int:
    name = self._priority_name(job, options)
    max_priority = int(options.get("max_priority"))
    if name.isdigit():
        value = int(name)
    else:
        value = (options.get("priority_levels") or {}).get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise QueueException(f"AMQP priority {name!r} has no integer mapping.")
    if not 0 <= value <= max_priority:
        raise QueueException(
            f"AMQP priority {name!r}={value} exceeds queue max {max_priority}."
        )
    return value


def _amqp_broker_open_new_connection(self, opts: dict[str, Any]) -> tuple:
    """Open a brand-new connection + channel pair."""
    try:
        import pika  # local: heavy optional dep
    except ImportError:
        raise QueueDriverLibraryNotFoundException(
            "pika is required for AMQPDriver. Install with: pip install pika"
        )

    connection = pika.BlockingConnection(self._connection_parameters(opts))
    channel = connection.channel()
    channel.confirm_delivery()
    return connection, channel


def _amqp_broker_acquire_thread_connection(self, url: str, opts: dict[str, Any]) -> None:
    """Bind a connection + channel to this thread for the publish.

    Reuse only the current thread's own connection. Pika
    BlockingConnection objects are owner-affine and must never cross
    thread boundaries.
    """
    if self.connection is not None and self.channel is not None:
        # Already bound on this thread (typical case for hot
        # publishers reusing the same pika channel).
        try:
            if self.connection.is_open and self.channel.is_open:
                return
        except OSError, ConnectionError, RuntimeError, AttributeError:
            pass
        # Stale handle — drop it and fall through.
        self._discard_thread_connection()

    # No healthy owner-local handle — open a fresh connection.
    self.connection, self.channel = self._open_new_connection(opts)


def _amqp_broker_return_thread_connection(self, url: str) -> None:
    """Keep the healthy connection bound to its owner thread."""
    return


def _amqp_broker_discard_thread_connection(self) -> None:
    """Drop the thread-local connection without returning it to
    the pool. Used after a publish error."""
    conn, chan = self.connection, self.channel
    self.connection = None
    self.channel = None
    for handle in (chan, conn):
        try:
            if handle is not None:
                handle.close()
        except (
            OSError,
            ConnectionError,
            RuntimeError,
            AttributeError,
            pika.exceptions.AMQPError,
        ):
            # Best-effort discard: closing an ALREADY-closed channel /
            # connection makes pika raise ``ChannelWrongStateError`` /
            # ``ConnectionWrongStateError`` (both ``AMQPError`` subclasses).
            # The whole point here is to drop a dead handle, so a
            # "already closed" close is success, not a failure to surface.
            pass


def _amqp_broker_connect(self, opts: dict[str, Any]) -> None:
    """Bind a connection + channel to this thread.

    Kept for read-only DLQ inspection. Runtime topology mutation belongs
    exclusively to the deploy-time ``queue:topology`` command.
    """
    if self.connection is not None and self.channel is not None:
        try:
            if self.connection.is_open and self.channel.is_open:
                return
        except OSError, ConnectionError, RuntimeError, AttributeError:
            pass
    self.connection, self.channel = self._open_new_connection(opts)


def _amqp_broker_build_url(self, opts: dict[str, Any]) -> str:
    """Build AMQP connection URL with proper encoding."""

    connection_params = {
        "username": opts.get("username", ""),
        "password": opts.get("password", ""),
        "host": opts.get("host", "localhost"),
        "port": opts.get("port", 5672),
        "vhost": opts.get("vhost", "/"),
    }

    # URL encode username and password (handles special characters like *, #, %, etc.)
    encoded_username = quote_plus(connection_params["username"])
    encoded_password = quote_plus(connection_params["password"])

    # Encode vhost (/ becomes %2F)
    encoded_vhost = (
        "%2F"
        if not connection_params["vhost"] or connection_params["vhost"] == "/"
        else connection_params["vhost"].replace("/", "%2F")
    )

    scheme = str(opts.get("scheme", "amqp") or "amqp").lower()
    if scheme not in {"amqp", "amqps"}:
        raise QueueException("AMQP scheme must be 'amqp' or 'amqps'.")

    base_url = (
        f"{scheme}://{encoded_username}:{encoded_password}"
        f"@{connection_params['host']}:{connection_params['port']}/{encoded_vhost}"
    )

    # Append connection options if present
    connection_options = opts.get("connection_options")
    if connection_options:
        return f"{base_url}?{urlencode(connection_options)}"

    return base_url


def _amqp_broker_connection_parameters(self, opts: dict[str, Any]):
    """Build pika parameters, including verified TLS and optional mTLS."""
    if pika is None:
        raise QueueDriverLibraryNotFoundException(
            "pika is required for AMQPDriver. Install with: pip install pika"
        )

    parameters = pika.URLParameters(self._build_url(opts))
    parameters.connection_attempts = 1
    parameters.retry_delay = 0
    parameters.socket_timeout = float(opts.get("socket_timeout_seconds", 5))
    parameters.stack_timeout = float(opts.get("stack_timeout_seconds", 10))
    parameters.blocked_connection_timeout = float(
        opts.get("blocked_connection_timeout_seconds", 10)
    )
    parameters.heartbeat = int(opts.get("heartbeat_seconds", 60))
    scheme = str(opts.get("scheme", "amqp") or "amqp").lower()
    if scheme != "amqps":
        return parameters

    context = ssl.create_default_context(cafile=opts.get("ssl_ca_certs") or None)
    certfile = opts.get("ssl_certfile")
    keyfile = opts.get("ssl_keyfile")
    if bool(certfile) != bool(keyfile):
        raise QueueException(
            "RABBIT_SSL_CERTFILE and RABBIT_SSL_KEYFILE must be configured together."
        )
    if certfile and keyfile:
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    parameters.ssl_options = pika.SSLOptions(
        context,
        str(opts.get("host", "localhost")),
    )
    return parameters
