"""Queue worker runtime collaborator."""

from __future__ import annotations

import contextlib
import logging
import sys

from cara.exceptions import ConfigurationException
from cara.facades import Log

_logger = logging.getLogger("cara.queue.worker")


class AMQPConnectionManager:
    """Manages AMQP connections for queue workers (Single Responsibility)."""

    def __init__(self, config_func, driver=None):
        self.config = config_func
        self.driver = driver
        self.connection = None

    def ensure_connection(self) -> bool:
        """Ensure AMQP connection is alive.

        Treats any prior operational failure (``StreamLostError``,
        ``ConnectionClosedByBroker``, TCP RST during a long job)
        as "connection is dead" even when ``is_closed`` still reports
        False. pika's BlockingConnection occasionally keeps a zombie
        connection object after the underlying stream dies; the next
        ``channel()`` call then explodes with the original
        ``StreamLostError`` instead of transparently reconnecting.
        A fresh heartbeat probe rules that out.
        """
        try:
            if self.connection is not None and not self.connection.is_closed:
                try:
                    # Cheap liveness probe — pika doesn't expose a
                    # dedicated ``ping``; dispatching data events
                    # triggers a heartbeat exchange and surfaces a
                    # stale connection as an exception here rather
                    # than much later in the consumer loop.
                    self.connection.process_data_events(time_limit=0)
                except Exception:
                    with contextlib.suppress(
                        OSError, RuntimeError, AttributeError, ConnectionError
                    ):
                        self.connection.close()
                    self.connection = None

            if self.connection is None or self.connection.is_closed:
                self.connection = self._create_connection()
            return True
        except Exception as e:
            try:
                Log.error("Failed to connect to RabbitMQ: %s", e, exc_info=True)
            except ImportError, RuntimeError:
                print(
                    f"[QueueWorkCommand] Failed to connect to RabbitMQ: {e}",
                    file=sys.stderr,
                )
            self.connection = None
            return False

    def _create_connection(self):
        """Create a new AMQP connection from the driver's parameters.

        ``AMQPDriver._connection_parameters`` is the ONE source of AMQP
        connection truth: it is the only place that reads ``scheme``,
        builds the verified TLS context (``check_hostname`` +
        ``CERT_REQUIRED``), refuses a half-configured mTLS pair, and pins
        ``connection_attempts=1`` / ``retry_delay=0`` so a dead broker
        surfaces here instead of inside pika's own retry loop.

        This method used to restate those parameters in an ``else``
        branch for the case where the driver could not supply them. That
        copy knew nothing about ``scheme``: it built
        ``pika.ConnectionParameters`` with ``PlainCredentials`` and no
        ``ssl_options``, so a worker whose operator had configured
        ``RABBIT_SCHEME=amqps`` would have connected in PLAINTEXT and put
        the broker username and password on the wire. It survived only
        because an unrelated precondition 1600 lines away happened to
        reject drivers without ``_connection_parameters``; relaxing that
        check would have silently downgraded every worker connection.
        Reading the SSOT removes the downgrade path entirely — a driver
        that cannot describe its own connection is a misconfiguration and
        must fail loudly, never connect insecurely.
        """
        import pika  # local: heavy optional dep

        if self.driver is None or not hasattr(self.driver, "_connection_parameters"):
            raise ConfigurationException(
                "queue:work requires the AMQP driver for durable subscriptions; "
                "refusing to build connection parameters without it."
            )
        return pika.BlockingConnection(
            self.driver._connection_parameters(self.driver.options)
        )

    def create_channel(self):
        """Create fresh channel for queue operations."""
        if not self.ensure_connection():
            return None
        return self.connection.channel()

    def close(self):
        """Clean up connection."""
        if self.connection and not self.connection.is_closed:
            with contextlib.suppress(ImportError, RuntimeError, AttributeError, OSError):
                self.connection.close()
