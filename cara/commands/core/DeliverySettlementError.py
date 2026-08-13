"""Durable execution finished but broker settlement did not."""

from cara.exceptions import QueueException


class DeliverySettlementError(QueueException, RuntimeError):
    """Execution finished but durable delivery settlement did not."""
