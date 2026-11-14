"""Durable AMQP delivery ledger."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "DeliveryClaim": (".DeliveryClaim", "DeliveryClaim"),
    "DeliveryEnvelopeExpired": (".DeliveryEnvelopeExpired", "DeliveryEnvelopeExpired"),
    "DeliveryEnvelopeMismatch": (
        ".DeliveryEnvelopeMismatch",
        "DeliveryEnvelopeMismatch",
    ),
    "DeliveryLeaseLost": (".DeliveryLeaseLost", "DeliveryLeaseLost"),
    "PublicationBacklogProbe": (".PublicationBacklogProbe", "PublicationBacklogProbe"),
    "QueueJobDeliveryStore": (".QueueJobDeliveryStore", "QueueJobDeliveryStore"),
    "QueueOperationsStore": (".QueueOperationsStore", "QueueOperationsStore"),
    "QueueOutboxHealth": (".QueueOutboxHealth", "QueueOutboxHealth"),
    "ReplayDelivery": (".ReplayDelivery", "ReplayDelivery"),
    "TerminalHookClaim": (".TerminalHookClaim", "TerminalHookClaim"),
    "UniqueDeliveryConflict": (".UniqueDeliveryConflict", "UniqueDeliveryConflict"),
    "relay_lane_of": ("._DeliveryConfiguration", "_delivery_relay_lane_of"),
}

__all__ = [
    "DeliveryClaim",
    "DeliveryEnvelopeExpired",
    "DeliveryEnvelopeMismatch",
    "DeliveryLeaseLost",
    "PublicationBacklogProbe",
    "QueueJobDeliveryStore",
    "QueueOperationsStore",
    "QueueOutboxHealth",
    "ReplayDelivery",
    "TerminalHookClaim",
    "UniqueDeliveryConflict",
    "relay_lane_of",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
