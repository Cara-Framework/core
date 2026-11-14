"""Queues — layer barrel (generated, DOCTRINE §5.1). — drivers subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AMQPConfirmBatch": (".AMQPConfirmBatch", "AMQPConfirmBatch"),
    "AMQPDriver": (".AMQPDriver", "AMQPDriver"),
}

__all__ = [
    "AMQPConfirmBatch",
    "AMQPDriver",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
