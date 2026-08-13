"""Queues — layer barrel (generated, DOCTRINE §5.1). — contracts subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseJob": (".BaseJob", "BaseJob"),
    "BaseQueueable": (".BaseQueueable", "BaseQueueable"),
    "CancellableJob": (".CancellableJob", "CancellableJob"),
    "JobCancelledException": (".JobCancelledException", "JobCancelledException"),
    "JobThrottledException": (".JobThrottledException", "JobThrottledException"),
    "PendingDispatch": (".PendingDispatch", "PendingDispatch"),
    "QueueContract": (".QueueContract", "QueueContract"),
    "Queueable": (".Queueable", "Queueable"),
    "SerializesModels": (".SerializesModels", "SerializesModels"),
    "ShouldDispatchAfterCommit": (
        ".ShouldDispatchAfterCommit",
        "ShouldDispatchAfterCommit",
    ),
    "ShouldQueue": (".ShouldQueue", "ShouldQueue"),
    "UniqueJob": (".UniqueJob", "UniqueJob"),
}

__all__ = [
    "BaseJob",
    "BaseQueueable",
    "CancellableJob",
    "JobCancelledException",
    "JobThrottledException",
    "PendingDispatch",
    "QueueContract",
    "Queueable",
    "SerializesModels",
    "ShouldDispatchAfterCommit",
    "ShouldQueue",
    "UniqueJob",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
