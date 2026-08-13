"""Events — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Event": (".Event", "Event"),
    "EventContract": (".contracts", "EventContract"),
    "EventProvider": (".EventProvider", "EventProvider"),
    "EventSubscriber": (".EventSubscriber", "EventSubscriber"),
    "HandleListenerJob": (".jobs", "HandleListenerJob"),
    "Listener": (".contracts", "Listener"),
}

__all__ = [
    "Event",
    "EventContract",
    "EventProvider",
    "EventSubscriber",
    "HandleListenerJob",
    "Listener",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
