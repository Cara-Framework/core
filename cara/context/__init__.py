"""Cara Context Module."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ACTOR_CARRIER": (".Actor", "ACTOR_CARRIER"),
    "AMBIENT_KEY": (".Ambient", "AMBIENT_KEY"),
    "Actor": (".Actor", "Actor"),
    "Ambient": (".Ambient", "Ambient"),
    "CENTRAL": (".Tenancy", "CENTRAL"),
    "ExecutionContext": (".ExecutionContext", "ExecutionContext"),
    "Tenancy": (".Tenancy", "Tenancy"),
    "ThreadLane": (".ThreadLane", "ThreadLane"),
    "ThreadLaneBusy": (".ThreadLaneBusy", "ThreadLaneBusy"),
    "UNSET": (".Tenancy", "UNSET"),
    "acting_as": (".Actor", "acting_as"),
    "current_actor": (".Actor", "current_actor"),
    "set_actor": (".Actor", "set_actor"),
}

__all__ = [
    "ACTOR_CARRIER",
    "AMBIENT_KEY",
    "Actor",
    "Ambient",
    "CENTRAL",
    "ExecutionContext",
    "Tenancy",
    "ThreadLane",
    "ThreadLaneBusy",
    "UNSET",
    "acting_as",
    "current_actor",
    "set_actor",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
