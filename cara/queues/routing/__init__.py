"""Process-local canonical queue routing."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ONE_WORD": (".QueueRouter", "ONE_WORD"),
    "QueueRouter": (".QueueRouter", "QueueRouter"),
    "RoutingKey": (".RoutingKey", "RoutingKey"),
    "matches_pattern": (".QueueRouter", "matches_pattern"),
    "patterns_overlap": (".QueueRouter", "patterns_overlap"),
}

__all__ = [
    "ONE_WORD",
    "QueueRouter",
    "RoutingKey",
    "matches_pattern",
    "patterns_overlap",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
