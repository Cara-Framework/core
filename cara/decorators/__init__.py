"""Decorators — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "EVENT_ORDER": (".Events", "EVENT_ORDER"),
    "RouteDecorator": (".RouteDecorator", "RouteDecorator"),
    "accessor": (".Accessor", "accessor"),
    "admin_only": (".Authorization", "admin_only"),
    "all_pending": (".route", "all_pending"),
    "authenticated_only": (".Authorization", "authenticated_only"),
    "authorize": (".Authorization", "authorize"),
    "can": (".Authorization", "can"),
    "can_any": (".Authorization", "can_any"),
    "clear": (".route", "clear"),
    "command": (".Command", "command"),
    "created": (".Events", "created"),
    "creating": (".Events", "creating"),
    "deleted": (".Events", "deleted"),
    "deleting": (".Events", "deleting"),
    "get_registered_commands": (".Command", "get_registered_commands"),
    "guest_only": (".Authorization", "guest_only"),
    "mutator": (".Mutator", "mutator"),
    "saved": (".Events", "saved"),
    "saving": (".Events", "saving"),
    "scheduled": (".Schedule", "scheduled"),
    "updated": (".Events", "updated"),
    "updating": (".Events", "updating"),
}

__all__ = [
    "EVENT_ORDER",
    "RouteDecorator",
    "accessor",
    "admin_only",
    "all_pending",
    "authenticated_only",
    "authorize",
    "can",
    "can_any",
    "clear",
    "command",
    "created",
    "creating",
    "deleted",
    "deleting",
    "get_registered_commands",
    "guest_only",
    "mutator",
    "saved",
    "saving",
    "scheduled",
    "updated",
    "updating",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
