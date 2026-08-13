"""Decorators — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

from .Command import _run_after
from .Command import _run_before
from .Command import _run_on_error

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "EVENT_ORDER": (".Events", "EVENT_ORDER"),
    "RouteDecorator": (".RouteDecorator", "RouteDecorator"),
    "accessor": (".Accessor", "accessor"),
    "admin_only": (".Authorization", "admin_only"),
    "after_command": (".Command", "after_command"),
    "all_pending": (".route", "all_pending"),
    "authenticated_only": (".Authorization", "authenticated_only"),
    "authorize": (".Authorization", "authorize"),
    "before_command": (".Command", "before_command"),
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
    "on_error": (".Command", "on_error"),
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
    "after_command",
    "all_pending",
    "authenticated_only",
    "authorize",
    "before_command",
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
    "on_error",
    "saved",
    "saving",
    "scheduled",
    "updated",
    "updating",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
