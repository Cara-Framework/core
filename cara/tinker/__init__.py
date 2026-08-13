"""
Cara Tinker Package

Laravel-style interactive shell for Cara framework.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Command": (".Command", "Command"),
    "Repl": (".Repl", "Repl"),
    "ScriptRunner": (".ScriptRunner", "ScriptRunner"),
    "Shell": (".Shell", "Shell"),
    "TinkerProvider": (".TinkerProvider", "TinkerProvider"),
    "create_tinker_command": (".Command", "create_tinker_command"),
    "main": (".Command", "main"),
}

__all__ = [
    "Command",
    "Repl",
    "ScriptRunner",
    "Shell",
    "TinkerProvider",
    "create_tinker_command",
    "main",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
