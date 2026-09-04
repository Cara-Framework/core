"""
Cara Tinker Package

Laravel-style interactive shell for Cara framework.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Repl": (".Repl", "Repl"),
    "ScriptRunner": (".ScriptRunner", "ScriptRunner"),
    "Shell": (".Shell", "Shell"),
    "TinkerProvider": (".TinkerProvider", "TinkerProvider"),
}

__all__ = [
    "Repl",
    "ScriptRunner",
    "Shell",
    "TinkerProvider",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
