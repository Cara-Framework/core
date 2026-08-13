"""View — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "View": (".View", "View"),
    "ViewCompiler": (".ViewCompiler", "ViewCompiler"),
    "ViewDirectives": (".ViewDirectives", "ViewDirectives"),
    "ViewDirectivesRegistry": (".ViewDirectivesRegistry", "ViewDirectivesRegistry"),
    "ViewEngine": (".ViewEngine", "ViewEngine"),
    "ViewInstance": (".ViewInstance", "ViewInstance"),
    "ViewProvider": (".ViewProvider", "ViewProvider"),
    "ViewRenderer": (".ViewRenderer", "ViewRenderer"),
}

__all__ = [
    "View",
    "ViewCompiler",
    "ViewDirectives",
    "ViewDirectivesRegistry",
    "ViewEngine",
    "ViewInstance",
    "ViewProvider",
    "ViewRenderer",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
