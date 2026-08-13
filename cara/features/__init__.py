"""Feature flags — cached, fail-closed runtime gate (Pennant-lite)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ABSENT": (".FeatureManager", "ABSENT"),
    "Feature": (".FeatureManager", "Feature"),
    "FeatureManager": (".FeatureManager", "FeatureManager"),
    "bucket": (".FeatureManager", "bucket"),
}

__all__ = [
    "ABSENT",
    "Feature",
    "FeatureManager",
    "bucket",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
