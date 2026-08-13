"""
Queue Tracking Package.

Advanced job tracking and monitoring for Cara Framework.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "JobTracker": (".JobTracker", "JobTracker"),
    "Trackable": (".Trackable", "Trackable"),
}

__all__ = [
    "JobTracker",
    "Trackable",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
