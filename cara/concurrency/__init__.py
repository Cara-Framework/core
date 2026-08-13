"""Concurrency primitives — deduplicated background tasks (more to come).

Generic, framework-level concurrency helpers. Apps pass their own
cache keys and coroutine factories; cara owns the dedup + cleanup
plumbing.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {}

__all__: list[str] = []

_install_lazy_exports(__name__, _LAZY_EXPORTS)
