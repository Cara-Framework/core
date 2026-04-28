"""Manager — abstract base for driver-style services.

Laravel's ``Illuminate\\Support\\Manager`` parity. The pattern
behind ``Cache::store('redis')``, ``Queue::connection('sqs')``,
``Notification::channel('slack')``: a single facade-like object
that lazily builds + caches per-name driver instances.

Subclasses define:

* :meth:`get_default_driver` — what name to use when callers
  don't pass one.
* ``create_<name>_driver(self) -> Any`` methods — one per
  supported driver.

The base class memoises each driver instance and dispatches
unknown attribute access to the default driver, so callers can
write ``cache.get(key)`` instead of ``cache.driver().get(key)``::

    class CacheManager(Manager):
        def get_default_driver(self) -> str: return "redis"
        def create_redis_driver(self): return RedisStore(...)
        def create_array_driver(self): return ArrayStore()

    cache = CacheManager(app)
    cache.driver().get("k")          # uses redis (default)
    cache.driver("array").put("k", 1)  # explicit driver
    cache.get("k")                   # auto-forwards to default
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional


class Manager:
    """Abstract driver-instance cache + dispatcher."""

    def __init__(self, application: Any = None) -> None:
        self.application = application
        # Per-name memoised driver instances. Keys are normalised
        # driver names (the same value passed to :meth:`driver`).
        self._drivers: Dict[str, Any] = {}
        # Custom driver factories registered at runtime via
        # :meth:`extend` — Laravel ``Manager::extend`` parity.
        self._custom_creators: Dict[str, Callable[[Any], Any]] = {}

    # ── Subclass contract ───────────────────────────────────────────

    def get_default_driver(self) -> str:
        """Return the name of the default driver — override in subclass."""
        raise NotImplementedError(
            f"{type(self).__name__} must implement get_default_driver()"
        )

    # ── Public API ──────────────────────────────────────────────────

    def driver(self, name: Optional[str] = None) -> Any:
        """Return (and lazily build) the named driver."""
        if name is None:
            name = self.get_default_driver()
        if not name:
            raise ValueError(
                f"{type(self).__name__} has no default driver configured"
            )
        if name not in self._drivers:
            self._drivers[name] = self._resolve(name)
        return self._drivers[name]

    def extend(self, name: str, callback: Callable[[Any], Any]) -> "Manager":
        """Register a custom driver factory.

        ``callback(application) -> driver`` mirrors Laravel's
        ``Manager::extend`` signature. Useful for service
        providers attaching app-specific drivers without
        subclassing the manager itself.
        """
        self._custom_creators[name] = callback
        # Bust the cache so a re-extended driver name picks up the
        # new factory next time it's resolved.
        self._drivers.pop(name, None)
        return self

    def has_driver(self, name: str) -> bool:
        """True if ``name`` is currently instantiated (warm cache)."""
        return name in self._drivers

    def forget_drivers(self) -> None:
        """Discard every memoised driver — Laravel ``forgetDrivers``."""
        self._drivers.clear()

    def get_drivers(self) -> Dict[str, Any]:
        """Return a snapshot of currently-instantiated drivers."""
        return dict(self._drivers)

    # ── Internals ──────────────────────────────────────────────────

    def _resolve(self, name: str) -> Any:
        # Custom (extend-registered) drivers win over built-in
        # ``create_<name>_driver`` methods, matching Laravel.
        if name in self._custom_creators:
            return self._custom_creators[name](self.application)
        creator = getattr(self, f"create_{name}_driver", None)
        if creator is None:
            raise ValueError(
                f"Driver [{name}] not supported by {type(self).__name__}. "
                f"Define create_{name}_driver() or call .extend({name!r}, factory)."
            )
        return creator()

    # ── Sugar: forward unknown attrs to the default driver ─────────

    def __getattr__(self, name: str) -> Any:
        # ``__getattr__`` only fires when normal lookup misses, so
        # subclass methods + the ones above resolve without
        # entering this branch. Methods starting with ``_`` are
        # reserved for internals to avoid feedback loops while
        # the object is being constructed.
        if name.startswith("_"):
            raise AttributeError(name)
        driver = self.driver()
        return getattr(driver, name)


__all__ = ["Manager"]
