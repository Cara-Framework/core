"""Fluent — dot-access wrapper around a dict.

Laravel's ``Illuminate\\Support\\Fluent`` parity. Lets you treat a
plain dict as an object with attribute access, while still
supporting item access and chainable mutation::

    cfg = Fluent({"host": "localhost", "port": 5432})
    cfg.host                        # "localhost"
    cfg.set("port", 6543).set("ssl", True)
    cfg.get("missing", "default")   # "default"
    cfg.to_dict()                   # {"host": ..., "port": ..., "ssl": ...}

Useful as the parameter bag for command/option DTOs, config
objects pulled from JSON, or any "loose schema" payload where you
want both ``obj.x`` ergonomics and ``obj["x"]`` flexibility.

Mirrors Laravel: missing attributes return ``None`` (not raise),
and ``__set__`` writes through to the underlying dict.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Mapping, Optional


class Fluent:
    """Dot-access dict wrapper with chainable ``set()``."""

    __slots__ = ("_attributes",)

    def __init__(self, attributes: Optional[Mapping[str, Any]] = None) -> None:
        # Coerce to a fresh dict so mutations don't leak into the caller's
        # source mapping.
        object.__setattr__(self, "_attributes", dict(attributes) if attributes else {})

    # ── Core accessors ──────────────────────────────────────────────

    def get(self, key: str, default: Any = None) -> Any:
        """Return ``key`` or ``default`` — Laravel ``Fluent::get``."""
        return self._attributes.get(key, default)

    def set(self, key: str, value: Any) -> "Fluent":
        """Set ``key`` and return self for chaining."""
        self._attributes[key] = value
        return self

    def has(self, key: str) -> bool:
        """True if ``key`` is present (regardless of value)."""
        return key in self._attributes

    def forget(self, *keys: str) -> "Fluent":
        """Remove ``keys`` if present — chainable."""
        for key in keys:
            self._attributes.pop(key, None)
        return self

    def merge(self, attributes: Mapping[str, Any]) -> "Fluent":
        """Shallow-merge ``attributes`` over current — chainable."""
        self._attributes.update(attributes)
        return self

    def only(self, *keys: str) -> Dict[str, Any]:
        """Return a dict containing only ``keys`` that exist."""
        return {k: self._attributes[k] for k in keys if k in self._attributes}

    def except_(self, *keys: str) -> Dict[str, Any]:
        """Return a dict excluding ``keys``."""
        return {k: v for k, v in self._attributes.items() if k not in keys}

    # ── Conversion ──────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """Return a shallow copy of the underlying dict."""
        return dict(self._attributes)

    def to_json(self) -> str:
        """JSON-encode the underlying dict — Laravel parity."""
        import json

        return json.dumps(self._attributes, default=str)

    # ── Magic methods ───────────────────────────────────────────────

    def __getattr__(self, key: str) -> Any:
        # Called only when normal attribute lookup misses; attribute names
        # starting with ``_`` are reserved for internals to avoid
        # accidental shadowing of slots / dunder names.
        if key.startswith("_"):
            raise AttributeError(key)
        return self._attributes.get(key)

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            object.__setattr__(self, key, value)
        else:
            self._attributes[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._attributes.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._attributes[key] = value

    def __delitem__(self, key: str) -> None:
        self._attributes.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._attributes

    def __iter__(self) -> Iterator[str]:
        return iter(self._attributes)

    def __len__(self) -> int:
        return len(self._attributes)

    def __bool__(self) -> bool:
        return bool(self._attributes)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Fluent):
            return self._attributes == other._attributes
        if isinstance(other, Mapping):
            return self._attributes == dict(other)
        return NotImplemented

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"Fluent({self._attributes!r})"


__all__ = ["Fluent"]
