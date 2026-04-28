"""``Arr`` — Laravel-style array (dict / list) helpers.

Mirrors ``Illuminate\\Support\\Arr`` — every method takes the array
as the first arg and returns a new value rather than mutating in
place. Useful when ``Collection`` is overkill but ``some_dict.get()``
isn't expressive enough.

The dot-notation accessors (``get``, ``set``, ``has``, ``forget``)
are the workhorses — they walk arbitrary nested structures of
``dict``s and ``list``s using ``"a.b.0.c"`` path strings, so call
sites don't have to chain ``.get(..., {}).get(..., {}).get(...)``
with manual None-guards.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union


class Arr:
    """Static array / dict helpers, Laravel-style.

    Methods are pure — never mutate inputs. Dot-notation accessors
    transparently walk dict + list shapes using string indices for
    list positions (``"items.0.name"``).
    """

    # ── Dot-notation accessors ──────────────────────────────────────

    @staticmethod
    def get(target: Any, key: Optional[str], default: Any = None) -> Any:
        """Read ``key`` from a nested dict / list using dot-notation.

        ``key=None`` returns the target unchanged. Missing keys (or
        index out of range) return ``default``. Use ``"*"`` as a
        path segment to map across a list (mirrors Laravel's
        ``data_get``).

        Examples::

            Arr.get({"a": {"b": 1}}, "a.b") == 1
            Arr.get({"items": [{"name": "x"}]}, "items.0.name") == "x"
            Arr.get({"items": [{"name": "x"}, {"name": "y"}]}, "items.*.name")
                == ["x", "y"]
        """
        if key is None:
            return target
        if target is None:
            return default

        segments = str(key).split(".")
        current: Any = target
        for i, segment in enumerate(segments):
            if segment == "*":
                # Wildcard: map the rest of the path across each item.
                if not isinstance(current, (list, tuple)):
                    return default
                rest = ".".join(segments[i + 1:])
                if not rest:
                    return list(current)
                return [Arr.get(item, rest, default) for item in current]
            if isinstance(current, dict):
                if segment not in current:
                    return default
                current = current[segment]
            elif isinstance(current, (list, tuple)):
                try:
                    idx = int(segment)
                except ValueError:
                    return default
                if idx < 0 or idx >= len(current):
                    return default
                current = current[idx]
            else:
                return default
        return current

    @staticmethod
    def set(target: Dict[str, Any], key: str, value: Any) -> Dict[str, Any]:
        """Write ``key`` into a nested dict using dot-notation.

        Creates intermediate dicts as needed. Mutates ``target`` (and
        returns it) — matches Laravel's ``Arr::set`` semantics.
        """
        if not key:
            return target
        segments = str(key).split(".")
        current: Any = target
        for segment in segments[:-1]:
            if not isinstance(current, dict):
                # Path crosses a non-dict node — bail rather than
                # silently overwrite.
                return target
            if segment not in current or not isinstance(current[segment], dict):
                current[segment] = {}
            current = current[segment]
        if isinstance(current, dict):
            current[segments[-1]] = value
        return target

    @staticmethod
    def has(target: Any, key: str) -> bool:
        """Return True if ``key`` resolves to anything (incl. None)."""
        if not key:
            return False
        sentinel = object()
        return Arr.get(target, key, sentinel) is not sentinel

    @staticmethod
    def forget(target: Dict[str, Any], key: str) -> Dict[str, Any]:
        """Remove ``key`` from a nested dict using dot-notation."""
        if not key:
            return target
        segments = str(key).split(".")
        current: Any = target
        for segment in segments[:-1]:
            if not isinstance(current, dict) or segment not in current:
                return target
            current = current[segment]
        if isinstance(current, dict):
            current.pop(segments[-1], None)
        return target

    # ── Slicing / picking ───────────────────────────────────────────

    @staticmethod
    def only(target: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
        """Return a new dict containing only ``keys`` from ``target``."""
        wanted = set(keys)
        return {k: v for k, v in target.items() if k in wanted}

    @staticmethod
    def except_(target: Dict[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
        """Return a new dict with ``keys`` excluded.

        Named ``except_`` because ``except`` is reserved in Python.
        Laravel's ``Arr::except`` equivalent.
        """
        excluded = set(keys)
        return {k: v for k, v in target.items() if k not in excluded}

    @staticmethod
    def pluck(
        items: Iterable[Any],
        value_key: str,
        index_key: Optional[str] = None,
    ) -> Union[List[Any], Dict[Any, Any]]:
        """Extract one field from each row.

        With ``index_key`` returns a dict keyed by that field; without
        it returns a list of values. Mirrors Laravel's ``Arr::pluck``.

        Examples::

            Arr.pluck([{"id": 1, "n": "a"}, {"id": 2, "n": "b"}], "n")
                == ["a", "b"]
            Arr.pluck(rows, "n", index_key="id") == {1: "a", 2: "b"}
        """
        if index_key is None:
            return [Arr.get(row, value_key) for row in items]
        return {Arr.get(row, index_key): Arr.get(row, value_key) for row in items}

    # ── Shape changes ───────────────────────────────────────────────

    @staticmethod
    def first(
        items: Iterable[Any],
        predicate: Optional[Callable[[Any], bool]] = None,
        default: Any = None,
    ) -> Any:
        """Return the first item (matching ``predicate`` if given), else ``default``."""
        for item in items:
            if predicate is None or predicate(item):
                return item
        return default

    @staticmethod
    def last(
        items: Sequence[Any],
        predicate: Optional[Callable[[Any], bool]] = None,
        default: Any = None,
    ) -> Any:
        """Return the last item (matching ``predicate`` if given), else ``default``."""
        if predicate is None:
            return items[-1] if items else default
        for item in reversed(list(items)):
            if predicate(item):
                return item
        return default

    @staticmethod
    def collapse(items: Iterable[Iterable[Any]]) -> List[Any]:
        """Flatten one level — list of lists → flat list."""
        out: List[Any] = []
        for sub in items:
            if isinstance(sub, (list, tuple)):
                out.extend(sub)
            else:
                out.append(sub)
        return out

    @staticmethod
    def flatten(items: Iterable[Any], depth: int = -1) -> List[Any]:
        """Recursively flatten nested lists.

        ``depth=-1`` flattens fully; ``depth=1`` collapses one level
        (equivalent to :meth:`collapse`).
        """
        out: List[Any] = []
        for item in items:
            if isinstance(item, (list, tuple)) and depth != 0:
                out.extend(Arr.flatten(item, depth - 1 if depth > 0 else -1))
            else:
                out.append(item)
        return out

    @staticmethod
    def divide(target: Dict[Any, Any]) -> tuple:
        """Split a dict into ``(keys_list, values_list)`` — Laravel parity."""
        return list(target.keys()), list(target.values())

    @staticmethod
    def wrap(value: Any) -> List[Any]:
        """Coerce ``value`` to a list — None → [], scalar → [scalar],
        list/tuple → list. Mirrors Laravel's ``Arr::wrap``.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        return [value]

    @staticmethod
    def is_assoc(target: Any) -> bool:
        """True if ``target`` is a dict (Laravel "associative array")."""
        return isinstance(target, dict)

    @staticmethod
    def is_list(target: Any) -> bool:
        """True if ``target`` is a list / tuple."""
        return isinstance(target, (list, tuple))


__all__ = ["Arr"]
