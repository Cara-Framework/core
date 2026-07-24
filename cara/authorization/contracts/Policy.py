"""Policy contract for authorization.

Policies are duck-typed: a policy declares only the ability methods it supports
(``view``, ``update``, ``delete``, custom verbs, …) and the Gate resolves them
by name. There is therefore no *required* method, so this contract is a plain
base — not an ABC — that documents the two optional lifecycle hooks every
policy may override.
"""

from __future__ import annotations

from typing import Any


class Policy:
    """Documents the optional ``before``/``after`` hooks shared by all policies."""

    def before(self, user: Any, ability: str, *args: Any) -> bool | None:
        """Run before any ability check. Return True/False to short-circuit."""
        return None

    def after(self, user: Any, ability: str, result: bool, *args: Any) -> bool | None:
        """Run after the ability check. Return True/False to override."""
        return None
