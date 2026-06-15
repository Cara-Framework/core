"""Base policy class for authorization policies.

Subclass this and override the abilities you support. Every ability may return
a ``bool`` or an :class:`AuthorizationResponse` (to attach a denial message).
Unoverridden abilities deny by default — policies fail closed.
"""

from __future__ import annotations

from typing import Any

from cara.authorization.AuthorizationResponse import AuthorizationResponse
from cara.authorization.contracts import Policy as PolicyContract

# What an ability method or hook may return.
PolicyResult = bool | AuthorizationResponse | None


class Policy(PolicyContract):
    """Common functionality and safe defaults for all policies."""

    def before(self, user: Any, ability: str, *args: Any) -> PolicyResult:
        """Run before any ability check.

        Return ``True`` to allow, ``False`` to deny, or ``None`` to defer to the
        ability method. Override for role bypasses (e.g. super-admins).
        """
        return None

    def after(self, user: Any, ability: str, result: bool, *args: Any) -> PolicyResult:
        """Run after the ability check.

        Return ``True``/``False`` to override, or ``None`` to keep the result.
        """
        return None

    # Standard CRUD abilities — deny by default. ------------------------- #

    def view_any(self, user: Any, model: Any = None) -> PolicyResult:
        """Whether the user can list models."""
        return False

    def view(self, user: Any, model: Any = None) -> PolicyResult:
        """Whether the user can view the model."""
        return False

    def create(self, user: Any, model: Any = None) -> PolicyResult:
        """Whether the user can create models."""
        return False

    def update(self, user: Any, model: Any) -> PolicyResult:
        """Whether the user can update the model."""
        return False

    def delete(self, user: Any, model: Any) -> PolicyResult:
        """Whether the user can delete the model."""
        return False

    def restore(self, user: Any, model: Any) -> PolicyResult:
        """Whether the user can restore the soft-deleted model."""
        return False

    def force_delete(self, user: Any, model: Any) -> PolicyResult:
        """Whether the user can permanently delete the model."""
        return False
