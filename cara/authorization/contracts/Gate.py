"""Gate contract — the public authorization interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from cara.authorization.AuthorizationResponse import AuthorizationResponse


class Gate(ABC):
    """Contract for managing authorization checks."""

    # -- configuration ----------------------------------------------------- #

    @abstractmethod
    def define(self, ability: str, callback: Callable | str) -> Gate:
        """Define a standalone ability."""

    @abstractmethod
    def policy(self, model_class: Any, policy_class: Any) -> Gate:
        """Bind a policy class to a model class."""

    @abstractmethod
    def register_policies(self, policies: list[tuple]) -> Gate:
        """Bind multiple ``(Model, Policy)`` tuples."""

    @abstractmethod
    def before(self, callback: Callable) -> Gate:
        """Register a pre-check callback that may short-circuit."""

    @abstractmethod
    def after(self, callback: Callable) -> Gate:
        """Register a post-check callback that may override the result."""

    # -- checks ------------------------------------------------------------ #

    @abstractmethod
    def allows(self, ability: str, *args: Any) -> bool:
        """Whether the current user may perform the ability."""

    @abstractmethod
    def denies(self, ability: str, *args: Any) -> bool:
        """Whether the current user may not perform the ability."""

    @abstractmethod
    def any(self, abilities: list[str], *args: Any) -> bool:
        """Whether the user has any of the abilities."""

    @abstractmethod
    def none(self, abilities: list[str], *args: Any) -> bool:
        """Whether the user has none of the abilities."""

    @abstractmethod
    def inspect(self, ability: str, *args: Any) -> AuthorizationResponse:
        """Return the full authorization response, including any message."""

    @abstractmethod
    def authorize(self, ability: str, *args: Any) -> AuthorizationResponse:
        """Authorize the ability or raise ``AuthorizationFailedException``."""

    @abstractmethod
    def for_user(self, user: Any) -> Gate:
        """Return a gate scoped to the given user."""
