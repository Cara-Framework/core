"""
Policy interface for authorization.
"""

from abc import ABC
from typing import Any


class Policy(ABC):
    """
    Policy contract interface.
    All policies must implement the methods they support.
    """

    def before(self, user: Any, ability: str, *args) -> bool | None:
        """
        Perform pre-authorization checks.
        """
        return None

    def after(self, user: Any, ability: str, result: bool, *args) -> bool | None:
        """
        Perform post-authorization checks.
        """
        return None
