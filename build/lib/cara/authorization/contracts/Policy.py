"""
Policy interface for authorization.
"""

from abc import ABC
from typing import Any, Optional


class Policy(ABC):
    """
    Policy contract interface.
    All policies must implement the methods they support.
    """

    def before(self, user: Any, ability: str, *args) -> Optional[bool]:
        """
        Perform pre-authorization checks.
        """
        return None

    def after(self, user: Any, ability: str, result: bool, *args) -> Optional[bool]:
        """
        Perform post-authorization checks.
        """
        return None
