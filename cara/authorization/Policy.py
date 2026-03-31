"""
Policy - Base policy class for authorization policies.
"""

from typing import Any, Optional

from cara.authorization.contracts import Policy


class Policy(Policy):
    """
    Base policy class that provides common functionality for all policies.
    """

    def __init__(self):
        """
        Initialize the base policy.
        """
        pass

    def before(self, user: Any, ability: str, *args) -> Optional[bool]:
        """
        Perform pre-authorization checks.
        This method runs before any ability checks.
        Return True to allow, False to deny, or None to continue to ability check.
        """
        # No default authorization logic - this should be defined in app code
        return None

    def after(self, user: Any, ability: str, result: bool, *args) -> Optional[bool]:
        """
        Perform post-authorization checks.
        This method runs after the ability check is complete.
        Return True to allow, False to deny, or None to keep original result.
        """
        return None

    def create(self, user: Any) -> bool:
        """
        Determine whether the user can create models.
        """
        return False

    def update(self, user: Any, model: Any) -> bool:
        """
        Determine whether the user can update the model.
        """
        return False

    def delete(self, user: Any, model: Any) -> bool:
        """
        Determine whether the user can delete the model.
        """
        return False

    def restore(self, user: Any, model: Any) -> bool:
        """
        Determine whether the user can restore the model.
        """
        return False

    def force_delete(self, user: Any, model: Any) -> bool:
        """
        Determine whether the user can permanently delete the model.
        """
        return False
