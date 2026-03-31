"""
Authorization Response - Response object for authorization inspections.
"""


class AuthorizationResponse:
    """
    Authorization response for inspect() method.
    """

    def __init__(self, allowed: bool, message: str = ""):
        self._allowed = allowed
        self._message = message

    def allowed(self) -> bool:
        """Check if action is allowed."""
        return self._allowed

    def denied(self) -> bool:
        """Check if action is denied."""
        return not self._allowed

    def message(self) -> str:
        """Get response message."""
        return self._message

    def __bool__(self) -> bool:
        """Allow boolean evaluation."""
        return self._allowed

    def __str__(self) -> str:
        """String representation."""
        status = "allowed" if self._allowed else "denied"
        return f"AuthorizationResponse({status}: {self._message})"

    def __repr__(self) -> str:
        """Debug representation."""
        return self.__str__()
