"""
Authenticatable Interface for users.
"""

from __future__ import annotations


class Authenticatable:
    """Strict identity and revocation contract for authenticated models."""

    def get_auth_id(self) -> str | int:
        """Return the stable subject identifier written into credentials."""
        raise NotImplementedError("Authenticatable models must implement get_auth_id()")

    def get_auth_version(self) -> int:
        """Return the user's token-revocation epoch.

        JWT authentication requires an explicit, persisted version. A default
        epoch would keep old tokens valid when a model forgot to implement the
        revocation contract.
        """
        raise NotImplementedError(
            "Authenticatable models must implement get_auth_version()"
        )
