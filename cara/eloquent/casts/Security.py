"""
Security Cast Types for Cara ORM.

Provides hashing and encryption capabilities for sensitive data.
Encryption delegates to the ``Crypt`` facade (versioned AES-256-GCM) registered in the
container by :class:`EncryptionProvider`.
"""

from __future__ import annotations
