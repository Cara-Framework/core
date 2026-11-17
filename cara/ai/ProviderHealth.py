"""Whether an AI provider is answering — one stamp every process reads.

A revoked or out-of-credit key fails EVERY call, whichever model is asked:
rotating through the model list burns each queued job against the same
refusal, and the process that queues AI work has no way to know. So the
process that called the provider stamps a failure that says nothing about
the request itself in the shared cache, per provider:

* ``auth`` — the key or account was refused (401/402/403, or AI is not
  configured), held :data:`AUTH_HOLD_SECONDS`: nothing fixes it but an
  operator, and the door re-opens on its own after the hold so the next call
  proves whether it was fixed;
* ``outage`` — the provider was down or timing out (5xx, timeout), held
  :data:`OUTAGE_HOLD_SECONDS`.

A 400/404/429 is about the request, the model name or a rate window, not
the provider's health, and writes nothing. A successful call clears the
stamp. Best effort by construction: a cache that cannot be read or written
never breaks the caller — the door then behaves as before (open).
"""

from __future__ import annotations

from typing import Any, Literal

from cara.facades import Cache, Log

from .AIConfigurationError import AIConfigurationError

ProviderFailure = Literal["auth", "outage"]

AUTH_HOLD_SECONDS = 60 * 60
OUTAGE_HOLD_SECONDS = 10 * 60

_AUTH_STATUSES = frozenset({401, 402, 403})
_TIMEOUT_NAMES = frozenset(
    {"ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout", "TimeoutError"}
)


def _key(provider: str) -> str:
    return f"ai:provider:{str(provider or 'default').strip().lower()}:unhealthy"


def provider_failure_kind(
    *, status: int | None = None, error: Any = None
) -> ProviderFailure | None:
    """``auth`` / ``outage`` when the failure is the provider's, else ``None``."""
    if isinstance(error, AIConfigurationError):
        return "auth"
    if isinstance(status, int) and not isinstance(status, bool):
        if status in _AUTH_STATUSES:
            return "auth"
        if status >= 500:
            return "outage"
        return None
    if error is not None and (
        type(error).__name__ in _TIMEOUT_NAMES
        or isinstance(error, (TimeoutError, ConnectionError))
    ):
        return "outage"
    return None


def record_provider_failure(
    provider: str, *, status: int | None = None, error: Any = None
) -> ProviderFailure | None:
    """Stamp ``provider`` unhealthy when the failure is its fault.

    Returns the kind stamped, or ``None`` when nothing was written.
    """
    kind = provider_failure_kind(status=status, error=error)
    if kind is None:
        return None
    try:
        Cache.put(
            _key(provider),
            kind,
            AUTH_HOLD_SECONDS if kind == "auth" else OUTAGE_HOLD_SECONDS,
        )
    except Exception as exc:  # no cache: the next call finds the outage itself
        Log.warning(
            "AI provider health stamp not written: %s",
            type(exc).__name__,
            category="cara.ai",
        )
        return None
    return kind


def record_provider_success(provider: str) -> None:
    """A call went through: ``provider`` is healthy again."""
    try:
        Cache.forget(_key(provider))
    except Exception as exc:  # the stamp expires on its own hold
        Log.warning(
            "AI provider health stamp not cleared: %s",
            type(exc).__name__,
            category="cara.ai",
        )


def provider_unhealthy(provider: str) -> ProviderFailure | None:
    """The stamp a recent call left for ``provider`` (``None`` = healthy)."""
    try:
        raw = Cache.get(_key(provider))
    except Exception:
        return None
    return raw if raw in ("auth", "outage") else None


__all__ = [
    "AUTH_HOLD_SECONDS",
    "OUTAGE_HOLD_SECONDS",
    "ProviderFailure",
    "provider_failure_kind",
    "provider_unhealthy",
    "record_provider_failure",
    "record_provider_success",
]
