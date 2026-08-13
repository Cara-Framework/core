"""Explicit, idempotent Sentry/GlitchTip process wiring."""

from __future__ import annotations

import math
import os
import socket
import subprocess
import threading
from numbers import Real
from typing import Any

from cara.facades import Log

_setup_done = False
_setup_lock = threading.Lock()


def setup_sentry(
    *,
    service_name: str,
    dsn: str,
    environment: str,
    traces_rate: float,
    release: str | None = None,
    git_repo_dir: str | None = None,
) -> None:
    """Configure Sentry from composition-root values.

    An empty DSN explicitly disables Sentry. Once a DSN is configured,
    dependency or SDK setup failures propagate and block boot; a configured
    error pipeline must never disappear behind a successful startup.
    """
    name = _required_text(service_name, "service_name")
    target_environment = _required_text(environment, "environment")
    if not isinstance(dsn, str):
        raise TypeError("Sentry dsn must be a string.")
    target_dsn = dsn.strip()
    rate = _sample_rate(traces_rate)
    if release is not None:
        release = _required_text(release, "release")
    if git_repo_dir is not None:
        git_repo_dir = _required_text(git_repo_dir, "git_repo_dir")

    global _setup_done
    with _setup_lock:
        if _setup_done:
            return
        if target_dsn:
            resolved_release = release or _git_short_sha(git_repo_dir) or "dev"
            _init_sentry(
                service_name=name,
                dsn=target_dsn,
                environment=target_environment,
                traces_rate=rate,
                release=resolved_release,
            )
        _setup_done = True


def _init_sentry(
    *,
    service_name: str,
    dsn: str,
    environment: str,
    traces_rate: float,
    release: str,
) -> None:
    import sentry_sdk  # local: heavy optional dep
    from sentry_sdk.integrations.logging import (
        LoggingIntegration,  # local: heavy optional dep
    )
    from sentry_sdk.integrations.threading import (
        ThreadingIntegration,  # local: heavy optional dep
    )

    sentry_sdk.init(
        dsn=dsn,
        release=f"{service_name}@{release}",
        environment=environment,
        server_name=socket.gethostname(),
        traces_sample_rate=traces_rate,
        integrations=[
            LoggingIntegration(level=None, event_level=None),
            ThreadingIntegration(propagate_hub=True),
        ],
        attach_stacktrace=True,
        send_default_pii=False,
        max_breadcrumbs=50,
    )


def set_request_user(user_id: Any, email: str | None = None) -> None:
    """Attach a masked request identity when the optional SDK is installed."""
    try:
        import sentry_sdk  # local: heavy optional dep
    except ImportError:
        return

    payload: dict[str, str] = {"id": str(user_id)}
    if email is not None:
        if not isinstance(email, str):
            raise TypeError("Sentry request email must be a string or None.")
        local, separator, domain = email.partition("@")
        if separator and local and domain:
            payload["email"] = f"{local[0]}***@{domain}"
    try:
        sentry_sdk.set_user(payload)
    except (OSError, RuntimeError, AttributeError) as exc:
        _report_scope_failure("set_user", exc)


def set_request_tag(key: str, value: Any) -> None:
    """Attach one bounded tag when the optional SDK is installed."""
    if value is None:
        return
    tag = _required_text(key, "tag key")
    try:
        import sentry_sdk  # local: heavy optional dep
    except ImportError:
        return
    try:
        sentry_sdk.set_tag(tag, str(value)[:200])
    except (OSError, RuntimeError, AttributeError) as exc:
        _report_scope_failure("set_tag", exc)


def clear_scope() -> None:
    """Clear request/job Sentry state when the optional SDK is installed."""
    try:
        import sentry_sdk  # local: heavy optional dep
    except ImportError:
        return
    try:
        scope = sentry_sdk.Scope.get_isolation_scope()
        scope.set_user(None)
        scope.clear_breadcrumbs()
    except (OSError, RuntimeError, AttributeError) as exc:
        _report_scope_failure("clear_scope", exc)


def _report_scope_failure(operation: str, error: Exception) -> None:
    Log.warning(
        "Sentry %s failed: %s",
        operation,
        error,
        category="observability.sentry",
    )


def _git_short_sha(repo_dir: str | None = None) -> str | None:
    """Resolve a release hint; missing git intentionally falls back to dev."""
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_dir or os.getcwd(),
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        return output.decode().strip() or None
    except OSError, subprocess.SubprocessError, UnicodeError:
        return None


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Sentry {field} must be a non-empty string.")
    return value.strip()


def _sample_rate(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError("Sentry traces_rate must be a real number.")
    rate = float(value)
    if not math.isfinite(rate) or not 0.0 <= rate <= 1.0:
        raise ValueError("Sentry traces_rate must be between 0.0 and 1.0.")
    return rate


__all__ = ["clear_scope", "set_request_tag", "set_request_user", "setup_sentry"]
