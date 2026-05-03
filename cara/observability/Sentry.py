"""Sentry / GlitchTip wiring — generic, idempotent setup.

One import, one call:

    from cara.observability import setup_sentry
    setup_sentry(service_name="my-service")   # idempotent

Reads DSN, traces sample rate, and environment from cara config
(``sentry.dsn``, ``sentry.traces_rate``, ``app.env``) with
``os.environ`` fallback for early-bootstrap calls before
``cara.configuration`` is fully loaded.

``sentry_sdk`` is an OPTIONAL runtime dependency — when the package
isn't installed (or the DSN is empty), ``setup_sentry`` is a no-op
and the rest of the bootstrap continues unaffected.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
from typing import Any, Optional


_setup_done = False
_setup_lock = threading.Lock()


def _env(key: str, default: Any = "") -> str:
    """Read from cara ``config()`` if available, fall back to ``os.environ``.

    During very early bootstrap (before ``ConfigurationProvider.boot()``
    fires) ``config()`` can throw — fall back to the matching uppercase
    env var so this helper is callable from anywhere in the bootstrap
    chain.
    """
    try:
        from cara.configuration import config

        val = config(key)
        if val is not None:
            return str(val)
    except Exception as e:
        # stderr fallback — Log facade may not be booted yet either.
        print(
            f"[cara.observability._env] config({key!r}) failed: "
            f"{e.__class__.__name__}: {e}",
            file=sys.stderr,
        )
    env_key = key.upper().replace(".", "_")
    return os.environ.get(env_key, str(default))


def setup_sentry(
    *,
    service_name: str,
    release: Optional[str] = None,
    git_repo_dir: Optional[str] = None,
) -> None:
    """Initialise Sentry / GlitchTip if a DSN is configured.

    Idempotent — repeat calls after the first are no-ops, so it's
    safe to invoke from multiple bootstrap entry points (HTTP server,
    queue worker, CLI command). The first caller wins and sets the
    ``service_name`` / ``release`` for the process lifetime.

    Args:
        service_name: Logical name reported as ``release`` prefix
            (``f"{service_name}@{release}"``). Required.
        release: Version tag. ``None`` falls back to the git short-SHA
            of ``git_repo_dir`` (or the current working directory),
            then ``"dev"`` if neither is available.
        git_repo_dir: Directory to run ``git rev-parse --short HEAD``
            in for release-tag resolution. Defaults to the current
            working directory; pass the app's repo root for accuracy
            when calling from a deeply-nested module.
    """
    global _setup_done
    with _setup_lock:
        if _setup_done:
            return

        rel = release or _git_short_sha(git_repo_dir) or "dev"
        _try(_init_sentry, service_name, rel)
        _setup_done = True


def _try(fn, *args, **kwargs) -> None:
    """Run a setup step swallowing failures so one broken backend
    cannot prevent the rest of the bootstrap from coming up."""
    try:
        fn(*args, **kwargs)
    except Exception as e:
        try:
            from cara.facades import Log

            Log.warning(
                f"[cara.observability] {fn.__name__} failed: "
                f"{e.__class__.__name__}: {e}",
                category="observability",
            )
        except Exception as log_err:
            # Last-resort stderr — the Log facade itself blew up.
            print(
                f"[cara.observability._try] Log.warning failed after "
                f"{fn.__name__} error ({e}): {log_err}",
                file=sys.stderr,
            )


def _init_sentry(service_name: str, release: str) -> None:
    dsn = _env("sentry.dsn").strip()
    if not dsn:
        return  # disabled — caller didn't configure a DSN

    # ``sentry_sdk`` is an optional dep. Importing inside the helper
    # so projects that don't install it are unaffected at import time.
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.threading import ThreadingIntegration

    sentry_sdk.init(
        dsn=dsn,
        release=f"{service_name}@{release}",
        environment=_env("app.env", "dev"),
        server_name=socket.gethostname(),
        traces_sample_rate=float(_env("sentry.traces_rate", "0.1")),
        integrations=[
            LoggingIntegration(level=None, event_level=None),
            ThreadingIntegration(propagate_hub=True),
        ],
        attach_stacktrace=True,
        send_default_pii=False,
        max_breadcrumbs=50,
    )
    try:
        from cara.facades import Log

        Log.info(
            f"Sentry/GlitchTip enabled (service={service_name}, release={release})"
        )
    except Exception as e:
        print(
            f"[cara.observability] Sentry enabled but Log.info failed: {e}",
            file=sys.stderr,
        )


def set_request_user(user_id: Any, email: Optional[str] = None) -> None:
    """Attach the resolved user to the current Sentry scope.

    Called from the auth middleware after ``request.set_user(...)``;
    every subsequent error in this request inherits the identity so
    Sentry can cluster by user without ops grepping the access log.
    Email is masked to keep PII out of the event store; pass ``None``
    when only the surrogate id is wanted.
    """
    try:
        import sentry_sdk
    except Exception:
        return
    payload: dict = {"id": str(user_id)}
    if email:
        # Mask: ``j****@example.com``. Keeps the first char + domain
        # so on-call can spot which tenant a flood comes from without
        # storing the raw address.
        try:
            local, _, domain = email.partition("@")
            if local and domain:
                head = local[0] if local else ""
                payload["email"] = f"{head}***@{domain}"
        except Exception:
            pass
    try:
        sentry_sdk.set_user(payload)
    except Exception:
        pass


def set_request_tag(key: str, value: Any) -> None:
    """Attach a tag (route, marketplace, request_id, …) to the scope.

    Tags are searchable in the Sentry UI. Skip silently when the SDK
    isn't installed so this stays a zero-cost helper in dev.
    """
    if value is None:
        return
    try:
        import sentry_sdk
        sentry_sdk.set_tag(key, str(value)[:200])
    except Exception:
        pass


def clear_scope() -> None:
    """Reset the per-request/job scope — call between queue ticks so
    a prior job's user/tag context doesn't leak into the next one.
    """
    try:
        import sentry_sdk
        scope = sentry_sdk.Scope.get_isolation_scope()
        scope.set_user(None)
        scope.clear_breadcrumbs()
    except Exception:
        pass


def _git_short_sha(repo_dir: Optional[str] = None) -> Optional[str]:
    """Resolve the current git short-SHA in ``repo_dir`` (or cwd).

    Returns ``None`` when git is unavailable, the directory isn't a
    repo, or the call times out — the caller falls through to a static
    ``"dev"`` release tag.
    """
    try:
        cwd = repo_dir or os.getcwd()
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        return out.decode().strip() or None
    except Exception as e:
        try:
            from cara.facades import Log

            Log.warning(
                f"[cara.observability._git_short_sha] resolve failed, "
                f"falling back to 'dev' release tag: "
                f"{e.__class__.__name__}: {e}",
                category="observability",
            )
        except Exception as log_err:
            print(
                f"[cara.observability._git_short_sha] git rev-parse failed "
                f"({e}) and Log.warning also failed: {log_err}",
                file=sys.stderr,
            )
        return None


__all__ = ["setup_sentry"]
