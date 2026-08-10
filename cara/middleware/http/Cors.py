"""The CORS policy — one source for every site that stamps CORS headers.

Three places decide whether a response gets ``Access-Control-Allow-Origin``:
``HandleCors`` on the success path, ``apply_cors_headers_to_response`` for
middleware that short-circuits before ``HandleCors`` runs, and
``DefaultExceptionHandler._cors_headers_for_scope`` for raised exceptions.
The exception path used to RESTATE the policy instead of reading it, and
the copy had drifted OPEN in two ways:

* It never consulted ``cors.cors.paths``. With the shipped default
  ``["api/*"]``, a request to a deliberately-non-CORS route (``/admin/...``,
  ``/internal/metrics``) got no ``Access-Control-Allow-Origin`` when it
  SUCCEEDED but ``Access-Control-Allow-Origin: *`` on its 401/403/404/500.
  An attacker's page could therefore ``fetch()`` those endpoints
  cross-origin and read status and body — a working existence-and-state
  oracle over exactly the routes the operator excluded from CORS.
* Any failure reading configuration fell back to a wildcard origin. The
  case where we know least about the policy granted the most, which is the
  inverse of §9's fail-closed rule.

A hand-copied predicate does not stay wrong loudly; it drifts silently.
So the predicate lives here, takes a plain path string (callable without a
``Request``), and is IMPORTED at every use.

Framework defaults are declared once, in :data:`CORS_DEFAULTS`, and every
site now READS them: ``HandleCors._load_config`` calls
:func:`load_cors_policy`, ``HandleCors._path_matches_cors_config`` calls
:func:`path_in_cors_scope`, and both ``HandleCors._add_cors_headers`` and the
exception handler call :func:`resolve_allow_origin`. There was a window in
which the middleware still carried its own key list and an AST guard test
compared the two copies for equality; that guard is gone with the copy.
"""

from __future__ import annotations

import fnmatch
import re
from typing import Any

#: Every ``cors.cors.*`` key the framework understands, with the default it
#: falls back to when the product ships no ``config/cors.py`` entry.
#:
#: ``allowed_origins`` defaults to ``["*"]``, which is what ``HandleCors``
#: has always shipped. It is a permissive default and tightening it to ``[]``
#: is worth doing — and can now be done in ONE place, which is the whole
#: point of this module: the success path and the error path can no longer
#: disagree about what "unconfigured" means.
CORS_DEFAULTS: dict[str, Any] = {
    "paths": ["api/*"],
    "allowed_methods": ["*"],
    "allowed_origins": ["*"],
    "allowed_origins_patterns": [],
    "allowed_headers": ["*"],
    "exposed_headers": [],
    "max_age": 0,
    "supports_credentials": False,
}


def load_cors_policy() -> dict[str, Any]:
    """Read the whole CORS policy from configuration.

    Raises whatever the configuration subsystem raises. Callers on an error
    path MUST translate a failure into "emit no CORS headers" rather than
    into a wildcard: a policy we could not read is not a policy that allows
    everyone.
    """
    from cara.configuration import config

    return {
        key: config(f"cors.cors.{key}", default) for key, default in CORS_DEFAULTS.items()
    }


def path_in_cors_scope(path: str, paths: Any) -> bool:
    """Whether ``path`` is inside the configured ``cors.cors.paths`` scope.

    Supports simple glob patterns like ``api/*``. An empty/absent list means
    "apply to all", which is the framework's historical default and what a
    product that never configured ``paths`` relies on.
    """
    if not paths:
        return True

    candidate = str(path or "").lstrip("/")
    return any(fnmatch.fnmatch(candidate, pattern) for pattern in paths)


def origin_explicitly_allowed(origin: str, policy: dict[str, Any]) -> bool:
    """Whether ``origin`` matches a NON-WILDCARD allowlist entry.

    The credentials path needs this: the browser refuses to send cookies to
    a wildcard, and reflecting an arbitrary ``Origin`` alongside
    ``Access-Control-Allow-Credentials: true`` is the textbook CSRF
    primitive.
    """
    if not origin:
        return False
    if origin in (policy.get("allowed_origins") or []):
        return True
    return any(
        re.match(pattern, origin)
        for pattern in (policy.get("allowed_origins_patterns") or [])
    )


def resolve_allow_origin(origin: str, policy: dict[str, Any]) -> str | None:
    """The value for ``Access-Control-Allow-Origin``, or ``None`` to omit it.

    ``None`` is the fail-closed answer and is returned whenever credentials
    are enabled without an explicit allowlist match — including the
    wildcard-plus-credentials misconfiguration, which is treated as "no
    origin allowed" rather than reflected.
    """
    if policy.get("supports_credentials"):
        if origin and origin_explicitly_allowed(origin, policy):
            return origin
        return None

    if "*" in (policy.get("allowed_origins") or []):
        return "*"
    if origin and origin_explicitly_allowed(origin, policy):
        return origin
    return None


__all__ = [
    "CORS_DEFAULTS",
    "load_cors_policy",
    "origin_explicitly_allowed",
    "path_in_cors_scope",
    "resolve_allow_origin",
]
