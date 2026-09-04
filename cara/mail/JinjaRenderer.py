"""Jinja2-backed renderer for mail templates.

Jinja2 is the only template engine cara ships, and the notification /
mail templates under ``resources/views/mail`` are the only templates it
renders. They are authored in standard Jinja2 — filters (``| default``,
``| format``, ``| int``, ``| float``, ``| length``), ``is defined``
tests, and ``{% for %}`` loops with slicing.

Historically cara carried its own ``@``-directive view compiler which
evaluated ``{{ ... }}`` as raw Python and implemented none of those, so
mail templates raised at render time (``unsupported operand type(s) for
|``, ``name 'length' is not defined`` …) and the emails never rendered.
That compiler is gone; this module is the whole rendering path.
"""

from __future__ import annotations

from typing import Any

import jinja2

from cara.support import paths

# Cache one Environment per resolved search-path tuple. Building a
# FileSystemLoader + parsing templates is non-trivial; mail send paths
# (and the queue worker) render the same handful of templates repeatedly.
_ENV_CACHE: dict[tuple[str, ...], jinja2.Environment] = {}


def _view_dirs(application) -> list[str]:
    """Resolve template search dir(s), preferring the bound view service.

    Falls back to ``paths("views")`` so rendering still works in minimal
    boots (CLI, queue worker) where the view service may be lazy.
    """
    dirs: list[str] = []
    try:
        service = application.make("view")
        engine = getattr(service, "engine", service)
        dirs = list(getattr(engine, "view_paths", []) or [])
    except Exception:
        dirs = []
    if not dirs:
        try:
            dirs = [paths("views")]
        except Exception:
            dirs = []
    return [d for d in dirs if d]


def _environment(application) -> jinja2.Environment:
    key = tuple(_view_dirs(application))
    env = _ENV_CACHE.get(key)
    if env is None:
        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(list(key)),
            # HTML mail: autoescape so user-supplied text ("Sons & Co",
            # "<3 promos") can't inject markup — mirrors the legacy engine's
            # always-escape policy.
            autoescape=jinja2.select_autoescape(["html", "htm", "xml"]),
            # Tolerate a missing optional var inside an attribute chain
            # (``record.image_url`` when ``record`` is absent) instead of
            # raising; templates still guard with ``| default`` / ``{% if %}``.
            undefined=jinja2.ChainableUndefined,
        )
        _ENV_CACHE[key] = env
    return env


def render_mail_view(application, view: str, data: dict[str, Any] | None = None) -> str:
    """Render a dotted mail view (e.g. ``mail.notifications.price_drop``).

    Raises ``jinja2.TemplateNotFound`` when no template file matches — mail
    rendering fails loud rather than shipping an empty body.
    """
    env = _environment(application)
    name = view.replace(".", "/")
    last: jinja2.TemplateNotFound | None = None
    for ext in (".html", ".htm", ".jinja", ".j2"):
        try:
            template = env.get_template(f"{name}{ext}")
        except jinja2.TemplateNotFound as exc:
            last = exc
            continue
        return template.render(**(data or {}))
    raise last or jinja2.TemplateNotFound(view)


def clear_cache() -> None:
    """Drop cached Environments (used by hot-reload / tests)."""
    _ENV_CACHE.clear()
