"""``ViewRenderer`` renders from its engine's configured ``view_paths``.

The live defect: the renderer handed ``render_mail_view`` the factory's
``application`` — the ``View`` factory carries none, so it passed ``None``,
the ``view`` lookup failed and the renderer fell back to ``paths("views")``.
The engine's configured ``view_paths`` (``config("view.paths")``) never
applied: a template that lived only there was not found.
"""

from __future__ import annotations

from types import SimpleNamespace

from cara.view.ViewEngine import ViewEngine
from cara.view.ViewRenderer import ViewRenderer


def _views(root, body: str):
    folder = root / "mail"
    folder.mkdir(parents=True)
    (folder / "greeting.html").write_text(body, encoding="utf-8")
    return root


def test_a_template_only_in_the_configured_paths_renders(tmp_path):
    views = _views(tmp_path / "custom_views", "Hello {{ name }} from {{ app_name }}")
    engine = ViewEngine(view_paths=[str(views)])
    # A factory with no ``application`` attribute, as the ``View`` factory is.
    renderer = ViewRenderer(engine, factory=SimpleNamespace())

    rendered = renderer.render_mail_template("mail.greeting", {"name": "Ada & Co"})

    assert rendered == "Hello Ada &amp; Co from Cara Application"


def test_each_engine_renders_from_its_own_paths(tmp_path):
    first = _views(tmp_path / "first", "first")
    second = _views(tmp_path / "second", "second")

    assert (
        ViewEngine(view_paths=[str(first)])
        .get_renderer()
        .render_mail_template("mail.greeting")
        == "first"
    )
    assert (
        ViewEngine(view_paths=[str(second)])
        .get_renderer()
        .render_mail_template("mail.greeting")
        == "second"
    )
