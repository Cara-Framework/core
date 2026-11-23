"""``render_mail_view`` escapes every template whose output is HTML.

Autoescape used to key on the last suffix (``html``/``htm``/``xml``), yet
``.jinja`` / ``.j2`` templates are loaded as the HTML mail body too — so a
value in those mails reached the recipient unescaped (stored XSS in mail).
Escaping follows the real output type: ``name.txt.j2`` stays plain text.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cara.mail import clear_cache, render_mail_view

_HOSTILE = '<a href="https://evil.example">Reset</a> & Co'
_ESCAPED = "&lt;a href=&#34;https://evil.example&#34;&gt;Reset&lt;/a&gt; &amp; Co"


@pytest.fixture
def app(tmp_path):
    (tmp_path / "mail").mkdir()
    clear_cache()
    yield SimpleNamespace(
        make=lambda _key: SimpleNamespace(
            engine=SimpleNamespace(view_paths=[str(tmp_path)])
        ),
        root=tmp_path,
    )
    clear_cache()


def _template(app, filename: str, source: str) -> None:
    (app.root / "mail" / filename).write_text(source, encoding="utf-8")


@pytest.mark.parametrize("ext", [".html", ".htm", ".jinja", ".j2"])
def test_every_html_mail_body_is_escaped(app, ext) -> None:
    _template(app, f"body{ext}", "<p>{{ name }}</p>")
    assert render_mail_view(app, "mail.body", {"name": _HOSTILE}) == f"<p>{_ESCAPED}</p>"


def test_a_plain_text_include_is_not_escaped_but_its_html_host_is(app) -> None:
    _template(app, "plain.txt.j2", "{{ name }}")
    _template(app, "wrapped.html.j2", "{{ name }}")
    _template(
        app,
        "host.j2",
        "<p>{{ name }}</p>|{% include 'mail/plain.txt.j2' %}|"
        "{% include 'mail/wrapped.html.j2' %}",
    )
    rendered = render_mail_view(app, "mail.host", {"name": _HOSTILE})
    assert rendered == f"<p>{_ESCAPED}</p>|{_HOSTILE}|{_ESCAPED}"
