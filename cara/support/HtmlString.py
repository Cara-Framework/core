"""HtmlString — marker for already-escaped HTML strings.

Laravel's ``Illuminate\\Support\\HtmlString`` parity. Wrapping a
string in ``HtmlString(s)`` signals to the rendering layer
("trust me, this is safe HTML, don't escape it again")::

    safe = HtmlString("<strong>bold</strong>")
    e(safe)              # passes through unchanged
    e("<x>")             # escaped to "&lt;x&gt;"

The :func:`e` helper at :mod:`cara.helpers` honours this marker —
if the value is an :class:`HtmlString` it returns the underlying
string verbatim, otherwise it HTML-escapes via ``html.escape``.

This is the canonical way to build trusted snippets (rendered
markdown, partials from a template) without re-escaping them at
the outer layer.
"""

from __future__ import annotations


class HtmlString:
    """Wrap a string that should NOT be HTML-escaped on render."""

    __slots__ = ("_html",)

    def __init__(self, html: str = "") -> None:
        self._html = "" if html is None else str(html)

    def to_html(self) -> str:
        """Return the underlying HTML string verbatim."""
        return self._html

    def is_empty(self) -> bool:
        return self._html == ""

    def is_not_empty(self) -> bool:
        return self._html != ""

    def __str__(self) -> str:
        return self._html

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"HtmlString({self._html!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, HtmlString):
            return self._html == other._html
        return self._html == other

    def __hash__(self) -> int:
        return hash(self._html)

    def __bool__(self) -> bool:
        return bool(self._html)

    def __len__(self) -> int:
        return len(self._html)


__all__ = ["HtmlString"]
