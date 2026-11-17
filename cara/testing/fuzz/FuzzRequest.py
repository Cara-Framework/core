"""One generated HTTP request, already at the wire's byte level."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FuzzRequest:
    """A request a fuzz strategy produced; the product's transport sends it.

    ``path`` is percent-encoded and carries the spec's own prefix (``/api/...``);
    ``query`` and ``body`` are raw bytes so a strategy can produce what no
    well-behaved client would (``%zz``, a lone surrogate, a truncated JSON).
    """

    method: str
    path: str
    query: bytes = b""
    body: bytes = b""
    content_type: str | None = "application/json"
    headers: dict[str, str] = field(default_factory=dict)
    note: str = ""

    def url(self, *, strip_prefix: str = "") -> str:
        """``path?query`` with ``strip_prefix`` removed from the path."""
        path = self.path.removeprefix(strip_prefix) if strip_prefix else self.path
        query = self.query.decode("latin-1")
        return f"{path}?{query}" if query else path

    def send_headers(self) -> dict[str, str]:
        """The headers to send, the content type included when there is one."""
        headers = dict(self.headers)
        if self.content_type is not None and self.method != "GET":
            headers.setdefault("Content-Type", self.content_type)
        return headers

    def describe(self) -> str:
        """A bounded, printable reproduction line for a failure message."""
        query = self.query.decode("latin-1")[:600]
        body = self.body.decode("utf-8", "backslashreplace")[:900]
        return (
            f"{self.method} {self.path[:300]}?{query} ctype={self.content_type} "
            f"headers={self.headers} body={body} note={self.note}"
        )
