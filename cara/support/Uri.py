"""Uri — fluent, immutable URL builder.

Laravel 11's ``Illuminate\\Support\\Uri`` parity. Wraps urllib's
parsing primitives behind a chainable API so query-string
mutations don't have to round-trip through ``urlparse`` /
``urlencode`` by hand::

    url = (
        Uri.of("https://example.com/path?a=1")
        .with_scheme("https")
        .with_host("api.example.com")
        .with_path("/v2/items")
        .with_query({"page": 2, "limit": 50})
        .with_fragment("section-3")
        .to_string()
    )
    # → https://api.example.com/v2/items?page=2&limit=50#section-3

Every ``with_*`` returns a new :class:`Uri` so the original is
never mutated — safe to share across threads. The query-bag
methods (:meth:`with_query`, :meth:`merge_query`,
:meth:`without_query`) accept dicts or strings interchangeably.
"""

from __future__ import annotations

from typing import Iterable, Mapping, Optional, Union
from urllib.parse import (
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urlparse,
    urlunparse,
)


class Uri:
    """Immutable fluent URL builder."""

    __slots__ = ("_scheme", "_user", "_password", "_host", "_port", "_path", "_query", "_fragment")

    def __init__(
        self,
        scheme: str = "",
        user: str = "",
        password: str = "",
        host: str = "",
        port: Optional[int] = None,
        path: str = "",
        query: str = "",
        fragment: str = "",
    ) -> None:
        self._scheme = scheme
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._path = path
        self._query = query
        self._fragment = fragment

    # ── Construction ────────────────────────────────────────────────

    @classmethod
    def of(cls, url: str) -> "Uri":
        """Parse a URL string into a :class:`Uri`."""
        parsed = urlparse(url)
        return cls(
            scheme=parsed.scheme or "",
            user=parsed.username or "",
            password=parsed.password or "",
            host=parsed.hostname or "",
            port=parsed.port,
            path=parsed.path or "",
            query=parsed.query or "",
            fragment=parsed.fragment or "",
        )

    # ── Accessors ──────────────────────────────────────────────────

    def scheme(self) -> str:
        return self._scheme

    def host(self) -> str:
        return self._host

    def port(self) -> Optional[int]:
        return self._port

    def path(self) -> str:
        return self._path

    def fragment(self) -> str:
        return self._fragment

    def user(self) -> str:
        return self._user

    def password(self) -> str:
        return self._password

    def query(self) -> dict:
        """Return query-string as a dict (last value wins on duplicates)."""
        return dict(parse_qsl(self._query, keep_blank_values=True))

    def query_string(self) -> str:
        """Return the raw query component (without leading ``?``)."""
        return self._query

    # ── Mutators (return new Uri) ───────────────────────────────────

    def with_scheme(self, scheme: str) -> "Uri":
        return self._replace(scheme=scheme)

    def with_host(self, host: str) -> "Uri":
        return self._replace(host=host)

    def with_port(self, port: Optional[int]) -> "Uri":
        return self._replace(port=port)

    def with_user(self, user: str, password: str = "") -> "Uri":
        return self._replace(user=user, password=password)

    def with_path(self, path: str) -> "Uri":
        # Normalise so callers don't have to remember leading-slash rules.
        if path and not path.startswith("/"):
            path = "/" + path
        return self._replace(path=path)

    def with_fragment(self, fragment: str) -> "Uri":
        # Strip an accidental leading ``#`` so callers can pass either form.
        return self._replace(fragment=fragment.lstrip("#"))

    def with_query(self, query: Union[str, Mapping[str, object]]) -> "Uri":
        """Replace the query component entirely."""
        return self._replace(query=self._encode_query(query))

    def merge_query(self, query: Mapping[str, object]) -> "Uri":
        """Merge ``query`` over existing params (existing keys overwritten)."""
        merged = dict(parse_qsl(self._query, keep_blank_values=True))
        merged.update({k: str(v) for k, v in query.items()})
        return self._replace(query=urlencode(merged, doseq=True))

    def without_query(self, *keys: str) -> "Uri":
        """Drop ``keys`` from the query string."""
        if not keys:
            return self
        existing = parse_qsl(self._query, keep_blank_values=True)
        kept = [(k, v) for k, v in existing if k not in keys]
        return self._replace(query=urlencode(kept))

    # ── Conversion ──────────────────────────────────────────────────

    def to_string(self) -> str:
        """Render back to a URL string."""
        netloc = self._build_netloc()
        return urlunparse(
            (self._scheme, netloc, self._path, "", self._query, self._fragment)
        )

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"Uri({self.to_string()!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Uri):
            return self.to_string() == other.to_string()
        if isinstance(other, str):
            return self.to_string() == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.to_string())

    # ── Internals ──────────────────────────────────────────────────

    def _replace(self, **changes) -> "Uri":
        # Tiny copy-with-changes helper — avoids ``dataclasses.replace``
        # dependency since we use ``__slots__``.
        return Uri(
            scheme=changes.get("scheme", self._scheme),
            user=changes.get("user", self._user),
            password=changes.get("password", self._password),
            host=changes.get("host", self._host),
            port=changes.get("port", self._port),
            path=changes.get("path", self._path),
            query=changes.get("query", self._query),
            fragment=changes.get("fragment", self._fragment),
        )

    def _build_netloc(self) -> str:
        if not self._host:
            return ""
        netloc = self._host
        if self._port:
            netloc = f"{netloc}:{self._port}"
        if self._user:
            creds = quote(self._user, safe="")
            if self._password:
                creds = f"{creds}:{quote(self._password, safe='')}"
            netloc = f"{creds}@{netloc}"
        return netloc

    @staticmethod
    def _encode_query(query: Union[str, Mapping[str, object], Iterable]) -> str:
        if isinstance(query, str):
            # Strip a leading ``?`` so callers can pass either form
            # without thinking about it.
            return query.lstrip("?")
        if isinstance(query, Mapping):
            return urlencode({k: str(v) for k, v in query.items()}, doseq=True)
        # Assume iterable of (key, value) pairs.
        return urlencode(list(query), doseq=True)


__all__ = ["Uri"]
