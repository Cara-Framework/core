"""
HTTP Request Object for the Cara framework.

This module provides the Request class, encapsulating HTTP request data and utility methods for
request handling.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs

from cara.exceptions import BadRequestException
from cara.http.request.Context import current_request
from cara.http.request.HeaderBag import HeaderBag
from cara.http.request.InputBag import InputBag
from cara.http.request.mixins import (
    MakesBodyParsing,
    MakesRequestHelpers,
    MakesValidationHelpers,
)
from cara.http.request.UploadedFile import UploadedFile
from cara.security import is_trusted_proxy


def _is_trusted_proxy(addr: str) -> bool:
    """True if ``addr`` is inside any configured trusted-proxy network.

    The rules (loopback auto-trusted, RFC1918 never, ``TRUSTED_PROXIES``
    read from ``app.trusted_proxies``) now live in
    :mod:`cara.security.TrustedProxies`, which is also what decides whether
    ``X-Forwarded-Proto`` may prove HTTPS. Keeping one resolver is the point:
    while this module had its own, ``SecurityHeaders`` had a second one reading
    a config key no product defines, so the framework trusted the operator's
    proxies for ``request.ip()`` and nobody at all for HSTS.

    The parse was previously ``lru_cache``d here with no key, which froze the
    boundary at whatever the first request observed; the shared resolver caches
    on the raw config value instead, so a reload still takes effect.
    """

    return is_trusted_proxy(addr)


#: Text no database or wire encoding can carry: a NUL (PostgreSQL text refuses
#: it) and a lone UTF-16 surrogate (JSON's ``\ud83d`` decodes to one, and it
#: has no UTF-8 form, so the driver cannot even encode the parameter).
_UNSTORABLE_TEXT = re.compile("[\\x00\\ud800-\\udfff]")

_UNSTORABLE_TEXT_MESSAGE = (
    "Text in this request contains a NUL character or a broken Unicode "
    "sequence, which can't be stored."
)


def _strings_in(value: Any) -> Iterator[str]:
    """Every string in a decoded payload, mapping keys included.

    Iterative on purpose: the JSON scanner accepts nesting deeper than Python's
    recursion limit, so a recursive walk would turn that body into a 500.
    """
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


class Request(MakesBodyParsing, MakesValidationHelpers, MakesRequestHelpers):
    """
    HTTP Request object for ASGI‐based APIs.

    Handles parsing of headers, query params, JSON body, form data, file uploads, integrates with
    the Validation component for input validation, and offers convenience methods like
    only()/except_()/has()/filled().
    """

    def __init__(self, application: Any) -> None:
        self.application = application
        self.scope: dict[str, Any] = {}
        self.receive: Any = None

        self.headers = HeaderBag()
        self._input = InputBag()
        self.params: dict[str, Any] = {}
        self.route = None

        self._user: Any = None
        self._ip: str | None = None
        self._body: bytes | None = None
        self._body_consumed = False
        self._validation_instance: Any = None

        self._query_params: dict[str, list[str]] | None = None
        self._form_params: dict[str, Any] | None = None
        self._json_data: dict[str, Any] | None = None
        self._files: dict[str, UploadedFile] | None = None
        self._form_refused = False

        self.validated: dict[str, Any] = {}
        self._request_id = str(uuid.uuid4())

        current_request.set(self)

    def app(self):
        """Return the application instance (Laravel-style convenience)."""
        return self.application

    def load(
        self,
        scope: dict[str, Any] = None,
        receive: Any = None,
    ) -> Request:
        """
        Initialize request data from ASGI scope and receive function.

        Parses headers and query parameters immediately.
        """
        self.scope = scope or {}
        self.receive = receive

        # HTTP header values are latin-1 on the wire (RFC 9110 obs-text) —
        # strict UTF-8 decoding raised UnicodeDecodeError outside any try
        # for any client sending a high byte, failing the request before
        # routing even started.
        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in self.scope.get("headers", [])
        }
        self.headers.load(headers)

        raw_qs = self.scope.get("query_string", b"").decode("utf-8", errors="replace")
        if raw_qs:
            self._input.load_query_string(raw_qs)

        return self

    # -----------------------
    # Basic Request Properties
    # -----------------------

    @property
    def method(self) -> str:
        """Return uppercase HTTP method (e.g., GET, POST)."""
        return self.scope.get("method", "GET").upper()

    @property
    def path(self) -> str:
        """Return request path."""
        return self.scope.get("path", "/")

    def header(self, name: str, default: Any = None) -> str | None:
        """
        Retrieve a header value (case‐insensitive).

        Returns default if missing.
        """
        value = self.headers.get(name)
        return value if value is not None else default

    def get_host(self) -> str:
        """Return the Host header value."""
        return self.header("host", "")

    def ip(self) -> str | None:
        """Return client IP address.

        X-Forwarded-For is only honored if the immediate peer is a trusted
        proxy (TRUSTED_PROXIES env var, comma-separated CIDRs or IPs). This
        prevents clients from spoofing arbitrary IPs to bypass per-IP rate
        limits / audit logs. Falls back to the ASGI client tuple otherwise.
        """
        if self._ip:
            return self._ip

        client = self.scope.get("client")
        peer_ip = client[0] if client else None

        if peer_ip and _is_trusted_proxy(peer_ip):
            forwarded = self.header("x-forwarded-for")
            if forwarded:
                # Rightmost entry added by the trusted proxy; walk left while
                # the hop is itself a trusted proxy, then take the first
                # untrusted address — that's the real client.
                hops = [h.strip() for h in forwarded.split(",") if h.strip()]
                for candidate in reversed(hops):
                    if not _is_trusted_proxy(candidate):
                        self._ip = candidate
                        return self._ip
                # All hops trusted — first entry is as good as any.
                if hops:
                    self._ip = hops[0]
                    return self._ip

        self._ip = peer_ip
        return self._ip

    @property
    def query_params(self) -> dict[str, list[str]]:
        """Return parsed query parameters as a dict of lists."""
        if self._query_params is None:
            raw_qs = self.scope.get("query_string", b"").decode()
            self._query_params = parse_qs(raw_qs)
        return self._query_params

    def get_query_param(self, key: str, default: Any = None) -> str | None:
        """Return first value for given query parameter, or default if missing."""
        values = self.query_params.get(key, [default])
        return values[0] if values else default

    # -----------------------
    # Input Access
    # -----------------------

    async def input(self, name: str, default: Any = "") -> Any:
        """Return single input value from any source (JSON, form, query)."""
        data = await self.all()
        return data.get(name, default)

    def param(self, name: str, default: Any = "") -> Any:
        """
        Retrieve a named route parameter.

        Returns default if not set.
        """
        return self.params.get(name, default)

    def load_params(self, params: dict[str, Any] = None) -> Request:
        """Load route parameters after routing was matched.

        This is also where a path or query holding unstorable text is refused
        with a 400, before any route middleware or handler can bind it to SQL.
        Not in :meth:`load`: that runs ahead of the conductor's ``try``, so a
        raise there would skip the global pipeline (no request-log row) and
        the terminable middleware that resets per-worker auth state.
        """
        self._refuse_unstorable_text([self.path, self._input.all()])
        if params:
            self.params = params
        return self

    @staticmethod
    def storable_text(value: str) -> str:
        """``value`` without the characters a database cannot hold (for logs)."""
        return _UNSTORABLE_TEXT.sub("", value)

    @staticmethod
    def _refuse_unstorable_text(payload: Any) -> None:
        if any(_UNSTORABLE_TEXT.search(text) for text in _strings_in(payload)):
            raise BadRequestException(_UNSTORABLE_TEXT_MESSAGE)

    async def json(self) -> dict[str, Any]:
        """Parse and cache the JSON body, refusing unstorable text with a 400.

        A refused body leaves no cached parse behind, so no later reader of
        the request can pick up what the first one was refused.
        """
        if self._json_data is None:
            data = await super().json()
            try:
                self._refuse_unstorable_text(data)
            except BadRequestException:
                self._json_data = None
                raise
        return self._json_data

    async def _parse_multipart(self) -> None:
        """Parse a form body, refusing unstorable text in fields and filenames."""
        if self._files is not None:
            return
        await super()._parse_multipart()
        filenames = [upload.filename for upload in self._files.values()]
        try:
            self._refuse_unstorable_text([self._form_params, filenames])
        except BadRequestException:
            self._files, self._form_params = {}, {}
            self._form_refused = True
            raise

    async def all(self) -> dict[str, Any]:
        """Every input merged, with a form refused for unstorable text refused here too.

        The base method forgives a form it can't parse and carries on with the
        query. Forgiving this refusal would run the handler on what's left and
        answer 200 for a save that dropped every field.
        """
        data = await super().all()
        if self._form_refused:
            raise BadRequestException(_UNSTORABLE_TEXT_MESSAGE)
        return data

    def set_input(self, name: str, value: Any) -> None:
        """Merge a value into the request input bag.

        Used by controllers to inject path parameters so FormRequest
        validation can access them as regular input fields.
        """
        self._input.set(name, value)

    # -----------------------
    # Route and Auth Helpers
    # -----------------------

    def get_route(self) -> Any:
        """Return matched route object."""
        return self.route

    def set_route(self, route: Any) -> Request:
        """Set matched route object."""
        self.route = route
        return self

    def user(self) -> Any:
        """Return authenticated user, if set."""
        return self._user

    def set_user(self, user: Any) -> Request:
        """Set authenticated user object."""
        self._user = user
        return self

    @property
    def request_id(self) -> str:
        """Return unique request ID."""
        return self._request_id

    @request_id.setter
    def request_id(self, value: str) -> None:
        """Allow middleware to set the ID (e.g. from ``X-Request-ID``)."""
        self._request_id = str(value) if value is not None else str(uuid.uuid4())
