"""
HTTP Request Object for the Cara framework.

This module provides the Request class, encapsulating HTTP request data and utility methods for
request handling.
"""

import ipaddress
import os
import uuid
from functools import lru_cache
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

from cara.http.request import UploadedFile
from cara.http.request.context import current_request
from cara.http.request.Header import HeaderBag
from cara.http.request.Input import InputBag
from cara.http.request.mixins import (
    BodyParsingMixin,
    RequestHelpersMixin,
    ValidationHelpersMixin,
)


@lru_cache(maxsize=1)
def _trusted_proxy_networks() -> tuple:
    """Parse TRUSTED_PROXIES env into a tuple of ip_network objects.

    Accepts comma-separated IPs or CIDR blocks. Private loopback + link-local
    ranges are always included to match a typical dev + containerized layout.
    """
    raw = os.environ.get("TRUSTED_PROXIES", "")
    nets = []
    # Loopback + common private ranges for dev / behind-LB topology.
    defaults = ("127.0.0.0/8", "::1/128", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    for entry in (*defaults, *[e.strip() for e in raw.split(",") if e.strip()]):
        try:
            nets.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            continue
    return tuple(nets)


def _is_trusted_proxy(addr: str) -> bool:
    """True if `addr` is inside any configured trusted-proxy network."""
    try:
        ip_obj = ipaddress.ip_address(addr)
    except (ValueError, TypeError):
        return False
    return any(ip_obj in net for net in _trusted_proxy_networks())


class Request(BodyParsingMixin, ValidationHelpersMixin, RequestHelpersMixin):
    """
    HTTP Request object for ASGI‐based APIs.

    Handles parsing of headers, query params, JSON body, form data, file uploads, integrates with
    the Validation component for input validation, and offers convenience methods like
    only()/except_()/has()/filled().
    """

    def __init__(self, application):
        self.application = application
        self.scope: Dict[str, Any] = {}
        self.receive: Any = None

        self.headers = HeaderBag()
        self._input = InputBag()
        self.params: Dict[str, Any] = {}
        self.route = None

        self._user: Any = None
        self._ip: Optional[str] = None
        self._body: Optional[bytes] = None
        self._body_consumed = False

        self._query_params: Optional[Dict[str, List[str]]] = None
        self._form_params: Optional[Dict[str, Any]] = None
        self._json_data: Optional[Dict[str, Any]] = None
        self._files: Optional[Dict[str, UploadedFile]] = None

        self.validated: Dict[str, Any] = {}
        self._request_id = str(uuid.uuid4())

        current_request.set(self)

    def app(self):
        """Return the application instance (Laravel-style convenience)."""
        return self.application

    def load(
        self,
        scope: Dict[str, Any] = None,
        receive: Any = None,
    ) -> "Request":
        """
        Initialize request data from ASGI scope and receive function.

        Parses headers and query parameters immediately.
        """
        self.scope = scope or {}
        self.receive = receive

        headers = {
            key.decode().lower(): value.decode()
            for key, value in self.scope.get("headers", [])
        }
        self.headers.load(headers)

        raw_qs = self.scope.get("query_string", b"").decode()
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

    def header(self, name: str, default: Any = None) -> Optional[str]:
        """
        Retrieve a header value (case‐insensitive).

        Returns default if missing.
        """
        value = self.headers.get(name)
        return value if value is not None else default

    def get_host(self) -> str:
        """Return the Host header value."""
        return self.header("host", "")

    def ip(self) -> Optional[str]:
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
    def query_params(self) -> Dict[str, List[str]]:
        """Return parsed query parameters as a dict of lists."""
        if self._query_params is None:
            raw_qs = self.scope.get("query_string", b"").decode()
            self._query_params = parse_qs(raw_qs)
        return self._query_params

    def get_query_param(self, key: str, default: Any = None) -> Optional[str]:
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

    def load_params(self, params: Dict[str, Any] = None) -> "Request":
        """Load route parameters after routing was matched."""
        if params:
            self.params = params
        return self

    # -----------------------
    # Route and Auth Helpers
    # -----------------------

    def get_route(self) -> Any:
        """Return matched route object."""
        return self.route

    def set_route(self, route: Any) -> "Request":
        """Set matched route object."""
        self.route = route
        return self

    def user(self) -> Any:
        """Return authenticated user, if set."""
        return self._user

    def set_user(self, user: Any) -> "Request":
        """Set authenticated user object."""
        self._user = user
        return self

    @property
    def request_id(self) -> str:
        """Return unique request ID."""
        return self._request_id

    def wants_json(self) -> bool:
        """
        Determine if the request wants a JSON response.

        Checks the Accept header for application/json content type.
        Also checks for XMLHttpRequest header for AJAX requests.
        """
        accept_header = self.header("Accept", "")

        # Check for explicit JSON accept header
        if "application/json" in accept_header:
            return True

        # Check for AJAX requests (common pattern)
        if self.header("X-Requested-With", "").lower() == "xmlhttprequest":
            return True

        return False
