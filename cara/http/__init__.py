"""HTTP public API with dependency-isolated lazy exports.

Importing an HTTP client must not eagerly load request body parsing or its
optional multipart dependency. Server-side types are imported only when a
consumer asks for the corresponding public symbol.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "Controller": ("cara.http.controllers", "Controller"),
    "FormRequest": ("cara.http.requests", "FormRequest"),
    "InvalidCursor": ("cara.http.Cursor", "InvalidCursor"),
    "JsonResource": ("cara.http.resources", "JsonResource"),
    "MissingValue": ("cara.http.resources", "MissingValue"),
    "Pagination": ("cara.http.Pagination", "Pagination"),
    "Request": ("cara.http.request.Request", "Request"),
    "ResourceCollection": ("cara.http.resources", "ResourceCollection"),
    "Response": ("cara.http.response.Response", "Response"),
    "apply_no_cache": ("cara.http.CacheHeaders", "apply_no_cache"),
    "apply_private_cache": ("cara.http.CacheHeaders", "apply_private_cache"),
    "apply_public_swr_cache": (
        "cara.http.CacheHeaders",
        "apply_public_swr_cache",
    ),
    "assert_editable_fields": ("cara.http.Payload", "assert_editable_fields"),
    "cursor_fingerprint": ("cara.http.Cursor", "cursor_fingerprint"),
    "cursor_rules": ("cara.http.Cursor", "cursor_rules"),
    "decode_cursor": ("cara.http.Cursor", "decode_cursor"),
    "encode_cursor": ("cara.http.Cursor", "encode_cursor"),
    "paging_rules": ("cara.http.Pagination", "paging_rules"),
    "slice_page_with_lookahead": (
        "cara.http.Cursor",
        "slice_page_with_lookahead",
    ),
    "strip_none_values": ("cara.http.Payload", "strip_none_values"),
    "validated_query_int": ("cara.http.Payload", "validated_query_int"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
