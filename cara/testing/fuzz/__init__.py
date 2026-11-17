"""Property fuzzing of an HTTP API from its published OpenAPI contract.

Hypothesis strategies that turn a product's generated OpenAPI document into
whole requests — every path hole, declared and generic query keys, JSON
bodies — each either *valid* (the schema honoured, real ids where the product
supplies a pool) or *hostile* (wrong types, integers past every column width,
NUL bytes, lone surrogates, SQL fragments, malformed JSON). One field is made
hostile at a time so the rest of the request still reaches the handler.

The framework owns the generators because nothing in them is product
vocabulary; a product supplies only its spec, its id pools and the transport
(a live server over HTTP, or its ASGI app in process). ``hypothesis`` is a
dev-only dependency: import this package only from tests.

    from cara.testing.fuzz import HypothesisStorage, IdPools, OpenApiRequests
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "FuzzRequest": (".FuzzRequest", "FuzzRequest"),
    "GENERIC_QUERY_KEYS": (".OpenApiRequests", "GENERIC_QUERY_KEYS"),
    "HostileValues": (".HostileValues", "HostileValues"),
    "HypothesisStorage": (".HypothesisStorage", "HypothesisStorage"),
    "IdPools": (".IdPools", "IdPools"),
    "OpenApiOperation": (".OpenApiOperation", "OpenApiOperation"),
    "OpenApiRequests": (".OpenApiRequests", "OpenApiRequests"),
}

__all__ = [
    "FuzzRequest",
    "GENERIC_QUERY_KEYS",
    "HostileValues",
    "HypothesisStorage",
    "IdPools",
    "OpenApiOperation",
    "OpenApiRequests",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
