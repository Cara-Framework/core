"""Http — layer barrel (generated, DOCTRINE §5.1). — client subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "FakeExhaustedError": (".FakeExhaustedError", "FakeExhaustedError"),
    "HttpFacade": (".HttpFacade", "HttpFacade"),
    "HttpFakeState": (".HttpFakeState", "HttpFakeState"),
    "PendingRequest": (".PendingRequest", "PendingRequest"),
    "StrayHttpRequestError": (".StrayHttpRequestError", "StrayHttpRequestError"),
    "activate": (".HttpFake", "activate"),
    "coerce": (".FakeResponses", "coerce"),
    "current": (".HttpFake", "current"),
    "deactivate": (".HttpFake", "deactivate"),
    "make_response": (".FakeResponses", "make_response"),
}

__all__ = [
    "FakeExhaustedError",
    "HttpFacade",
    "HttpFakeState",
    "PendingRequest",
    "StrayHttpRequestError",
    "activate",
    "coerce",
    "current",
    "deactivate",
    "make_response",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
