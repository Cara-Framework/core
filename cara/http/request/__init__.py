"""Http — layer barrel (generated, DOCTRINE §5.1). — request subpackage."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "Header": (".Header", "Header"),
    "HeaderBag": (".HeaderBag", "HeaderBag"),
    "Input": (".Input", "Input"),
    "InputBag": (".InputBag", "InputBag"),
    "KeyPart": (".utils", "KeyPart"),
    "MakesBodyParsing": (".mixins", "MakesBodyParsing"),
    "MakesRequestHelpers": (".mixins", "MakesRequestHelpers"),
    "MakesValidationHelpers": (".mixins", "MakesValidationHelpers"),
    "QueryStringParser": (".utils", "QueryStringParser"),
    "Request": (".Request", "Request"),
    "RequestProvider": (".RequestProvider", "RequestProvider"),
    "T": (".InputBag", "T"),
    "UploadedFile": (".UploadedFile", "UploadedFile"),
    "current_request": (".Context", "current_request"),
}

__all__ = [
    "Header",
    "HeaderBag",
    "Input",
    "InputBag",
    "KeyPart",
    "MakesBodyParsing",
    "MakesRequestHelpers",
    "MakesValidationHelpers",
    "QueryStringParser",
    "Request",
    "RequestProvider",
    "T",
    "UploadedFile",
    "current_request",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
