"""Laravel-style API Resources for transforming models into JSON responses."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "JsonResource": (".JsonResource", "JsonResource"),
    "MissingValue": (".MissingValue", "MissingValue"),
    "ResourceCollection": (".ResourceCollection", "ResourceCollection"),
    "opt_bool": (".Serialization", "opt_bool"),
    "opt_datetime": (".Serialization", "opt_datetime"),
    "opt_float": (".Serialization", "opt_float"),
    "opt_int": (".Serialization", "opt_int"),
    "opt_list": (".Serialization", "opt_list"),
    "opt_str": (".Serialization", "opt_str"),
}

__all__ = [
    "JsonResource",
    "MissingValue",
    "ResourceCollection",
    "opt_bool",
    "opt_datetime",
    "opt_float",
    "opt_int",
    "opt_list",
    "opt_str",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
