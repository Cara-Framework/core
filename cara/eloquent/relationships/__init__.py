"""Eloquent — layer barrel (generated, DOCTRINE §5.1). — relationships subpackage."""

from cara._LazyExports import _install_lazy_exports

from .BelongsTo import BelongsTo as belongs_to
from .HasMany import HasMany as has_many
from .HasOne import HasOne as has_one

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseRelationship": (".BaseRelationship", "BaseRelationship"),
    "BelongsTo": (".BelongsTo", "BelongsTo"),
    "HasMany": (".HasMany", "HasMany"),
    "HasOne": (".HasOne", "HasOne"),
}

__all__ = [
    "BaseRelationship",
    "BelongsTo",
    "HasMany",
    "HasOne",
    "belongs_to",
    "has_many",
    "has_one",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
