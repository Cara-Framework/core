"""Field definitions for Eloquent models using Blueprint."""

from __future__ import annotations

from .FieldMeta import FieldMeta


class Field(metaclass=FieldMeta):
    """Field factory that automatically proxies all Blueprint column methods."""

    pass
