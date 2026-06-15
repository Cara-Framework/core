from __future__ import annotations

from .Facade import Facade


class Cache(metaclass=Facade):
    key = "cache"
