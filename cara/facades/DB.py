from __future__ import annotations

from .Facade import Facade


class DB(metaclass=Facade):
    key = "DB"
