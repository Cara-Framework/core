from __future__ import annotations

from .Facade import Facade


class Config(metaclass=Facade):
    key = "config"
