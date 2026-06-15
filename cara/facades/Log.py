from __future__ import annotations

from .Facade import Facade


class Log(metaclass=Facade):
    key = "logger"
