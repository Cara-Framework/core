from __future__ import annotations

from .Facade import Facade


class Crypt(metaclass=Facade):
    key = "crypt"
