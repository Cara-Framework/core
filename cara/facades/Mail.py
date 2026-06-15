from __future__ import annotations

from .Facade import Facade


class Mail(metaclass=Facade):
    key = "mail"
