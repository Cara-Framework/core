from __future__ import annotations

from .Facade import Facade


class RateLimiter(metaclass=Facade):
    key = "rate"
