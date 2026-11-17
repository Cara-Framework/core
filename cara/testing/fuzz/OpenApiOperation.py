"""One operation of an OpenAPI document, with the bits a fuzz sweep reads."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

_HTTP_METHODS = ("get", "put", "post", "delete", "patch")


@dataclass(frozen=True)
class OpenApiOperation:
    """``method`` upper-case, ``path`` as the spec spells it (``{name}`` holes)."""

    method: str
    path: str
    operation: dict = field(compare=False, repr=False)

    @property
    def operation_id(self) -> str:
        return str(self.operation.get("operationId") or f"{self.method} {self.path}")

    @property
    def middleware(self) -> tuple[str, ...]:
        """Cara's ``x-middleware`` extension: the route's middleware aliases."""
        return tuple(self.operation.get("x-middleware") or ())

    @property
    def path_params(self) -> list[str]:
        return re.findall(r"\{(\w+)\}", self.path)

    def __str__(self) -> str:  # pytest id
        return f"{self.method} {self.path}"

    @classmethod
    def all(
        cls, spec: dict, *, where: Callable[[OpenApiOperation], bool] | None = None
    ) -> list[OpenApiOperation]:
        """Every operation of ``spec`` (stable order), filtered by ``where``."""
        return [op for op in cls._walk(spec) if where is None or where(op)]

    @classmethod
    def _walk(cls, spec: dict) -> Iterator[OpenApiOperation]:
        for path in sorted(spec.get("paths", {})):
            item = spec["paths"][path]
            for method in _HTTP_METHODS:
                if method in item:
                    yield cls(method.upper(), path, item[method])
