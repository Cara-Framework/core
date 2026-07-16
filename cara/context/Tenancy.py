"""Explicit tenant execution context with fail-closed UNSET semantics."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any


class _TenancyState:
    __slots__ = ("name",)

    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:
        return f"Tenancy.{self.name}"


UNSET = _TenancyState("UNSET")
CENTRAL = _TenancyState("CENTRAL")
_tenant_state: ContextVar[Any] = ContextVar(
    "cara_tenant_state",
    default=UNSET,
)


class Tenancy:
    """Request/job-scoped tenant, explicit central mode, or fail-closed unset."""

    UNSET = UNSET
    CENTRAL = CENTRAL

    @staticmethod
    def set(tenant_id: Any):
        if tenant_id is None or tenant_id is UNSET or tenant_id is CENTRAL:
            raise RuntimeError("Tenant scope requires a non-null tenant id.")
        return _tenant_state.set(tenant_id)

    @staticmethod
    def reset(token) -> None:
        _tenant_state.reset(token)

    @staticmethod
    def state() -> Any:
        return _tenant_state.get()

    @staticmethod
    def id() -> Any | None:
        state = _tenant_state.get()
        return None if state is UNSET or state is CENTRAL else state

    @staticmethod
    def is_unset() -> bool:
        return _tenant_state.get() is UNSET

    @staticmethod
    def is_central() -> bool:
        return _tenant_state.get() is CENTRAL

    @staticmethod
    def is_tenant() -> bool:
        state = _tenant_state.get()
        return state is not UNSET and state is not CENTRAL

    @staticmethod
    def clear() -> None:
        _tenant_state.set(UNSET)

    @staticmethod
    @contextmanager
    def as_tenant(tenant_id: Any):
        token = Tenancy.set(tenant_id)
        try:
            yield
        finally:
            _tenant_state.reset(token)

    @staticmethod
    @contextmanager
    def central():
        token = _tenant_state.set(CENTRAL)
        try:
            yield
        finally:
            _tenant_state.reset(token)
