"""Pagination parameter coercion — shared across all controllers.

Replaces 30+ occurrences of::

    limit = int(validated.get("limit") or 24)
    offset = int(validated.get("offset") if validated.get("offset") is not None else 0)

with::

    pg = Pagination.from_validated(validated, default_limit=24)
    # pg.limit, pg.offset, pg.page

ROOT-CAUSE (scenario 3 cycle 1, stress test) — defense-in-depth
``max_offset`` cap. Pre-fix, ``from_validated`` clamped ``limit`` to
``[1, max_limit]`` and floored ``offset`` at 0, but had no upper
bound on ``offset``. Every well-known caller already validated offset
upstream (via ``PAGING_RULES`` / ``paging_rules`` / per-Request
``between:0,N``), but the contract was: "if upstream forgot, the DB
gets ``OFFSET 999_999_999`` which scans everything before the cursor".
A new endpoint that copies an existing ``Pagination.from_validated``
call without also adding offset validation is one diff away from a
silent DB-scan vector. Adding a framework-level ``max_offset`` (default
``1_000_000``, matching the app's ``paging_rules`` default)
closes the gap as a belt-and-braces guard. Existing callers continue
to validate at the FormRequest layer (loud 422) and now get a quiet
clamp at the framework layer too. Page is recomputed from the
(possibly-clamped) offset so the ``meta.page`` value never lies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Pagination:
    """Immutable pagination parameters with safe coercion."""

    limit: int
    offset: int
    page: int

    MAX_LIMIT: int = 500
    # Framework-level defense-in-depth cap. Matches the app's
    # ``paging_rules`` default so a Request that spreads
    # ``paging_rules()`` and a controller that calls
    # ``Pagination.from_validated`` agree on the upper bound.
    MAX_OFFSET: int = 1_000_000

    @classmethod
    def from_validated(
        cls,
        data: dict[str, Any],
        *,
        default_limit: int = 24,
        max_limit: int = 500,
        max_offset: int = MAX_OFFSET,
        limit_key: str = "limit",
        offset_key: str = "offset",
        page_key: str = "page",
    ) -> Pagination:
        """Build from a validated request dict with safe int coercion.

        ``max_offset`` is a defense-in-depth cap; upstream FormRequest
        validation typically rejects out-of-range offsets with 422
        before reaching here, but the framework also clamps so a
        forgotten validator can never reach the DB with an unbounded
        ``OFFSET``. Pass an explicit value to tighten or widen.
        """
        limit = cls._safe_int(data.get(limit_key), default=default_limit)
        limit = max(1, min(limit, max_limit))

        raw_page = data.get(page_key)
        raw_offset = data.get(offset_key)

        # Treat empty string the same as missing on BOTH params so a
        # form-submitted ``?offset=&page=5`` honours the populated
        # page. Pre-fix ``if raw_offset is not None`` matched the
        # empty string (it's not None), entered the offset branch,
        # ``_safe_int("", default=0)`` returned 0, and the user's
        # ``page=5`` was silently ignored — the listing snapped to
        # page 1 instead of page 5. Common shape when a form UI
        # submits empty input fields alongside populated ones.
        offset_provided = raw_offset is not None and raw_offset != ""
        page_provided = raw_page is not None and raw_page != ""

        if offset_provided:
            offset = max(0, cls._safe_int(raw_offset, default=0))
        elif page_provided:
            page = max(1, cls._safe_int(raw_page, default=1))
            offset = (page - 1) * limit
        else:
            offset = 0

        # Framework-level ceiling — clamps the (already-floored) offset
        # to ``[0, max_offset]``. Page is derived AFTER clamp so the
        # response meta reflects what was actually used.
        offset = min(offset, max(0, max_offset))
        page = (offset // limit) + 1 if limit > 0 else 1

        return cls(limit=limit, offset=offset, page=page)

    @classmethod
    def from_query(
        cls,
        request: Any,
        *,
        default_limit: int = 24,
        max_limit: int = 500,
        max_offset: int = MAX_OFFSET,
    ) -> Pagination:
        """Build directly from a Request's query parameters."""
        data = {
            "limit": request.query("limit"),
            "offset": request.query("offset"),
            "page": request.query("page"),
        }
        return cls.from_validated(
            data,
            default_limit=default_limit,
            max_limit=max_limit,
            max_offset=max_offset,
        )

    @staticmethod
    def _safe_int(raw: Any, *, default: int = 0) -> int:
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except (TypeError, ValueError):
            return default


def paging_rules(
    *,
    min_limit: int = 1,
    max_limit: int = 100,
    max_offset: int = Pagination.MAX_OFFSET,
) -> dict[str, str]:
    """Return the canonical ``limit`` / ``offset`` validation rules.

    The input-side twin of :meth:`Pagination.from_validated` (the
    consumption-side clamp): a ``FormRequest`` spreads these rules to
    reject bad paging input with a loud 422, and the controller then
    clamps the validated values. Co-located so both share one source of
    the ``max_offset`` bound (``Pagination.MAX_OFFSET``).

    Both keys are ``nullable`` so an absent field passes through to the
    controller's ``or DEFAULT`` fallback, but a present ``"abc"`` / ``-3``
    fails validation with a 422.

    Usage::

        class SomeListRequest(FormRequest):
            def rules(self) -> dict:
                return {**paging_rules(max_limit=50), "period": "nullable|string"}
    """
    return {
        "limit": f"nullable|integer|between:{min_limit},{max_limit}",
        "offset": f"nullable|integer|min:0|max:{max_offset}",
    }
