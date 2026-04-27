"""Fluent filter / sort / paginate composition over a Cara QueryBuilder.

Laravel-style chaining for the read path:

::

    result = (
        FilterPipeline(Product.active(),
                       filters=PRODUCT_FILTERS, sorts=PRODUCT_SORTS)
        .filter_by(payload)
        .sort_by(payload.get("sort_by"))
        .with_("images", "current_price", "container")
        .cached("products", ttl=300, when=lambda p: not p.parsed)
        .paginate(limit=24, offset=0, resource=ProductResource)
    )

The pipeline is the only place that knows the order of operations
(parse → apply WHERE → count → apply ORDER BY + JOINs → eager-load
→ paginate). Repos / services compose at the Pipeline level
instead of re-implementing that order; the framework guarantees
identical behavior across every list endpoint.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Dict, Iterable, List, Optional, Type

from .FilterSet import FilterSet
from .Sorter import SortRegistry


class FilterPipeline:
    """Fluent composer over a Cara QueryBuilder.

    Holds the builder + filter set + sort registry, plus the
    user-supplied state (payload / sort name / eager relations).
    Mutations return ``self`` so callers can chain. Terminal
    operations (``paginate``, ``count``, ``get``) materialise the
    query.

    ``ctx`` is opaque app-defined state (typically a ``FilterContext``
    dataclass with SQL-alias expressions); the pipeline only forwards
    it to ``FilterSet.where_clauses`` / ``apply_to_builder``.
    """

    def __init__(
        self,
        builder: Any,
        *,
        filters: Optional[FilterSet] = None,
        sorts: Optional[SortRegistry] = None,
        ctx: Any = None,
        product_id_column: str = "id",
    ) -> None:
        self._builder = builder
        self._filters = filters
        self._sorts = sorts
        self._ctx = ctx
        self._product_id_column = product_id_column

        self._payload: Optional[Dict[str, Any]] = None
        self._parsed: Optional[Dict[str, Any]] = None
        self._sort_name: Optional[str] = None
        self._eager: List[str] = []

    # ── Fluent builders ─────────────────────────────────────────────

    def filter_by(self, payload: Optional[Dict[str, Any]]) -> "FilterPipeline":
        """Apply the configured ``FilterSet`` to a raw payload.

        The payload is stored as-is — the actual SQL composition
        happens lazily in the terminal operation so callers can
        chain ``filter_by(...).sort_by(...).with_(...)`` in any
        order without thrashing the builder.

        Callers that already hold a parsed dict (FormRequest, cache
        layer) should prefer ``filtered_by(parsed)`` to avoid the
        round-trip parse — same end state, half the work.
        """
        if self._filters is None:
            raise RuntimeError(
                "FilterPipeline.filter_by called without a filter set; "
                "construct the pipeline with ``filters=<your FilterSet>``."
            )
        self._payload = payload or {}
        # Eagerly parse so callers can observe ``self.parsed`` /
        # ``self.cache_key`` before terminating the pipeline (useful
        # in tests + cache-key generation).
        self._parsed = self._filters.parse(self._payload)
        return self

    def filtered_by(self, parsed: Optional[Dict[str, Any]]) -> "FilterPipeline":
        """Use an already-parsed canonical filter dict (skip parsing).

        Identical end state to ``filter_by`` but bypasses
        ``FilterSet.parse`` — for callers (FormRequest,
        cache-warming jobs, repo entry points) that have already
        produced the canonical form. Re-parsing canonical values
        through filter-specific input coercers (CSV split, JSON
        decode, bool truthiness) is unnecessary and in some cases
        type-unsafe (e.g. running a list through a CSV splitter).
        """
        if self._filters is None:
            raise RuntimeError(
                "FilterPipeline.filtered_by called without a filter set; "
                "construct the pipeline with ``filters=<your FilterSet>``."
            )
        self._parsed = dict(parsed or {})
        # Mirror the parsed state into ``_payload`` so introspection
        # (``self.payload``, debug repr) reads consistently regardless
        # of whether the caller arrived via raw or parsed input.
        self._payload = dict(self._parsed)
        return self

    def sort_by(self, name: Optional[str]) -> "FilterPipeline":
        """Layer the configured ``SortRegistry`` ORDER BY (+ JOIN/SELECT)."""
        if self._sorts is None:
            raise RuntimeError(
                "FilterPipeline.sort_by called without a sort registry; "
                "construct the pipeline with ``sorts=<your SortRegistry>``."
            )
        self._sort_name = name
        return self

    def with_(self, *relations: Any) -> "FilterPipeline":
        """Eager-load the given relations on the materialised rows.

        Accepts either bare relation-name strings::

            pipe.with_("images", "current_price", "container")

        or any iterable (``RelationSet``, list, tuple) — useful for
        canonical presets::

            pipe.with_(PRODUCT_CARD_RELATIONS)

        or a mix of both::

            pipe.with_(PRODUCT_CARD_RELATIONS, "details")

        Duplicates are dropped while preserving first-seen order so
        composing a base preset with extras never reorders the base.
        """
        flat: List[str] = []
        seen: set = set()
        for item in relations:
            if isinstance(item, str):
                names: Iterable[str] = (item,) if item else ()
            else:
                # Iterable / RelationSet / tuple / list
                names = item
            for name in names:
                if not name or name in seen:
                    continue
                seen.add(name)
                flat.append(name)
        self._eager = flat
        return self

    # ── Caching ────────────────────────────────────────────────────

    def cached(
        self,
        prefix: str,
        *,
        ttl: int,
        when: Optional[Callable[["FilterPipeline"], bool]] = None,
    ) -> "_CachedPipeline":
        """Wrap the next terminal op in ``Cache.remember(...)``.

        ::

            result = (
                pipe.cached("products", ttl=300,
                            when=lambda p: p.sort_name in CACHEABLE_SORTS
                                           and not p.parsed)
                    .paginate(limit=24, offset=0)
            )

        The cache key is auto-derived from ``prefix`` +
        ``sort_name`` + ``limit`` + ``offset`` + the canonical
        ``cache_key`` fragment. Two semantically identical requests
        always collide on the same entry regardless of payload-key
        insertion order — that property comes from
        ``FilterSet.cache_key`` and the pipeline never invalidates it.

        Args:
            prefix: Cache-key prefix (``"products"``, ``"deals"``, …).
                Should be unique per endpoint to prevent collisions
                between two surfaces that happen to share the same
                filter shape.
            ttl: Seconds to keep an entry alive. Cache.remember owns
                expiry — ``0`` would disable caching effectively
                but the predicate ``when`` is the cleaner switch.
            when: Optional predicate over the pipeline state. If it
                returns False the terminal op runs uncached.
                Defaults to "always cacheable" — pass a stricter
                predicate (e.g. unfiltered + featured/trending only)
                to avoid blowing up the cache with one entry per
                unique filter combination.
        """
        return _CachedPipeline(self, prefix=prefix, ttl=ttl, when=when)

    # ── Introspection ──────────────────────────────────────────────

    @property
    def parsed(self) -> Dict[str, Any]:
        """Canonical parsed-filter dict (empty if ``filter_by`` not called)."""
        return dict(self._parsed or {})

    @property
    def payload(self) -> Dict[str, Any]:
        """Raw payload as-supplied (empty when ``filter_by`` not called)."""
        return dict(self._payload or {})

    @property
    def sort_name(self) -> str:
        """Resolved canonical sort name — ``""`` when no registry attached."""
        return self._resolved_sort_name()

    @property
    def relations(self) -> List[str]:
        """Eager-load relation names (empty when ``with_`` not called)."""
        return list(self._eager)

    @property
    def cache_key(self) -> str:
        """Cache-key fragment for the active filter state.

        Returns an empty fragment-equivalent (``"no_filters"``) if
        ``filter_by`` wasn't called, mirroring ``FilterSet.cache_key``.
        """
        if self._filters is None or self._parsed is None:
            return "no_filters"
        return self._filters.cache_key(self._parsed)

    def explain(self) -> Dict[str, Any]:
        """Render the pipeline's effective state without running SQL.

        Useful for ``/admin/debug/listing?...`` style introspection
        endpoints, regression tests that pin filter behaviour
        without touching the database, and the wizard's "preview
        the SQL we'd generate" view. Includes every piece a
        downstream consumer needs to reproduce the query: the
        parsed state, the cache key, the rendered WHERE fragments
        and their parameters, the resolved sort name, and the
        eager-load list.
        """
        where_sqls: List[str] = []
        where_params: List[Any] = []
        if self._filters is not None and self._parsed:
            where_sqls, where_params = self._filters.where_clauses(
                self._parsed, ctx=self._ctx,
            )
        return {
            "filter_set": (
                self._filters.names() if self._filters is not None else []
            ),
            "filter_state": dict(self._parsed or {}),
            "filter_payload": dict(self._payload or {}),
            "filter_cache_key": self.cache_key,
            "where_sqls": where_sqls,
            "where_params": where_params,
            "sort_registry": (
                self._sorts.names() if self._sorts is not None else []
            ),
            "sort_name": self._resolved_sort_name(),
            "eager": list(self._eager),
            "ctx": repr(self._ctx),
        }

    # ── Terminal operations ────────────────────────────────────────

    def count(self) -> int:
        """Materialise the count of matching rows under WHERE only."""
        return self._with_filters().count(self._product_id_column)

    def get(self, *, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Any]:
        """Materialise the rows under filter + sort + (optional) paging."""
        b = self._fully_composed()
        if limit is not None:
            b = b.limit(int(limit))
        if offset is not None:
            b = b.offset(int(offset))
        return b.get()

    def paginate(
        self,
        *,
        limit: int,
        offset: int,
        resource: Optional[Type] = None,
    ) -> Dict[str, Any]:
        """Materialise rows + count + transform into the standard list shape.

        Returns ``{"data": [...], "total": int, "limit": int,
        "offset": int, "sort_by": str, "filter_state": dict,
        "cache_key": str}`` — the shape every list endpoint
        already produces.

        Args:
            limit: Page size. Capped by the request layer; this
                method passes it through unchanged.
            offset: Zero-based row offset.
            resource: Optional ``cara.http.JsonResource`` subclass.
                When given, each row is transformed via
                ``resource(row).to_array()`` instead of the model's
                ``serialize()`` — the same transformation
                ``response.json(ResourceCollection(...))`` would
                apply for a single-item read. Pass ``None`` (the
                default) to fall back to ``model.serialize()`` for
                endpoints that haven't migrated to a resource yet.
        """
        # Counting is independent of sort / eager-load, so do it
        # before the more expensive composition.
        total = self.count()
        rows = self.get(limit=limit, offset=offset)

        if resource is not None:
            data = [resource(r).to_array() for r in rows]
        else:
            data = [r.serialize() for r in rows]

        return {
            "data": data,
            "total": total,
            "limit": limit,
            "offset": offset,
            "sort_by": self._resolved_sort_name(),
            "filter_state": dict(self._parsed or {}),
            "cache_key": self.cache_key,
        }

    # ── Internal composition ───────────────────────────────────────

    def _fresh_builder(self) -> Any:
        """Return an independent copy of the base builder.

        Cara's ``where_raw`` / ``order_by`` / ``with_`` all mutate
        the builder in place, so terminal ops can't share a single
        instance between ``count()`` and ``get()`` — the second
        operation would inherit the first's state and double-apply
        the WHERE clauses. Calling ``clone()`` on every terminal
        gives each op a virgin builder, mirroring the original
        ``Model.active()`` factory pattern (one fresh builder per
        terminal).
        """
        b = self._builder
        clone = getattr(b, "clone", None)
        return clone() if callable(clone) else b

    def _with_filters(self) -> Any:
        """Fresh builder with WHERE clauses applied (no sort, no eager)."""
        b = self._fresh_builder()
        if self._filters is not None and self._parsed:
            b = self._filters.apply_to_builder(b, self._parsed, ctx=self._ctx)
        return b

    def _fully_composed(self) -> Any:
        """Fresh builder with WHERE + ORDER BY + JOIN/SELECT + eager loads."""
        b = self._with_filters()
        if self._sorts is not None:
            b, _ = self._sorts.apply(b, self._sort_name)
        if self._eager:
            b = b.with_(self._eager)
        return b

    def _resolved_sort_name(self) -> str:
        """Canonical sort name (resolves aliases, default fallback)."""
        if self._sorts is None:
            return ""
        return self._sorts.resolve(self._sort_name).name


class _CachedPipeline:
    """Cache-aware proxy returned by ``FilterPipeline.cached(...)``.

    Forwards terminal ops to the wrapped pipeline, wrapping each in
    ``Cache.remember`` when the predicate ``when`` is satisfied.
    Non-terminal builders aren't proxied — ``cached()`` should be
    the last fluent step before the terminal call so the cached
    snapshot is unambiguous.
    """

    def __init__(
        self,
        pipe: FilterPipeline,
        *,
        prefix: str,
        ttl: int,
        when: Optional[Callable[[FilterPipeline], bool]] = None,
    ) -> None:
        self._pipe = pipe
        self._prefix = prefix
        self._ttl = ttl
        self._when = when or (lambda _p: True)

    # ── Cache key derivation ───────────────────────────────────────

    def _build_key(self, terminal: str, **terminal_args: Any) -> str:
        """Compose a stable cache key for a given terminal invocation.

        Format: ``{prefix}:{terminal}:{sort_name}:{kw}:{md5(filter_cache_key)}``.

        The MD5 of the filter fragment keeps long key fragments
        bounded (Redis keys have practical length limits) while
        preserving the round-trip determinism contract.
        """
        kw = ":".join(f"{k}={v}" for k, v in sorted(terminal_args.items()))
        digest = hashlib.md5(self._pipe.cache_key.encode()).hexdigest()
        sort = self._pipe.sort_name or "default"
        return f"{self._prefix}:{terminal}:{sort}:{kw}:{digest}"

    # ── Terminal proxies ───────────────────────────────────────────

    def paginate(
        self,
        *,
        limit: int,
        offset: int,
        resource: Optional[Type] = None,
    ) -> Dict[str, Any]:
        """Cached ``paginate`` — same kwargs, transparent caching."""
        if not self._when(self._pipe):
            return self._pipe.paginate(limit=limit, offset=offset, resource=resource)

        # Lazy import keeps ``cara.facades`` out of the import path
        # for tests that stub Cara — only invoked when actually caching.
        from cara.facades import Cache

        key = self._build_key("paginate", limit=limit, offset=offset)
        return Cache.remember(
            key, self._ttl,
            lambda: self._pipe.paginate(limit=limit, offset=offset, resource=resource),
        )

    def count(self) -> int:
        """Cached ``count`` — useful for facet counts that are read N×/page."""
        if not self._when(self._pipe):
            return self._pipe.count()

        from cara.facades import Cache

        return Cache.remember(self._build_key("count"), self._ttl, self._pipe.count)


def pipeline(
    builder: Any,
    *,
    filters: Optional[FilterSet] = None,
    sorts: Optional[SortRegistry] = None,
    ctx: Any = None,
    product_id_column: str = "id",
) -> FilterPipeline:
    """Sugar constructor — ``pipeline(query, filters=..., sorts=...)``.

    Equivalent to ``FilterPipeline(query, filters=..., sorts=...)``;
    exists so call sites read like a free function instead of a
    class instantiation.
    """
    return FilterPipeline(
        builder,
        filters=filters,
        sorts=sorts,
        ctx=ctx,
        product_id_column=product_id_column,
    )


__all__ = ["FilterPipeline", "pipeline"]
