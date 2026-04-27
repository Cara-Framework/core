"""``FilteredFormRequest`` — Cara FormRequest that knows about FilterSets.

The handwritten ``ProductIndexRequest`` and friends used to look like:

::

    class ProductIndexRequest(FormRequest):
        def rules(self) -> dict:
            return {
                "sort_by": PRODUCT_SORTS.validation_rule(),
                "limit":   "nullable|integer|between:1,100",
                "offset":  "nullable|integer|min:0",
                **PRODUCT_FILTERS.validation_rules(),
            }

— same boilerplate on every list endpoint. ``FilteredFormRequest``
collapses that into class-level configuration:

::

    class ProductIndexRequest(FilteredFormRequest):
        filter_set    = PRODUCT_FILTERS
        sort_registry = PRODUCT_SORTS
        relations     = ("images", "current_price", "container",
                         "details", "videos")
        # ``extra_rules`` defaults to ``PAGING_RULES`` — opt out
        # by setting ``extra_rules = {}`` if your endpoint uses
        # cursor pagination instead.

Category- or brand-scoped endpoints layer on dynamic forced state:

::

    class CategoryProductsRequest(FilteredFormRequest):
        filter_set    = PRODUCT_FILTERS
        sort_registry = PRODUCT_SORTS
        default_sort  = "trending"   # used when caller omits sort_by

        async def merge_filters(self, request, validated):
            # Inject the route param so the pipeline always scopes
            # to the current category subtree, regardless of what
            # the user typed in the URL.
            return {"category_id": int(request.param("category_id"))}

That's the entire endpoint contract. The base class:

* auto-derives ``rules()`` from ``filter_set.validation_rules()`` +
  ``sort_registry.validation_rule()`` + ``extra_rules``;
* layers ``default_filters`` (fill-in) → ``merge_filters()``
  (dynamic override) → ``forced_filters`` (static override) onto
  the validated payload before parsing;
* parses the result through ``filter_set.parse()`` once, exposing
  it on the request as ``request.parsed_filters`` and on the
  validated dict as ``_parsed_filters`` (the leading underscore
  keeps it out of casual ``**validated`` splats);
* provides ``request.pipeline(builder)`` so controllers can chain
  straight into a ``FilterPipeline`` with the configured eager
  ``relations`` already applied.

The result: a typical list endpoint controller is 3 lines.

``filter_ctx`` is opaque (any app-defined SQL-alias context object).
The framework only forwards it to ``FilterPipeline`` / ``FilterSet``;
concrete filters know how to read its fields.
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Iterable, Mapping, Optional, Union

from cara.http import FormRequest

from .FilterSet import FilterSet
from .Pipeline import FilterPipeline
from .Relations import RelationSet
from .Sorter import SortRegistry


# Type alias — endpoint relations may be a canonical ``RelationSet``
# (preferred) or a bare iterable of strings (back-compat for ad-hoc
# inline tuples). ``Pipeline.with_`` accepts both.
Relations = Union[RelationSet, Iterable[str]]


# Sensible default — every list endpoint we've ever shipped uses
# offset paging with ≤100 page size. Subclasses can override or
# extend by setting their own ``extra_rules`` mapping.
PAGING_RULES: Mapping[str, str] = {
    "limit": "nullable|integer|between:1,100",
    "offset": "nullable|integer|min:0",
}


class FilteredFormRequest(FormRequest):
    """FormRequest base for endpoints driven by a ``FilterSet``.

    Subclasses set the class-level configuration; the base class
    handles rules, parsing, scope injection, and pipeline
    construction.

    Class attributes:
        filter_set: The canonical ``FilterSet`` for this endpoint
            (e.g. ``PRODUCT_FILTERS``). Required.
        sort_registry: Optional ``SortRegistry`` — when present the
            base class auto-adds the ``sort_by`` rule and threads
            the resolved name through the pipeline.
        extra_rules: Non-filter rules (paging, free-form flags).
            Defaults to ``PAGING_RULES``; override to ``{}`` for
            endpoints that don't paginate.
        filter_ctx: SQL-alias context (opaque). Defaults to ``None``;
            subclasses provide their own ``DEFAULT_CONTEXT`` /
            ``FACET_CONTEXT`` instance.
        relations: Eager-load tuple applied to every pipeline built
            via ``request.pipeline(builder)``. Saves the controller
            from repeating the same ``("images", "current_price",
            ...)`` tuple at every call site.
        default_filters: Payload values applied with ``setdefault`` —
            user input always wins, but missing keys fall through
            to these defaults. Used for things like a category page
            wanting ``in_stock=true`` until the user explicitly
            unticks it.
        forced_filters: Payload values that *override* user input.
            Used for endpoint-level scope locks (deal feed forces
            ``on_sale=true``; admin product browser forces
            ``status=any``). Static dict — see ``merge_filters`` for
            dynamic injection.
        default_sort: Sort name applied when the user omits
            ``sort_by``. Defaults to ``""`` (let SortRegistry pick
            its registered default).
    """

    filter_set: ClassVar[Optional[FilterSet]] = None
    sort_registry: ClassVar[Optional[SortRegistry]] = None
    extra_rules: ClassVar[Mapping[str, str]] = PAGING_RULES
    filter_ctx: ClassVar[Any] = None

    relations: ClassVar[Relations] = ()
    default_filters: ClassVar[Mapping[str, Any]] = {}
    forced_filters: ClassVar[Mapping[str, Any]] = {}
    default_sort: ClassVar[str] = ""

    # Payload key that carries the sort name. Defaults to
    # ``"sort_by"`` (storefront convention) but admin / legacy
    # surfaces sometimes ship ``"sort"``. Override at the class
    # level — the validation rule, the parsed lookup, and the
    # ``request.sort_name`` exposure all flow from this single
    # attribute so renaming an endpoint's param is one edit.
    sort_param: ClassVar[str] = "sort_by"

    # ── Rule synthesis ─────────────────────────────────────────────

    def rules(self) -> dict:
        """Auto-derive the Cara validation rule dict.

        Composition order: ``extra_rules`` → ``filter_set`` rules →
        ``sort_registry`` rule. Earlier entries win on key clash, so
        a subclass that needs a stricter ``limit`` rule than the
        default just sets it in its own ``extra_rules`` override.
        """
        rules: dict = dict(self.extra_rules)
        if self.filter_set is not None:
            for key, rule in self.filter_set.validation_rules().items():
                rules.setdefault(key, rule)
        if self.sort_registry is not None:
            rules.setdefault(self.sort_param, self.sort_registry.validation_rule())
        return rules

    # ── Subclass hook ──────────────────────────────────────────────

    async def merge_filters(self, request, validated: Dict[str, Any]) -> Dict[str, Any]:
        """Dynamic forced-filter hook — return ``{key: value}`` to inject.

        Called after validation, *before* parsing. Use for scope
        locks that depend on the request itself: route params
        (category id, brand slug), the current user, or feature
        flags.

        The return value overrides user input but is itself
        overridden by ``forced_filters`` (static class-level
        override wins). If you need static + dynamic together,
        prefer ``forced_filters`` for the static slice and this
        hook for the per-request slice.

        Default: no-op. Override in subclasses.
        """
        return {}

    # ── Validation + post-process ──────────────────────────────────

    async def validate_request(self, request) -> dict:
        """Validate, layer scope, parse, then expose state.

        Sequence:

        1. Cara validates the raw payload against ``rules()``.
        2. ``default_filters`` fill in missing keys (user wins).
        3. ``merge_filters(request, validated)`` injects dynamic
           overrides (route params, current user).
        4. ``forced_filters`` overrides everything (static lock).
        5. ``filter_set.parse(...)`` produces the canonical dict.
        6. ``default_sort`` fills in ``sort_by`` if still missing.
        7. State is mirrored onto ``request`` and the validated dict;
           ``request.pipeline(builder)`` is bound for chaining.
        """
        validated = await super().validate_request(request)

        # Step 2 — fill-in defaults (user wins)
        for key, value in self.default_filters.items():
            validated.setdefault(key, value)

        # Step 3 — dynamic per-request override (route params, etc.)
        dynamic_overrides = await self.merge_filters(request, validated) or {}
        if dynamic_overrides:
            validated.update(dynamic_overrides)

        # Step 4 — static forced override (final word)
        if self.forced_filters:
            validated.update(self.forced_filters)

        # Step 5 — parse once into canonical form
        parsed: dict = {}
        if self.filter_set is not None:
            parsed = self.filter_set.parse(validated)

        # Step 6 — sort fallback. ``sort_param`` defaults to
        # ``"sort_by"`` but admin / legacy surfaces may override it
        # to ``"sort"`` so the param-name change flows through one
        # attribute instead of separate hand-rolled lookups.
        sort_name = (
            validated.get(self.sort_param)
            if self.sort_registry is not None else None
        )
        if (not sort_name) and self.default_sort and self.sort_registry is not None:
            sort_name = self.default_sort
            validated[self.sort_param] = sort_name

        # Step 7 — best-effort attach to the request object. Some
        # Cara request implementations are dataclass-like and reject
        # arbitrary attributes; that's fine, callers can read the
        # same data off the validated dict.
        for attr, value in (
            ("parsed_filters", parsed),
            ("filter_set", self.filter_set),
            ("sort_registry", self.sort_registry),
            ("sort_name", sort_name),
            ("filter_ctx", self.filter_ctx),
            ("relations", self.relations),
        ):
            try:
                setattr(request, attr, value)
            except (AttributeError, TypeError):
                # Frozen / dataclass-like request; mirroring onto
                # ``validated`` below covers callers that only see
                # the dict, so the missing attribute isn't a problem.
                continue

        # Mirror onto the validated dict so handlers that only
        # receive ``validated`` (not ``request``) can still pick
        # up the parsed state without re-parsing.
        validated["_parsed_filters"] = parsed
        if sort_name is not None or self.sort_registry is not None:
            validated["_sort_name"] = sort_name

        # Bind a pipeline factory so controllers can do
        # ``request.pipeline(Product.active()).paginate(...)``
        # without re-plumbing the filter set / sort registry / eager.
        try:
            request.pipeline = self._pipeline_factory(parsed, sort_name)
        except (AttributeError, TypeError):
            # Frozen request — callers can build a pipeline manually
            # using validated["_parsed_filters"] / _sort_name above.
            pass

        return validated

    # ── Pipeline sugar ─────────────────────────────────────────────

    def _pipeline_factory(self, parsed: dict, sort_name: Optional[str]):
        """Return a ``builder -> FilterPipeline`` closure.

        Captures the already-parsed filter state, the resolved sort
        name, and the configured eager ``relations`` so callers
        don't repeat themselves. Equivalent to:

        ::

            FilterPipeline(builder, filters=..., sorts=..., ctx=...)
                .filtered_by(parsed)
                .sort_by(sort_name)
                .with_(*relations)
        """
        filter_set = self.filter_set
        sort_registry = self.sort_registry
        ctx = self.filter_ctx
        # ``relations`` may be a ``RelationSet`` or a bare iterable —
        # ``Pipeline.with_`` handles both, so just forward it.
        relations = self.relations

        def factory(builder: Any) -> FilterPipeline:
            pipe = FilterPipeline(
                builder,
                filters=filter_set,
                sorts=sort_registry,
                ctx=ctx,
            )
            if filter_set is not None:
                # Use the already-parsed dict — the request validated
                # + parsed once during ``validate_request``; re-parsing
                # here would just round-trip canonical values through
                # the input coercers for no reason.
                pipe.filtered_by(parsed)
            if sort_registry is not None:
                pipe.sort_by(sort_name)
            if relations:
                pipe.with_(relations)
            return pipe

        return factory


__all__ = ["FilteredFormRequest", "PAGING_RULES"]
