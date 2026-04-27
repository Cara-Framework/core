"""Base class for a single composable filter dimension.

A ``Filter`` is the smallest unit of the filtering framework: it
describes ONE filter dimension (brand, price range, in-stock toggle,
attribute multi-select, …) end-to-end:

* what payload keys it accepts
* how to validate them
* how to parse raw input into a canonical Python value
* how to render itself as a SQL fragment
* how to contribute to a deterministic cache key
* how to describe itself to a frontend wizard / docs generator

Subclasses MUST implement ``validation_rules``, ``parse``, and
``where_sql``. UI metadata (``label``, ``description``,
``ui_control``, ``group``) is declared as class attributes so a
front-end wizard can introspect a ``FilterSet`` without writing
filter-specific code.

The framework's only mutable state is filter-instance attributes —
all SQL rendering takes the parsed value + an opaque ``ctx`` and
returns a string + params, so filters can be safely shared across
threads.

Note on ``ctx``: this base class declares ``ctx`` as ``Any`` rather
than tying the framework to a specific shape. Apps define their own
``FilterContext`` (typically a small dataclass carrying SQL-alias
expressions like ``product.id`` vs ``p.id``) and concrete filter
subclasses tighten the type hint at the override site.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


# ── UI control vocabulary ───────────────────────────────────────────
# Stable string values the frontend wizard / form renderer matches
# against to pick a control component. Adding a new control means
# coordinating a new value with the frontend; existing ones must
# never be repurposed.
UI_CONTROL_TEXT_INPUT: str = "text_input"
UI_CONTROL_NUMERIC_INPUT: str = "numeric_input"
UI_CONTROL_TOGGLE: str = "toggle"
UI_CONTROL_RANGE_SLIDER: str = "range_slider"
UI_CONTROL_CHECKBOX_LIST: str = "checkbox_list"
UI_CONTROL_RADIO_LIST: str = "radio_list"
UI_CONTROL_ATTRIBUTE_MATRIX: str = "attribute_matrix"
UI_CONTROL_HIDDEN: str = "hidden"


# ── Filter group vocabulary ─────────────────────────────────────────
# Wizards group filters into steps; each filter declares which group
# it belongs to so the frontend renders one step per group.
FILTER_GROUP_SCOPE: str = "scope"                 # category / search anchors
FILTER_GROUP_BRAND: str = "brand"                 # brand selection
FILTER_GROUP_PRICE: str = "price"                 # price / on-sale
FILTER_GROUP_AVAILABILITY: str = "availability"   # in-stock toggle
FILTER_GROUP_QUALITY: str = "quality"             # condition, rating
FILTER_GROUP_MARKETPLACE: str = "marketplace"
FILTER_GROUP_SPECS: str = "specs"                 # attributes


class Filter(ABC):
    """One filter dimension. Subclass to add a new dimension.

    Subclass contract:

    * ``name`` — class attribute, snake_case, must be unique within
      a ``FilterSet``. Used in cache keys, the JSON describe output,
      and (where applicable) as the primary payload field name.
    * ``validation_rules()`` — Cara FormRequest rule dict; one
      filter can declare multiple keys (PriceRangeFilter has
      ``price_min`` + ``price_max``).
    * ``parse(payload)`` — coerce raw payload to the canonical
      value the rest of the filter expects, or ``None`` when the
      filter is not active.
    * ``where_sql(value, ctx)`` — produce ``(sql_fragment, params)``.
      Always returns a self-contained fragment; the caller joins
      multiple filters with ``AND``.

    Optional:

    * ``encode_value(value)`` — render the canonical value back to a
      ``{payload_key: string}`` mapping for URL round-trip. Default
      uses the same serialiser as ``cache_key``; override for
      structured values that need a custom format.
    * ``cache_key(value)`` — bypass the default serializer when a
      filter needs custom canonicalization (e.g. case-insensitive
      strings).

    UI metadata (class attributes, optional but strongly encouraged):

    * ``label`` — human-readable name for the wizard ("Brand")
    * ``description`` — short help text shown next to the control
    * ``ui_control`` — vocabulary value (``UI_CONTROL_*`` constants)
    * ``group`` — wizard-step grouping (``FILTER_GROUP_*`` constants)
    * ``options_source`` — endpoint hint for filters whose options
      come from a facet query (e.g. ``"facet:brand_counts"``).
      ``None`` for self-describing filters (toggles, ranges).
    * ``requires`` — names of OTHER filters that must be set before
      this one is meaningful (e.g. an attribute filter requires
      ``category_id`` first). The wizard uses this to gate steps.
    """

    #: Canonical, snake_case filter name. Subclasses MUST override.
    name: str = ""

    # ── UI metadata (subclass overrides) ────────────────────────────

    #: Human-readable wizard label.
    label: str = ""

    #: Short help text under the control.
    description: str = ""

    #: One of the ``UI_CONTROL_*`` vocabulary values above.
    ui_control: str = UI_CONTROL_TEXT_INPUT

    #: One of the ``FILTER_GROUP_*`` vocabulary values; groups the
    #: filter into a wizard step.
    group: str = FILTER_GROUP_SCOPE

    #: Hint for the frontend on where to fetch dynamic options
    #: (``"facet:brand_counts"`` etc.). ``None`` for self-describing
    #: filters that don't need a separate options query.
    options_source: Optional[str] = None

    #: Names of OTHER filters that must be present in the parsed
    #: state before this one is meaningful. The wizard uses this to
    #: gate step visibility.
    requires: Tuple[str, ...] = ()

    # ── Required by every concrete subclass ─────────────────────────

    @abstractmethod
    def validation_rules(self) -> Dict[str, str]:
        """Cara validation rule strings keyed by raw payload field name.

        Most filters return a single key (the same as ``self.name``).
        Range filters return two keys (``<name>_min`` /
        ``<name>_max``). The full set of returned keys must match
        what ``parse()`` reads from the payload, so the FormRequest
        validates exactly the keys the filter consumes.
        """

    @abstractmethod
    def parse(self, payload: Dict[str, Any]) -> Any:
        """Extract + canonicalize this filter's value from the payload.

        Returns ``None`` when the filter is not active for this
        request (no value supplied, empty list, blank string, etc.).
        Returning ``None`` keeps the filter from contributing to the
        WHERE clause AND from polluting the cache key.

        For active filters, return the canonical Python value the
        SQL renderer expects:

        * scalar filters → str / int / float / bool
        * multi-select  → ``list`` (deduped, sorted by the caller for cache stability)
        * range         → ``dict`` with ``"min"`` / ``"max"`` keys
        * structured    → ``dict`` (e.g. AttributesFilter's per-attribute selections)
        """

    @abstractmethod
    def where_sql(
        self, value: Any, *, ctx: Any = None
    ) -> Tuple[str, List[Any]]:
        """Render this filter as SQL.

        Returns a ``(sql_fragment, params)`` pair. The fragment is a
        self-contained boolean expression suitable for joining into
        a WHERE clause with ``AND``; ``params`` are positional
        values for ``%s`` placeholders inside the fragment.

        ``ctx`` is an opaque, app-defined SQL-alias context
        (typically a frozen dataclass carrying expressions like
        ``product.id`` vs ``p.id``). Concrete subclasses tighten
        the type hint at the override site.
        """

    # ── Default implementations ─────────────────────────────────────

    def cache_key(self, value: Any) -> str:
        """Stable string fragment used by ``FilterSet.cache_key``.

        Override only if the default ``_serialize_value`` doesn't
        produce a deterministic result (e.g. case-insensitive strings
        where ``"Sony"`` and ``"sony"`` should hash to the same key).
        """
        return f"{self.name}={self._serialize_value(value)}"

    def encode_value(self, value: Any) -> Dict[str, str]:
        """Render the canonical value back to URL-friendly query params.

        Returns ``{payload_key: string}`` mapping — typically
        ``{self.name: <serialised value>}``. Range filters override
        to emit two keys (``price_min`` + ``price_max``); structured
        filters (AttributesFilter) override to emit JSON-encoded
        strings.

        Round-trip contract: ``parse(encode_value(value))`` MUST
        produce a value that ``cache_key`` agrees is identical.
        """
        return {self.name: self._serialize_value(value)}

    def describe(self) -> Dict[str, Any]:
        """Return a JSON-serialisable spec for this filter.

        Used by ``FilterSet.describe()`` to build the wizard /
        OpenAPI / docs payload. Override only when a filter needs
        to expose extra metadata beyond the standard fields (e.g.
        boolean flags, default values).
        """
        return {
            "name": self.name,
            "label": self.label or self.name.replace("_", " ").title(),
            "description": self.description,
            "ui_control": self.ui_control,
            "group": self.group,
            "options_source": self.options_source,
            "requires": list(self.requires),
            "payload_keys": sorted(self.validation_rules().keys()),
            "rules": self.validation_rules(),
        }

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"

    # ── Protected helpers shared by subclasses ──────────────────────

    @staticmethod
    def _serialize_value(value: Any) -> str:
        """Deterministic string for any canonical filter value.

        * Lists are sorted before serialisation so ``[a, b]`` and
          ``[b, a]`` produce the same key — order MUST NOT change
          cache identity for set-membership filters.
        * Dicts are sorted by key for the same reason.
        * Strings are stripped (leading/trailing whitespace can't
          change identity in any filter we ship).
        """
        if value is None:
            return ""
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (list, tuple, set)):
            items = sorted(str(x).strip() for x in value if str(x).strip())
            return ",".join(items)
        if isinstance(value, dict):
            parts = [
                f"{k}={Filter._serialize_value(value[k])}"
                for k in sorted(value.keys())
            ]
            return "{" + ";".join(parts) + "}"
        return str(value).strip()

    @staticmethod
    def _parse_csv_or_json(raw: Any) -> List[str]:
        """Coerce a CSV string or JSON array into a deduped, sorted list.

        Centralises the pattern that used to exist as ``_csv_or_json``
        in three different services. Sorted output guarantees cache-
        key stability regardless of how the storefront serialised
        the array.
        """
        if raw is None or raw == "":
            return []
        if isinstance(raw, list):
            cleaned = [str(x).strip() for x in raw if str(x).strip()]
        elif isinstance(raw, str):
            text = raw.strip()
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        cleaned = [str(x).strip() for x in parsed if str(x).strip()]
                    else:
                        cleaned = []
                except (ValueError, TypeError):
                    cleaned = []
            else:
                cleaned = [x.strip() for x in text.split(",") if x.strip()]
        else:
            cleaned = []
        # Dedupe + sort. Sorting guarantees identical filter values
        # produce identical cache keys regardless of input order, and
        # also makes parameter lists stable in tests.
        return sorted(set(cleaned))

    @staticmethod
    def _truthy(raw: Any) -> bool:
        """Accept the storefront's truthy variants ("true", "1", True)."""
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            return raw.strip().lower() in ("true", "1", "yes", "on")
        return False


__all__ = [
    "FILTER_GROUP_AVAILABILITY",
    "FILTER_GROUP_BRAND",
    "FILTER_GROUP_MARKETPLACE",
    "FILTER_GROUP_PRICE",
    "FILTER_GROUP_QUALITY",
    "FILTER_GROUP_SCOPE",
    "FILTER_GROUP_SPECS",
    "Filter",
    "UI_CONTROL_ATTRIBUTE_MATRIX",
    "UI_CONTROL_CHECKBOX_LIST",
    "UI_CONTROL_HIDDEN",
    "UI_CONTROL_NUMERIC_INPUT",
    "UI_CONTROL_RADIO_LIST",
    "UI_CONTROL_RANGE_SLIDER",
    "UI_CONTROL_TEXT_INPUT",
    "UI_CONTROL_TOGGLE",
]
