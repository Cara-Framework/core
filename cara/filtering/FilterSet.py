"""Composable bundle of ``Filter`` instances.

A ``FilterSet`` is the unit of consumption. Repos take a set,
HTTP layers take the same set, caches key off the same set's
serialisation. Adding a new filter dimension is two lines: write
the ``Filter`` subclass and append it to the relevant set.

The set is immutable — ``with_`` / ``without`` return new
instances, so callers can derive variants (e.g. "all filters
except brand_slugs" for the brand-facet query) without mutating
the canonical set.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from .Filter import Filter


class FilterSet:
    """An ordered, name-unique bundle of ``Filter`` instances."""

    def __init__(self, filters: Iterable[Filter]) -> None:
        self._filters: List[Filter] = list(filters)

        seen: Dict[str, Filter] = {}
        for f in self._filters:
            if not f.name:
                raise ValueError(
                    f"Filter {f.__class__.__name__!r} has no ``name`` attribute"
                )
            if f.name in seen:
                raise ValueError(
                    f"Duplicate filter name {f.name!r} in FilterSet "
                    f"({seen[f.name].__class__.__name__} vs "
                    f"{f.__class__.__name__})"
                )
            seen[f.name] = f
        self._by_name: Dict[str, Filter] = seen

    # ── Introspection ───────────────────────────────────────────────

    def __iter__(self):
        return iter(self._filters)

    def __len__(self) -> int:
        return len(self._filters)

    def __contains__(self, name: str) -> bool:
        return name in self._by_name

    def get(self, name: str) -> Filter:
        """Return the named filter or raise ``KeyError``."""
        return self._by_name[name]

    def names(self) -> List[str]:
        """Filter names in declaration order."""
        return [f.name for f in self._filters]

    # ── Composition ────────────────────────────────────────────────

    def with_(self, *filters: Filter) -> "FilterSet":
        """Return a new set with extra filters appended.

        Caller can override an existing filter by passing one with
        the same name — the previous instance is dropped, the new
        one takes its position. Used for endpoint-specific tweaks
        (e.g. an admin endpoint that wants a stricter price range).
        """
        replaced = {f.name: f for f in filters}
        merged: List[Filter] = []
        for existing in self._filters:
            merged.append(replaced.pop(existing.name, existing))
        merged.extend(replaced.values())
        return FilterSet(merged)

    def without(self, *names: str) -> "FilterSet":
        """Return a new set with the named filters removed.

        Used for self-skip facet queries — when computing options
        for the brand facet, the underlying candidate query should
        apply every filter EXCEPT brand_slugs (otherwise selecting
        a brand collapses the facet to that brand's own count).
        """
        drop = set(names)
        return FilterSet(f for f in self._filters if f.name not in drop)

    # ── Parsing & validation ───────────────────────────────────────

    def validation_rules(self) -> Dict[str, str]:
        """Merged Cara validation rules for every filter in the set.

        Caller wires this into a FormRequest's ``rules()`` so the
        rules and the filter consumers stay in lockstep — there's
        no opportunity for a typo (``attributes_raw`` vs
        ``attributes``) to silently no-op a filter.
        """
        merged: Dict[str, str] = {}
        for f in self._filters:
            for key, rule in f.validation_rules().items():
                if key in merged:
                    raise ValueError(
                        f"Filter {f.name!r} declares validation key "
                        f"{key!r} which conflicts with another filter"
                    )
                merged[key] = rule
        return merged

    def parse(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Extract the canonical value for each filter that has one.

        Returns ``{filter_name: canonical_value}`` for active filters
        only. Inactive filters (no input or empty input) are not in
        the returned dict — that's how downstream consumers
        (where_clauses, cache_key) skip them uniformly.
        """
        out: Dict[str, Any] = {}
        for f in self._filters:
            value = f.parse(payload or {})
            if value is None:
                continue
            out[f.name] = value
        return out

    # ── SQL rendering ──────────────────────────────────────────────

    def where_clauses(
        self,
        parsed: Dict[str, Any],
        *,
        ctx: Any = None,
    ) -> Tuple[List[str], List[Any]]:
        """Return ``(sqls, params)`` lists for joining with ``AND``.

        ``parsed`` is the dict returned by ``parse()``. Filters not
        in ``parsed`` are skipped — the inactive-filter contract.
        """
        sqls: List[str] = []
        params: List[Any] = []
        for f in self._filters:
            value = parsed.get(f.name)
            if value is None:
                continue
            sql, p = f.where_sql(value, ctx=ctx)
            if not sql:
                continue
            sqls.append(sql)
            params.extend(p)
        return sqls, params

    def apply_to_builder(
        self,
        builder: Any,
        parsed: Dict[str, Any],
        *,
        ctx: Any = None,
    ) -> Any:
        """Apply each active filter to a Cara QueryBuilder via ``where_raw``.

        Convenience for ORM-driven repos. Equivalent to looping
        ``where_clauses`` and calling ``builder.where_raw(sql,
        params)`` for each, but reads cleaner at the call site.
        """
        for f in self._filters:
            value = parsed.get(f.name)
            if value is None:
                continue
            sql, p = f.where_sql(value, ctx=ctx)
            if sql:
                builder = builder.where_raw(sql, p)
        return builder

    # ── Cache identity ─────────────────────────────────────────────

    def cache_key(self, parsed: Dict[str, Any]) -> str:
        """Deterministic, content-addressed key fragment.

        Two filter parses that differ only in payload-key insertion
        order produce identical fragments — important so a cache
        hit doesn't depend on which order the storefront serialised
        its query string.
        """
        parts = [
            f.cache_key(parsed[f.name])
            for f in self._filters
            if f.name in parsed
        ]
        if not parts:
            return "no_filters"
        return "|".join(sorted(parts))

    # ── URL codec ──────────────────────────────────────────────────

    def encode(self, parsed: Dict[str, Any]) -> Dict[str, str]:
        """Render the parsed state back to query-string-friendly params.

        Returns ``{payload_key: string}`` ready to be passed to
        ``urlencode`` or ``http.get(..., params=...)``. Each filter
        owns the format of its keys via ``encode_value``.

        Round-trip contract: ``self.parse(self.encode(parsed))``
        produces a parsed dict whose ``cache_key`` is identical to
        ``parsed``'s. This is the property that lets the storefront
        and api agree on filter identity even when state moves
        through URLs (shared links, bookmarks, deep-linked wizard
        steps).
        """
        out: Dict[str, str] = {}
        for f in self._filters:
            if f.name not in parsed:
                continue
            for key, encoded in f.encode_value(parsed[f.name]).items():
                if encoded == "" or encoded is None:
                    continue
                out[key] = encoded
        return out

    def decode(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a raw query-string dict back into canonical parsed form.

        Convenience alias for ``parse`` named to mirror ``encode``.
        Frontends use this seam to say ``set.decode(request.query)``
        without first thinking about whether the raw dict is
        "payload" or "query".
        """
        return self.parse(query)

    # ── Introspection / wizard schema ───────────────────────────────

    def describe(self) -> Dict[str, Any]:
        """JSON-serialisable spec for the entire set.

        Frontend wizards / docs generators consume this to render
        controls dynamically — every filter is self-describing
        (label, control type, group, dependencies), so the wizard
        doesn't need filter-specific code.

        The returned shape::

            {
                "filters": [<Filter.describe()>, ...],
                "groups":  ["scope", "brand", ...],   # ordered
                "names":   ["search", "category_id", ...],
                "rules":   {<merged validation rules>},
            }

        ``groups`` is the ordered list of unique groups across the
        filter set in declaration order — frontends use it to order
        the wizard steps.
        """
        descriptions: List[Dict[str, Any]] = [f.describe() for f in self._filters]
        groups: List[str] = []
        seen: set = set()
        for d in descriptions:
            g = d.get("group")
            if g and g not in seen:
                groups.append(g)
                seen.add(g)
        return {
            "filters": descriptions,
            "groups": groups,
            "names": self.names(),
            "rules": self.validation_rules(),
        }


__all__ = ["FilterSet"]
