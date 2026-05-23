"""Single source of truth for shopper-facing variation attribute keys.

These are the ``attribute_definition.key`` values that:

  * Amazon's PTD schemas declare as variation axes for products that
    come in multiple SKUs (color, size, style, …).
  * Are universally shopper-facing — i.e. a customer narrowing the
    listing grid wants to filter by them, regardless of category.

Three consumers read this list and they must agree, otherwise an
attribute can end up declared as a variation in one layer but not
the other — producing the failure mode we hit before
(``color`` flagged is_variation in ptd_attribute_map but wired as
``ui_control='text'`` because the seeder and the facet repo had
divergent sets):

  1. ``services/app/commands/setup/SeedAttributesCommand.py`` —
     decides the global ``attribute_definition.ui_control`` at seed
     time (colour family → ``color`` swatch, everything else here
     → ``select`` pill group) and stamps ``is_filterable=true`` on
     the attribute_definition row.

  2. ``api/app/repositories/FacetQueryRepository.load_filterable_attrs``
     — when the PTD signal is missing for a niche category, these
     keys earn a facet slot as long as the data supports it
     (coverage threshold still applies).

  3. ``api/app/repositories/FacetQueryRepository._SHOPPER_HOSTILE_KEYS``
     is the inverse list (Amazon plumbing that's filterable=true by
     the heuristic but useless to shoppers). They must not overlap.

Adding a key here means: it'll be considered shopper-facing
everywhere. Removing one is a coordinated change with the seeder
data and live database — read the call sites first.
"""

from __future__ import annotations

# Colour-family keys take the round-swatch UI (``ui_control='color'``).
# Kept as a separate set rather than a tag because the storefront's
# ``SmartFilterSidebar`` keys off the literal ``ui_control`` value
# to switch between renderers — see ``ColorSwatchFilter``.
COLOR_SWATCH_KEYS: frozenset[str] = frozenset({
    "color",
    "color_name",
    "colour",
    "colour_name",
})

# Variation keys that ship as ``string`` from Amazon's PTD but are
# shoppable enums in practice — they earn the ``select`` /
# ``multi_select`` pill-group UI instead of free-text input.
# Mirrors Amazon's variation set from
# ``SeedAttributesCommand.is_variation_field`` minus the colour
# family (those go to the swatch branch).
VARIATION_SELECT_KEYS: frozenset[str] = frozenset({
    "size",
    "size_name",
    "style",
    "style_name",
    "material",
    "material_type",
    "pattern",
    "pattern_name",
    "flavor",
    "scent",
    "item_shape",
    "configuration",
    "handle",
})

# The union — every shopper-facing variation key, regardless of UI.
# Facet eligibility / seeder ``is_variation`` flag both read this
# set, so the two layers can never drift.
ALL_VARIATION_KEYS: frozenset[str] = COLOR_SWATCH_KEYS | VARIATION_SELECT_KEYS
