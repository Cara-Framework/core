"""Composable filter framework — Laravel-inspired filter / sort / paginate primitives.

Generic, domain-free building blocks. Apps compose concrete
filters / sorters into ``FilterSet`` / ``SortRegistry`` instances
and pass an opaque ``ctx`` (typically a small dataclass with
SQL-alias expressions) so the same filter renders against any
table layout.

Usage:
    from cara.filtering import (
        Filter, FilterSet, FilterPipeline, pipeline,
        Sorter, SortRegistry,
        RelationSet, relations,
        FilteredFormRequest, PAGING_RULES,
        UI_CONTROL_TOGGLE, FILTER_GROUP_PRICE,
    )

App side defines its own ``FilterContext`` (e.g.
``app/filtering/FilterContext.py``) carrying domain-specific column
aliases. Concrete filter subclasses tighten the ``ctx`` type hint at
the override site.
"""

from .Filter import (
    FILTER_GROUP_AVAILABILITY,
    FILTER_GROUP_BRAND,
    FILTER_GROUP_MARKETPLACE,
    FILTER_GROUP_PRICE,
    FILTER_GROUP_QUALITY,
    FILTER_GROUP_SCOPE,
    FILTER_GROUP_SPECS,
    UI_CONTROL_ATTRIBUTE_MATRIX,
    UI_CONTROL_CHECKBOX_LIST,
    UI_CONTROL_HIDDEN,
    UI_CONTROL_NUMERIC_INPUT,
    UI_CONTROL_RADIO_LIST,
    UI_CONTROL_RANGE_SLIDER,
    UI_CONTROL_TEXT_INPUT,
    UI_CONTROL_TOGGLE,
    Filter,
)
from .FilterSet import FilterSet
from .Pipeline import FilterPipeline, pipeline
from .Relations import RelationSet, relations
from .Request import FilteredFormRequest, PAGING_RULES
from .Sorter import Sorter, SortRegistry

__all__ = [
    "FILTER_GROUP_AVAILABILITY",
    "FILTER_GROUP_BRAND",
    "FILTER_GROUP_MARKETPLACE",
    "FILTER_GROUP_PRICE",
    "FILTER_GROUP_QUALITY",
    "FILTER_GROUP_SCOPE",
    "FILTER_GROUP_SPECS",
    "Filter",
    "FilterPipeline",
    "FilterSet",
    "FilteredFormRequest",
    "PAGING_RULES",
    "RelationSet",
    "Sorter",
    "SortRegistry",
    "UI_CONTROL_ATTRIBUTE_MATRIX",
    "UI_CONTROL_CHECKBOX_LIST",
    "UI_CONTROL_HIDDEN",
    "UI_CONTROL_NUMERIC_INPUT",
    "UI_CONTROL_RADIO_LIST",
    "UI_CONTROL_RANGE_SLIDER",
    "UI_CONTROL_TEXT_INPUT",
    "UI_CONTROL_TOGGLE",
    "pipeline",
    "relations",
]
