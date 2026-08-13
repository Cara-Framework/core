"""
Eloquent Model - Laravel-style ORM Model.

This module provides the base Model class for Cara's ORM, allowing static-like
method calls (e.g., User.first()) through the ModelMeta metaclass.

Features:
- Attribute management with fillable/guarded/hidden
- Type casting with automatic and custom casts
- Relationship loading and management
- Event lifecycle (creating, updating, deleting, etc)
- Query builder integration with scopes
- Model observers for event-driven logic
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

# Import cast system
from ..casts.ArrayCast import ArrayCast
from ..casts.BoolCast import BoolCast
from ..casts.CollectionCast import CollectionCast
from ..casts.DateCast import DateCast
from ..casts.DateTimeCast import DateTimeCast
from ..casts.DecimalCast import DecimalCast
from ..casts.EmailCast import EmailCast
from ..casts.EncryptedCast import EncryptedCast
from ..casts.EncryptedJsonCast import EncryptedJsonCast
from ..casts.FloatCast import FloatCast
from ..casts.HashCast import HashCast
from ..casts.IntCast import IntCast
from ..casts.JsonCast import JsonCast
from ..casts.TimestampCast import TimestampCast
from ..casts.URLCast import URLCast
from ..casts.UUIDCast import UUIDCast

# Import concerns for clean architecture
from ..concerns.HasAttributes import HasAttributes
from ..concerns.HasRelationships import HasRelationships
from ..concerns.HasTimestamps import HasTimestamps
from ..observers import ObservesEvents
from ..query import QueryBuilder
from ..scopes import MakesTimestamps
from . import _ModelData, _ModelPersistence, _ModelPresentation, _ModelRelations
from .ModelMeta import ModelMeta

_logger = logging.getLogger("cara.eloquent.models")


class Model(
    HasAttributes,
    HasRelationships,
    MakesTimestamps,
    ObservesEvents,
    HasTimestamps,
    metaclass=ModelMeta,
):
    """Laravel-style ORM Model class.

    Provides a complete ORM with attribute management, relationships, timestamps,
    events, and query builder integration.

    Class Attributes:
        __fillable__: List of attributes that can be mass-assigned
        __guarded__: List of attributes protected from mass-assignment
        __hidden__: List of attributes hidden from serialization
        __visible__: List of visible attributes (if set, only these are visible)
        __casts__: Dict of attribute names to cast types
        __dates__: List of attributes treated as dates
        __table__: The database table name (auto-derived from class name if not set)
        __connection__: The database connection name
        __primary_key__: The primary key column name
        __primary_key_type__: The PHP/JS type of the primary key
        __timestamps__: Whether to manage created_at/updated_at timestamps
        __timezone__: The timezone for date attributes
        __with__: Relations to eager load by default
        __observers__: Model observer configurations
    """

    # Mass assignment and serialization
    __fillable__: list[str] = ["*"]
    __guarded__: list[str] = []
    __hidden__: list[str] = []
    __visible__: list[str] = []
    __appends__: list[str] = []

    # Database configuration
    __table__: str | None = None
    __connection__: str = "default"
    __resolved_connection__: Any | None = None
    __primary_key__: str = "id"
    __primary_key_type__: str = "int"
    __selects__: list[str] = []

    # Attribute casting and dates
    __casts__: dict[str, str | type] = {}
    __dates__: list[str] = []
    __cast_map__: dict[str, type] = {}
    __internal_cast_map__: dict[str, type] = {}

    # Timestamps
    __timestamps__: bool = True
    __timezone__: str = "UTC"
    date_created_at: str = "created_at"
    date_updated_at: str = "updated_at"

    # Relationships and eager loading
    __with__: tuple = ()
    __relationship_hidden__: dict[str, list[str]] = {}

    # Events and observers
    __observers__: dict[str, Any] = {}
    __has_events__: bool = True

    # Query execution
    __dry__: bool = False
    __force_update__: bool = False

    # Internal state
    _booted: bool = False
    _scopes: dict[type, dict[str, Callable]] = {}

    # Strict lazy-load guard (Laravel's ``Model::preventLazyLoading``).
    # OFF by default — a total no-op for every existing query/test unless
    # explicitly enabled via ``Model.prevent_lazy_loading()``. When on, an
    # un-eager-loaded relationship accessed on a COLLECTION-hydrated model
    # (where N+1 actually bites) raises ``LazyLoadingViolation`` instead of
    # silently firing a per-row query. Single-instance finds
    # (``find()``/``first()``) are never flagged.
    _prevent_lazy_loading: bool = False

    builder: QueryBuilder
    """Passthrough delegates to QueryBuilder for query method calls."""
    __passthrough__ = set(
        (
            "add_select",
            "aggregate",
            "all",
            "avg",
            "between",
            "bulk_create",
            "chunk",
            "count",
            "decrement",
            "delete",
            "distinct",
            "doesnt_exist",
            "doesnt_have",
            "exists",
            "find_or",
            "find_or_404",
            "find_or_fail",
            "first_or_fail",
            "first",
            "first_where",
            "first_or_create",
            "force_update",
            # SoftDeleteScope installs these as query-builder macros during
            # model boot. They remain part of the documented static model
            # surface (``Product.with_trashed()``), so the metaclass must let
            # them reach the builder like ordinary query methods.
            "with_trashed",
            "only_trashed",
            "restore",
            "force_delete",
            "force_delete_query",
            "from_",
            "from_raw",
            "get",
            "get_table_schema",
            "group_by_raw",
            "group_by",
            "has",
            "having",
            "having_raw",
            "increment",
            "in_random_order",
            "join_on",
            "join",
            "joins",
            "last",
            "left_join",
            "limit",
            "lock_for_update",
            "make_lock",
            "max",
            "min",
            "new_from_builder",
            "new",
            "not_between",
            "offset",
            "on",
            "or_where",
            "or_where_null",
            "order_by_raw",
            "order_by",
            "paginate",
            "cursor_paginate",
            "chunk_by_id",
            "lazy",
            "lazy_by_id",
            "union",
            "union_all",
            "right_join",
            "select_raw",
            "select",
            "set_global_scope",
            "set_schema",
            "shared_lock",
            "simple_paginate",
            "skip",
            "statement",
            "sum",
            "table_raw",
            "take",
            "to_qmark",
            "to_sql",
            "truncate",
            "update",
            "when",
            "where_between",
            "where_column",
            "where_date",
            "or_where_doesnt_have",
            "or_has",
            "or_where_has",
            "or_doesnt_have",
            "or_where_not_exists",
            "or_where_date",
            "where_exists",
            "where_from_builder",
            "where_has",
            "where_in",
            "where_json_contains",
            "where_json_doesnt_contain",
            "where_json_path",
            "or_where_json_contains",
            "where_like",
            "where_not_between",
            "where_not_in",
            "where_not_like",
            "where_not_null",
            "where_null",
            "where_raw",
            "without_global_scopes",
            "where",
            "where_doesnt_have",
            "with_",
            "with_count",
            "with_sum",
            "with_avg",
            "with_min",
            "with_max",
            "tap",
            "pipe",
            "transaction",
            "latest",
            "oldest",
            "value",
            "upsert",
            "cursor",
        )
    )

    __cast_map__ = {}

    __internal_cast_map__: dict[str, type] = {
        "bool": BoolCast,
        "json": JsonCast,
        "int": IntCast,
        "float": FloatCast,
        "date": DateCast,
        "decimal": DecimalCast,
        "datetime": DateTimeCast,
        "timestamp": TimestampCast,
        "array": ArrayCast,
        "encrypted": EncryptedCast,
        "encrypted_json": EncryptedJsonCast,
        "uuid": UUIDCast,
        "url": URLCast,
        "email": EmailCast,
        "hash": HashCast,
        "collection": CollectionCast,
    }

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a new Model instance.

        Args:
            **kwargs: Initial attribute values to set on the model
        """
        # Call parent constructors (including HasRelationships)
        super().__init__(**kwargs)

        # Initialize attribute storage
        self.__attributes__: dict[str, Any] = {}
        self.__original_attributes__: dict[str, Any] = {}
        self.__dirty_attributes__: dict[str, Any] = {}

        # Initialize appends if not already present
        if not hasattr(self, "__appends__"):
            self.__appends__ = []

        # Initialize relationships storage
        self._relations: dict[str, Any] = {}
        self._relationships: dict[str, Any] = {}
        self._global_scopes: dict[str, Any] = {}

        # Initialize model events cache
        self._model_events: dict[str, list[Callable]] | None = None

        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Bootstrap the model (register observers, etc)
        self.boot()

    get_primary_key = classmethod(_ModelPersistence._model_get_primary_key)

    get_primary_key_type = _ModelPersistence._model_get_primary_key_type

    get_primary_key_value = _ModelPersistence._model_get_primary_key_value

    get_foreign_key = _ModelPersistence._model_get_foreign_key

    # NOTE: ``query`` is defined later in this class as a ``@classmethod``
    # (Laravel parity — ``Model.query()``). The instance-level shadow that
    # used to live here was dead code and has been removed.

    get_builder = _ModelPersistence._model_get_builder

    get_selects = _ModelPersistence._model_get_selects

    get_columns = classmethod(_ModelPersistence._model_get_columns)

    get_connection_details = _ModelPersistence._model_get_connection_details

    boot = _ModelPersistence._model_boot

    append_passthrough = _ModelPersistence._model_append_passthrough

    _get_model_events = _ModelPersistence._model_get_model_events

    _fire_model_event = _ModelPersistence._model_fire_model_event

    save = _ModelPersistence._model_save

    delete = _ModelPersistence._model_delete

    _touch_parents = _ModelPersistence._model_touch_parents

    touch = _ModelPersistence._model_touch

    get_table_name = classmethod(_ModelPersistence._model_get_table_name)

    table = classmethod(_ModelPersistence._model_table)

    find = classmethod(_ModelPersistence._model_find)

    find_or_fail = classmethod(_ModelPersistence._model_find_or_fail)

    prevent_lazy_loading = classmethod(_ModelPersistence._model_prevent_lazy_loading)

    _mark_from_collection = _ModelPersistence._model_mark_from_collection

    _guard_against_lazy_load = _ModelPresentation._model_guard_against_lazy_load
    is_loaded = _ModelPresentation._model_is_loaded
    is_created = _ModelPresentation._model_is_created
    hydrate = classmethod(_ModelPresentation._model_hydrate)
    fill = _ModelPresentation._model_fill
    fill_original = _ModelPresentation._model_fill_original
    new_collection = classmethod(_ModelPresentation._model_new_collection)
    create = classmethod(_ModelPresentation._model_create)
    cast_value = classmethod(_ModelPresentation._model_cast_value)
    cast_values = classmethod(_ModelPresentation._model_cast_values)
    fresh = _ModelPresentation._model_fresh
    refresh = _ModelPresentation._model_refresh
    serialize = _ModelPresentation._model_serialize
    to_array = _ModelPresentation._model_to_array
    to_json = _ModelPresentation._model_to_json
    make_hidden = _ModelPresentation._model_make_hidden
    make_visible = _ModelPresentation._model_make_visible
    set_hidden = _ModelPresentation._model_set_hidden
    set_visible = _ModelPresentation._model_set_visible
    append = _ModelPresentation._model_append
    except_keys = _ModelPresentation._model_except_keys
    without_timestamps = _ModelPresentation._model_without_timestamps
    _clone_for_visibility = _ModelPresentation._model_clone_for_visibility
    first_or_create = classmethod(_ModelPresentation._model_first_or_create)

    first_or_new = classmethod(_ModelData._model_first_or_new)
    update_or_create = classmethod(_ModelData._model_update_or_create)
    truncate = classmethod(_ModelData._model_truncate)
    query = classmethod(_ModelData._model_query)
    relations_to_dict = _ModelData._model_relations_to_dict
    __getattr__ = _ModelData._model_getattr
    only = _ModelData._model_only
    __setattr__ = _ModelData._model_setattr
    get_raw_attribute = _ModelData._model_get_raw_attribute
    is_dirty = _ModelData._model_is_dirty
    is_clean = _ModelData._model_is_clean
    get_original = _ModelData._model_get_original
    get_dirty_attributes = _ModelData._model_get_dirty_attributes
    get_value = _ModelData._model_get_value
    get_dirty_value = _ModelData._model_get_dirty_value
    all_attributes = _ModelData._model_all_attributes
    delete_attribute = _ModelData._model_delete_attribute
    get_cast_map = _ModelData._model_get_cast_map
    _cast_attribute = _ModelData._model_cast_attribute
    __getitem__ = _ModelData._model_getitem
    get_dates = _ModelData._model_get_dates
    get_new_date = _ModelData._model_get_new_date
    get_new_datetime_string = _ModelData._model_get_new_datetime_string
    get_new_serialized_date = _ModelData._model_get_new_serialized_date

    _convert_date_to_utc_for_database = (
        _ModelRelations._model_convert_date_to_utc_for_database
    )
    _get_user_timezone = _ModelRelations._model_get_user_timezone
    set_appends = _ModelRelations._model_set_appends
    save_many = _ModelRelations._model_save_many
    detach_many = _ModelRelations._model_detach_many
    related = _ModelRelations._model_related
    get_related = _ModelRelations._model_get_related
    attach = _ModelRelations._model_attach
    detach = _ModelRelations._model_detach
    save_quietly = _ModelRelations._model_save_quietly
    delete_quietly = _ModelRelations._model_delete_quietly
    attach_related = _ModelRelations._model_attach_related
    filter_fillable = classmethod(_ModelRelations._model_filter_fillable)
    filter_mass_assignment = classmethod(_ModelRelations._model_filter_mass_assignment)
    filter_guarded = classmethod(_ModelRelations._model_filter_guarded)
    upsert = classmethod(_ModelRelations._model_upsert)


_ModelData._bind_model(Model)
_ModelPresentation._bind_model(Model)
_ModelPersistence._bind_model(Model)
_ModelRelations._bind_model(Model)
