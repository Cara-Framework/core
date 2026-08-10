"""
HasAttributes Concern

Single Responsibility: Handle all attribute-related operations for Eloquent models.
Extracted from Model.py to follow SRP and DRY principles.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any

from cara.support import Collection, json_dumps

_logger = logging.getLogger("cara.eloquent.attributes")


class HasAttributes:
    """
    Mixin for handling model attributes, casting, and serialization.

    This concern handles:
    - Attribute getting/setting with casts
    - Mass assignment protection
    - Serialization (to_array, to_json)
    - Hidden/visible attribute management
    - Dirty attribute tracking
    """

    # These will be set by the actual Model class
    __fillable__ = ["*"]
    __guarded__ = []
    __casts__ = {}
    __dates__ = []
    __hidden__ = []
    __visible__ = []
    __appends__ = []

    def __init__(self, **kwargs):
        # Visibility/serialization caches. The attribute VALUES themselves are
        # owned by ``Model`` (``__attributes__`` / ``__original_attributes__``
        # / ``__dirty_attributes__``) — this concern once kept a second
        # ``_attributes`` / ``_original`` / ``_changes`` / ``_loaded`` set that
        # ``Model`` never read, and every method that wrote it lost the value.
        self.__dict__["_hidden_cache"] = set()
        self.__dict__["_visible_cache"] = set()
        self.__dict__["_appends_cache"] = set()
        self.__dict__["_without_timestamps"] = False

        # Fill with provided attributes
        if kwargs:
            self.fill(kwargs)

    # ===== Attribute Access =====
    #
    # ``__getattr__``, ``__setattr__``, ``get_raw_attribute`` and
    # ``fill_original`` used to live here too. All four were shadowed by
    # ``Model`` — the concern's ONLY consumer — so their bodies never ran, and
    # all four addressed the retired parallel store. Keeping shadowed copies
    # around is not harmless: this concern's ``__setattr__`` delegated to
    # ``set_attribute``, so once ``set_attribute`` became the thin wrapper over
    # the real write door the pair formed an infinite recursion for any class
    # that mixed the concern in without ``Model``'s ``__setattr__``. Dead code
    # cannot be trusted to stay dead.

    def get_attribute(self, attribute: str) -> Any:
        """Get an attribute value with casting."""
        value = self.get_raw_attribute(attribute)

        if value is not None:
            return self._cast_attribute(attribute, value)

        return value

    def set_attribute(self, attribute: str, value: Any) -> None:
        """Set an attribute value — the same write ``model.attr = value`` performs.

        ``Model.__setattr__`` is the single write door: it applies ``@mutator``
        methods, the cast registry and date conversion, then records the value
        in ``__dirty_attributes__`` so ``save()`` can see it.

        Pre-fix this method kept its OWN parallel store — ``_attributes`` /
        ``_original`` / ``_changes`` — which ``Model`` never reads. The write
        silently went nowhere: ``m.set_attribute("foo", 5)`` left ``m.foo``
        missing, ``m.get_attribute("foo")`` answering ``None`` and ``save()``
        with nothing to persist. A public ORM setter that loses data is worse
        than one that raises, because nothing anywhere reports a failure.
        """
        setattr(self, attribute, value)

    # ===== Mass Assignment =====

    def fill(self, attributes: dict[str, Any]) -> HasAttributes:
        """Fill model with attributes respecting mass assignment protection."""
        filtered_attributes = self.filter_mass_assignment(attributes)

        for key, value in filtered_attributes.items():
            self.set_attribute(key, value)

        return self

    @classmethod
    def filter_mass_assignment(cls, attributes: dict[str, Any]) -> dict[str, Any]:
        """Filter attributes through mass assignment protection."""
        return cls.filter_guarded(cls.filter_fillable(attributes))

    @classmethod
    def filter_fillable(cls, attributes: dict[str, Any]) -> dict[str, Any]:
        """Filter attributes through fillable whitelist."""
        if "*" in cls.__fillable__:
            return attributes

        dropped = [key for key in attributes if key not in cls.__fillable__]
        if dropped:
            try:
                from cara.facades import Log

                model_name = cls.__name__ if hasattr(cls, "__name__") else str(cls)
                Log.warning(
                    "[MassAssignment] %s: dropped non-fillable keys %s",
                    model_name,
                    dropped,
                )
            except Exception:
                _logger.debug("mass assignment warning log failed", exc_info=True)

        return {
            key: value for key, value in attributes.items() if key in cls.__fillable__
        }

    @classmethod
    def filter_guarded(cls, attributes: dict[str, Any]) -> dict[str, Any]:
        """Filter attributes through guarded blacklist."""
        if "*" in cls.__guarded__:
            return {}

        return {
            key: value for key, value in attributes.items() if key not in cls.__guarded__
        }

    # ===== Serialization =====

    def to_array(self, exclude=None, include=None) -> dict[str, Any]:
        """
        Convert model to array with Laravel-style visibility control.

        Args:
            exclude: Fields to exclude from serialization
            include: Fields to include in serialization (overrides visibility)

        Returns:
            Dictionary representation of the model
        """
        # Get base attributes
        data = self._get_base_attributes()

        # Apply visibility rules
        data = self._apply_visibility_rules(data, exclude, include)

        # Apply casts and format dates
        data = self._apply_casts_and_dates(data)

        # Add relationships
        data.update(self._serialize_relations())

        # Add appended attributes
        data.update(self._serialize_appends())

        return data

    def to_json(self, **kwargs) -> str:
        """Convert to JSON through ``cara.support.JsonEncoding``.

        The encoder is the shared wire rule, so ``allow_nan`` is off and
        an unknown object raises instead of arriving at the client as
        ``"<Order object at 0x10c3f2a10>"`` behind a 200 — which is what
        the previous bare ``default=str`` did.

        **Two honest caveats, because the first version of this docstring
        claimed a fix it does not deliver.** ``Model`` overrides this
        method, so no ORM model reaches this body at all; and
        ``Model.serialize`` rewrites every ``Decimal`` as ``float`` while
        building the dict, so ``to_array()`` has already spent the
        precision before any encoder runs. Model money therefore leaves
        as a JSON number while a hand-built payload leaves as an exact
        string. That divergence, and why closing it is a coordinated
        product change rather than a framework one, is written down in
        ``cara/support/JsonEncoding.py``.
        """
        return json_dumps(self.to_array(), **kwargs)

    # ===== Visibility Control =====

    def make_hidden(self, *attributes) -> HasAttributes:
        """Hide attributes from serialization."""
        clone = self._clone_for_visibility()
        clone._hidden_cache.update(attributes)
        return clone

    def make_visible(self, *attributes) -> HasAttributes:
        """Make attributes visible in serialization."""
        clone = self._clone_for_visibility()
        clone._visible_cache.update(attributes)
        # Remove from hidden cache if present
        clone._hidden_cache.difference_update(attributes)
        return clone

    def without_timestamps(self) -> HasAttributes:
        """Exclude timestamps from serialization."""
        clone = self._clone_for_visibility()
        clone._without_timestamps = True
        return clone

    def append(self, *attributes) -> HasAttributes:
        """Add computed attributes to serialization."""
        clone = self._clone_for_visibility()
        clone._appends_cache.update(attributes)
        return clone

    def only(self, *attributes) -> dict[str, Any]:
        """Get only specified attributes using Collection.only()."""
        data = self.to_array()
        return Collection(data).only(*attributes)

    # ===== Dirty Tracking =====

    def is_dirty(self) -> bool:
        """Check if model has unsaved changes."""
        return bool(getattr(self, "_changes", {}))

    def get_dirty_attributes(self) -> dict[str, Any]:
        """Get all dirty attributes."""
        return getattr(self, "_changes", {}).copy()

    def get_original(self, key: str | None = None) -> Any:
        """Get original attribute value(s)."""
        original = getattr(self, "_original", {})

        if key is None:
            return original.copy()

        return original.get(key)

    # ===== Internal Helper Methods =====

    def _get_base_attributes(self) -> dict[str, Any]:
        """Get base model attributes."""
        return getattr(self, "_attributes", {}).copy()

    def _apply_visibility_rules(
        self, data: dict[str, Any], exclude=None, include=None
    ) -> dict[str, Any]:
        """Apply visibility rules to data."""
        # Handle include parameter (highest priority)
        if include:
            include_set = set(include)
            data = {k: v for k, v in data.items() if k in include_set}

        # Handle exclude parameter
        if exclude:
            exclude_set = set(exclude)
            data = {k: v for k, v in data.items() if k not in exclude_set}

        # Apply model-level hidden attributes
        hidden = set(self.__hidden__) | getattr(self, "_hidden_cache", set())
        if hidden:
            data = {k: v for k, v in data.items() if k not in hidden}

        # Apply model-level visible attributes (if defined)
        visible = set(self.__visible__) | getattr(self, "_visible_cache", set())
        if visible:
            data = {k: v for k, v in data.items() if k in visible}

        # Handle timestamps exclusion
        if getattr(self, "_without_timestamps", False):
            timestamp_fields = {self.date_created_at, self.date_updated_at}
            data = {k: v for k, v in data.items() if k not in timestamp_fields}

        return data

    def _apply_casts_and_dates(self, data: dict[str, Any]) -> dict[str, Any]:
        """Apply casts and date formatting to data."""
        result = {}

        for key, value in data.items():
            if value is not None:
                # Apply timezone conversion for dates first
                if self._is_date_attribute(key):
                    value = self._format_date_for_api(key, value)
                else:
                    # Apply casting for non-date attributes
                    value = self._cast_attribute(key, value)

                result[key] = value
            else:
                result[key] = value

        return result

    def _is_date_attribute(self, attribute: str) -> bool:
        """Check if attribute is a date field."""
        date_fields = getattr(self, "__dates__", [])
        timestamp_fields = ["created_at", "updated_at", "deleted_at"]

        # Check if it's in dates array or timestamp fields
        return attribute in date_fields or attribute in timestamp_fields

    def _format_date_for_api(self, attribute: str, value: Any) -> str:
        """Format date attribute for API response with timezone conversion."""
        try:
            from cara.eloquent.utils.DateManager import DateManager

            # Get user timezone from config
            user_timezone = self._get_user_timezone()

            # Convert UTC database value to user timezone
            return DateManager.format_for_api(value, user_timezone) or value
        except ImportError:
            # Fallback if DateManager not available
            return str(value) if value else value

    def _get_user_timezone(self) -> str:
        """Get user timezone from config or request context."""
        try:
            from cara.configuration import config

            return config("app.timezone", "UTC")
        except Exception:
            return "UTC"

    def _serialize_relations(self) -> dict[str, Any]:
        """Serialize model relationships."""
        # This will be implemented in HasRelationships concern
        return {}

    def _serialize_appends(self) -> dict[str, Any]:
        """Serialize appended attributes."""
        result = {}
        appends = set(self.__appends__) | getattr(self, "_appends_cache", set())

        for attribute in appends:
            # Try to get accessor value
            with contextlib.suppress(AttributeError):
                result[attribute] = getattr(self, attribute)

        return result

    def _clone_for_visibility(self) -> HasAttributes:
        """Create a shallow clone for visibility modifications."""
        import copy

        clone = copy.copy(self)
        clone.__dict__["_hidden_cache"] = getattr(self, "_hidden_cache", set()).copy()
        clone.__dict__["_visible_cache"] = getattr(self, "_visible_cache", set()).copy()
        clone.__dict__["_appends_cache"] = getattr(self, "_appends_cache", set()).copy()
        return clone

    # ===== Casting Support =====

    def _cast_attribute(self, attribute: str, value: Any) -> Any:
        """Cast attribute value using registered casts."""
        if attribute in self.__casts__:
            cast_type = self.__casts__[attribute]
            return self.cast_value(attribute, value, cast_type)

        return value

    def _set_cast_attribute(self, attribute: str, value: Any) -> Any:
        """Cast value for setting attribute."""
        if attribute in self.__casts__:
            cast_type = self.__casts__[attribute]
            # Get cast instance and use set method
            from cara.eloquent.casts import cast_registry

            cast_instance = cast_registry.get_cast_instance(cast_type)
            if cast_instance:
                return cast_instance.set(value)

        return value

    @classmethod
    def cast_value(cls, attribute: str, value: Any, cast_type: str | None = None) -> Any:
        """Cast a value using the specified cast type."""
        if cast_type is None and attribute in cls.__casts__:
            cast_type = cls.__casts__[attribute]

        if cast_type:
            from cara.eloquent.casts import cast_registry

            cast_instance = cast_registry.get_cast_instance(cast_type)
            if cast_instance:
                return cast_instance.get(value)

        return value
