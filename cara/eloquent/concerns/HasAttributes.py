"""
HasAttributes Concern

Single Responsibility: Handle all attribute-related operations for Eloquent models.
Extracted from Model.py to follow SRP and DRY principles.
"""

import json
from typing import Any, Dict, Optional

from cara.support.Collection import Collection


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
        # Initialize internal state
        self.__dict__["_attributes"] = {}
        self.__dict__["_original"] = {}
        self.__dict__["_changes"] = {}
        self.__dict__["_loaded"] = False
        self.__dict__["_hidden_cache"] = set()
        self.__dict__["_visible_cache"] = set()
        self.__dict__["_appends_cache"] = set()
        self.__dict__["_without_timestamps"] = False

        # Fill with provided attributes
        if kwargs:
            self.fill(kwargs)

    # ===== Attribute Access =====

    def __getattr__(self, attribute: str) -> Any:
        """Get attribute with automatic casting and accessor support."""
        # Check for accessor methods first
        accessor_method = f"get_{attribute}_attribute"
        if hasattr(self, accessor_method) and callable(getattr(self, accessor_method)):
            try:
                method = getattr(self, accessor_method)
                if callable(method):
                    return method(self.get_raw_attribute(attribute))
            except RecursionError:
                pass

        # Check for relationship
        if hasattr(self.__class__, attribute):
            attr = getattr(self.__class__, attribute)
            if hasattr(attr, "__call__") and hasattr(attr, "get_related"):
                return attr.__get__(self, self.__class__)

        # Get regular attribute
        return self.get_attribute(attribute)

    def __setattr__(self, attribute: str, value: Any) -> None:
        """Set attribute with automatic mutator support and casting."""
        # Check for mutator methods first
        mutator_method = f"set_{attribute}_attribute"
        if hasattr(self, mutator_method) and callable(getattr(self, mutator_method)):
            method = getattr(self, mutator_method)
            if callable(method):
                try:
                    method(value)
                    return
                except RecursionError:
                    pass

        # Set regular attribute
        self.set_attribute(attribute, value)

    def get_attribute(self, attribute: str) -> Any:
        """Get an attribute value with casting."""
        value = self.get_raw_attribute(attribute)

        if value is not None:
            return self._cast_attribute(attribute, value)

        return value

    def set_attribute(self, attribute: str, value: Any) -> None:
        """Set an attribute value with casting."""
        # Apply mutator casting if defined
        value = self._set_cast_attribute(attribute, value)

        # Store the value
        if not hasattr(self, "_attributes"):
            self.__dict__["_attributes"] = {}

        self._attributes[attribute] = value

        # Track changes
        if hasattr(self, "_original") and attribute in self._original:
            if self._original[attribute] != value:
                if not hasattr(self, "_changes"):
                    self.__dict__["_changes"] = {}
                self._changes[attribute] = value

    def get_raw_attribute(self, attribute: str) -> Any:
        """Get raw attribute value without casting."""
        if hasattr(self, "_attributes") and attribute in self._attributes:
            return self._attributes[attribute]
        return None

    # ===== Mass Assignment =====

    def fill(self, attributes: Dict[str, Any]) -> "HasAttributes":
        """Fill model with attributes respecting mass assignment protection."""
        filtered_attributes = self.filter_mass_assignment(attributes)

        for key, value in filtered_attributes.items():
            self.set_attribute(key, value)

        return self

    def fill_original(self, attributes: Dict[str, Any]) -> "HasAttributes":
        """Fill original attributes (for loaded models)."""
        if not hasattr(self, "_original"):
            self.__dict__["_original"] = {}

        self._original.update(attributes)
        self._attributes.update(attributes)
        self.__dict__["_loaded"] = True

        return self

    @classmethod
    def filter_mass_assignment(cls, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Filter attributes through mass assignment protection."""
        return cls.filter_guarded(cls.filter_fillable(attributes))

    @classmethod
    def filter_fillable(cls, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Filter attributes through fillable whitelist."""
        if "*" in cls.__fillable__:
            return attributes

        return {
            key: value for key, value in attributes.items() if key in cls.__fillable__
        }

    @classmethod
    def filter_guarded(cls, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Filter attributes through guarded blacklist."""
        if "*" in cls.__guarded__:
            return {}

        return {
            key: value for key, value in attributes.items() if key not in cls.__guarded__
        }

    # ===== Serialization =====

    def to_array(self, exclude=None, include=None) -> Dict[str, Any]:
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
        """Convert model to JSON string."""
        return json.dumps(self.to_array(), default=str, **kwargs)

    def serialize(self, exclude=None, include=None) -> Dict[str, Any]:
        """Legacy method - uses to_array internally."""
        return self.to_array(exclude=exclude, include=include)

    # ===== Visibility Control =====

    def make_hidden(self, *attributes) -> "HasAttributes":
        """Hide attributes from serialization."""
        clone = self._clone_for_visibility()
        clone._hidden_cache.update(attributes)
        return clone

    def make_visible(self, *attributes) -> "HasAttributes":
        """Make attributes visible in serialization."""
        clone = self._clone_for_visibility()
        clone._visible_cache.update(attributes)
        # Remove from hidden cache if present
        clone._hidden_cache.difference_update(attributes)
        return clone

    def without_timestamps(self) -> "HasAttributes":
        """Exclude timestamps from serialization."""
        clone = self._clone_for_visibility()
        clone._without_timestamps = True
        return clone

    def append(self, *attributes) -> "HasAttributes":
        """Add computed attributes to serialization."""
        clone = self._clone_for_visibility()
        clone._appends_cache.update(attributes)
        return clone

    def only(self, *attributes) -> Dict[str, Any]:
        """Get only specified attributes using Collection.only()."""
        data = self.to_array()
        return Collection(data).only(*attributes)

    # ===== Dirty Tracking =====

    def is_dirty(self) -> bool:
        """Check if model has unsaved changes."""
        return bool(getattr(self, "_changes", {}))

    def get_dirty_attributes(self) -> Dict[str, Any]:
        """Get all dirty attributes."""
        return getattr(self, "_changes", {}).copy()

    def get_original(self, key: Optional[str] = None) -> Any:
        """Get original attribute value(s)."""
        original = getattr(self, "_original", {})

        if key is None:
            return original.copy()

        return original.get(key)

    # ===== Internal Helper Methods =====

    def _get_base_attributes(self) -> Dict[str, Any]:
        """Get base model attributes."""
        return getattr(self, "_attributes", {}).copy()

    def _apply_visibility_rules(
        self, data: Dict[str, Any], exclude=None, include=None
    ) -> Dict[str, Any]:
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

    def _apply_casts_and_dates(self, data: Dict[str, Any]) -> Dict[str, Any]:
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
            # Try to get from config first
            from config.app import APP_TIMEZONE

            return APP_TIMEZONE
        except ImportError:
            # Fallback to UTC
            return "UTC"

    def _serialize_relations(self) -> Dict[str, Any]:
        """Serialize model relationships."""
        # This will be implemented in HasRelationships concern
        return {}

    def _serialize_appends(self) -> Dict[str, Any]:
        """Serialize appended attributes."""
        result = {}
        appends = set(self.__appends__) | getattr(self, "_appends_cache", set())

        for attribute in appends:
            # Try to get accessor value
            try:
                result[attribute] = getattr(self, attribute)
            except AttributeError:
                pass

        return result

    def _clone_for_visibility(self) -> "HasAttributes":
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
    def cast_value(cls, attribute: str, value: Any, cast_type: str = None) -> Any:
        """Cast a value using the specified cast type."""
        if cast_type is None and attribute in cls.__casts__:
            cast_type = cls.__casts__[attribute]

        if cast_type:
            from cara.eloquent.casts import cast_registry

            cast_instance = cast_registry.get_cast_instance(cast_type)
            if cast_instance:
                return cast_instance.get(value)

        return value
