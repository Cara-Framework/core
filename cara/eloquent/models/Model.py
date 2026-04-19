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

import copy
import inspect
import json
from datetime import date as datetimedate
from datetime import datetime
from datetime import time as datetimetime
from typing import Any, Callable, Dict, List, Optional, Type, Union

import pendulum
from inflection import tableize, underscore

from cara.exceptions import ModelNotFoundException
from cara.support.Collection import Collection

# Import cast system
from ..casts.collections import ArrayCast, CollectionCast
from ..casts.datetime import DateCast, DateTimeCast, TimestampCast
from ..casts.primitives import BoolCast, DecimalCast, FloatCast, IntCast, JsonCast
from ..casts.security import EncryptedCast, HashCast
from ..casts.validation import EmailCast, URLCast, UUIDCast

# Import concerns for clean architecture
from ..concerns.HasAttributes import HasAttributes
from ..concerns.HasRelationships import HasRelationships
from ..concerns.HasTimestamps import HasTimestamps
from ..observers import ObservesEvents
from ..query import QueryBuilder
from ..scopes import TimeStampsMixin


class ModelMeta(type):
    """Metaclass for Model that enables static method calls and scope registration.

    This metaclass allows Models to be called statically (e.g., User.first())
    while automatically registering query scopes defined with scope_ prefix.
    """

    def __new__(
        mcs: Type["ModelMeta"],
        name: str,
        bases: tuple,
        namespace: Dict[str, Any],
        **kwargs: Any,
    ) -> "ModelMeta":
        """Create new Model class with automatic scope method generation.

        Args:
            name: The class name
            bases: Base classes
            namespace: The class namespace
            **kwargs: Additional arguments

        Returns:
            The new model class
        """
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Auto-register scope methods (Laravel style)
        cls._class_scopes: Dict[str, Callable] = {}

        # Find all scope_ methods and create corresponding class methods
        for attr_name in dir(cls):
            if attr_name.startswith("scope_") and callable(
                getattr(cls, attr_name, None)
            ):
                scope_method = getattr(cls, attr_name)

                # Extract scope name (remove 'scope_' prefix)
                scope_name = attr_name[6:]

                # Create a wrapper that returns a new query builder with scope applied
                def create_scope_method(
                    scope_func: Callable, scope_name: str
                ) -> classmethod:
                    def scope_wrapper(cls_inner: Type, *args: Any, **kwargs: Any) -> QueryBuilder:
                        # Create new instance and get fresh query builder
                        instance = cls_inner()
                        builder = instance.get_builder()

                        # Apply the scope to the builder
                        return scope_func(instance, builder, *args, **kwargs)

                    scope_wrapper.__name__ = scope_name
                    scope_wrapper.__doc__ = f"Query scope: {scope_name}"
                    return classmethod(scope_wrapper)

                # Add the scope method to the class
                setattr(cls, scope_name, create_scope_method(scope_method, scope_name))
                cls._class_scopes[scope_name] = scope_method

        if cls._class_scopes and hasattr(cls, "_scopes"):
            cls._scopes[cls] = cls._class_scopes

        return cls

    def __getattribute__(cls: Type, attribute: str) -> Any:
        """Enhanced meta method with Laravel-style scope handling.

        Enables static method calls on models by instantiating and delegating.
        Falls back to instance method access if class attribute not found.

        Args:
            attribute: The attribute name to access

        Returns:
            The attribute value

        Raises:
            AttributeError: If attribute cannot be found
        """
        try:
            # First try normal attribute access
            return super().__getattribute__(attribute)
        except AttributeError:
            # If attribute doesn't exist, try to instantiate and get it (original behavior)
            try:
                instantiated = cls()
                return getattr(instantiated, attribute)
            except AttributeError:
                raise AttributeError(
                    f"'{cls.__name__}' object has no attribute '{attribute}'"
                ) from None


class Model(
    HasAttributes,
    HasRelationships,
    TimeStampsMixin,
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
    __fillable__: List[str] = ["*"]
    __guarded__: List[str] = []
    __hidden__: List[str] = []
    __visible__: List[str] = []
    __appends__: List[str] = []

    # Database configuration
    __table__: Optional[str] = None
    __connection__: str = "default"
    __resolved_connection__: Optional[Any] = None
    __primary_key__: str = "id"
    __primary_key_type__: str = "int"
    __selects__: List[str] = []

    # Attribute casting and dates
    __casts__: Dict[str, Union[str, Type]] = {}
    __dates__: List[str] = []
    __cast_map__: Dict[str, Type] = {}
    __internal_cast_map__: Dict[str, Type] = {}

    # Timestamps
    __timestamps__: bool = True
    __timezone__: str = "UTC"
    date_created_at: str = "created_at"
    date_updated_at: str = "updated_at"

    # Relationships and eager loading
    __with__: tuple = ()
    __relationship_hidden__: Dict[str, List[str]] = {}

    # Events and observers
    __observers__: Dict[str, Any] = {}
    __has_events__: bool = True

    # Query execution
    __dry__: bool = False
    __force_update__: bool = False

    # Internal state
    _booted: bool = False
    _scopes: Dict[Type, Dict[str, Callable]] = {}

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

    __internal_cast_map__: Dict[str, Type] = {
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
        self.__attributes__: Dict[str, Any] = {}
        self.__original_attributes__: Dict[str, Any] = {}
        self.__dirty_attributes__: Dict[str, Any] = {}

        # Initialize appends if not already present
        if not hasattr(self, "__appends__"):
            self.__appends__ = []

        # Initialize relationships storage
        self._relations: Dict[str, Any] = {}
        self._relationships: Dict[str, Any] = {}
        self._global_scopes: Dict[str, Any] = {}

        # Initialize model events cache
        self._model_events: Optional[Dict[str, List[Callable]]] = None

        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Bootstrap the model (register observers, etc)
        self.boot()

    @classmethod
    def get_primary_key(cls) -> str:
        """Get the primary key column name."""
        return cls.__primary_key__

    def get_primary_key_type(self):
        """
        Gets the primary key column type.

        Returns:
            mixed
        """
        return self.__primary_key_type__

    def get_primary_key_value(self):
        """
        Gets the primary key value.

        Raises:
            AttributeError: Raises attribute error if the model does not have an
                attribute with the primary key.

        Returns:
            str|int
        """
        try:
            return getattr(self, self.get_primary_key())
        except AttributeError:
            name = self.__class__.__name__
            raise AttributeError(
                f"class '{name}' has no attribute {self.get_primary_key()}. Did you set the primary key correctly on the model using the __primary_key__ attribute?"
            )

    def get_foreign_key(self):
        """
        Gets the foreign key based on this model name.

        Args:
            relationship (str): The relationship name.

        Returns:
            str
        """
        return underscore(self.__class__.__name__ + "_" + self.get_primary_key())

    # NOTE: ``query`` is defined later in this class as a ``@classmethod``
    # (Laravel parity — ``Model.query()``). The instance-level shadow that
    # used to live here was dead code and has been removed.

    def get_builder(self):
        if hasattr(self, "builder"):
            return self.builder

        self.builder = QueryBuilder(
            connection=self.__connection__,
            table=self.get_table_name(),
            connection_details=self.get_connection_details(),
            model=self,
            scopes=self._scopes.get(self.__class__),
            dry=self.__dry__,
        )

        return self.builder

    def get_selects(self):
        return self.__selects__

    @classmethod
    def get_columns(cls):
        row = cls.first()
        return list(row.__attributes__.keys()) if row else []

    def get_connection_details(self):
        from cara.facades import DB

        return DB.get_connection_details()

    def boot(self):
        if not self._booted:
            self.observe_events(self, "booting")
            for base_class in inspect.getmro(self.__class__):
                class_name = base_class.__name__

                if class_name.endswith("Mixin"):
                    getattr(self, "boot_" + class_name)(self.get_builder())
                elif (
                    base_class != Model
                    and issubclass(base_class, Model)
                    and "__fillable__" in base_class.__dict__
                    and "__guarded__" in base_class.__dict__
                ):
                    raise AttributeError(
                        f"{type(self).__name__} must specify either __fillable__ or __guarded__ properties, but not both."
                    )

            self._booted = True
            self.observe_events(self, "booted")

            self.append_passthrough(list(self.get_builder()._macros.keys()))

    def append_passthrough(self, passthrough):
        self.__passthrough__.update(passthrough)
        return self

    def _get_model_events(self):
        """
        Get cached model events or discover them if not cached.

        Returns:
            dict: Mapping of event names to list of listener methods
        """
        if self._model_events is None:
            from cara.decorators.events import get_model_events

            self._model_events = get_model_events(self.__class__)
        return self._model_events

    def _fire_model_event(self, event_name: str) -> bool:
        """
        Fire a model event and return whether it should continue.

        Args:
            event_name (str): The name of the event to fire

        Returns:
            bool: True if operation should continue, False if cancelled
        """
        events = self._get_model_events()

        if event_name not in events:
            return True  # No listeners, continue

        # Fire all listeners for this event
        for listener_method in events[event_name]:
            try:
                # Call the listener method (bound method call)
                result = listener_method(self)

                # If any listener returns False, cancel the operation
                if result is False:
                    return False

            except Exception as e:
                # Log error but don't stop other listeners
                from cara.facades import Log

                Log.error(
                    f"Model event error in {listener_method.__name__}: {e}",
                    exc_info=True,
                )

        return True  # All listeners passed, continue

    def save(self, **kwargs: Any) -> bool:
        """Save the model to the database.

        Laravel-style save method with full event lifecycle.
        Fires appropriate events: creating/updating -> saving -> created/updated -> saved

        Returns:
            True if successful, False if cancelled by event or error occurred
        """
        # Determine if this is a new record or existing one
        is_new_record = not self.is_created()

        # Fire pre-save events
        if is_new_record:
            # Fire creating event - can cancel operation
            if not self._fire_model_event("creating"):
                return False
        else:
            # Fire updating event - can cancel operation
            if not self._fire_model_event("updating"):
                return False

        # Fire saving event - can cancel operation
        if not self._fire_model_event("saving"):
            return False

        try:
            # Perform the actual save operation
            if is_new_record:
                # Create new record
                result = self.__class__.create(self.all_attributes(), **kwargs)
                if result:
                    # Copy created record's attributes back to this instance
                    self.__attributes__.update(result.__attributes__)
                    self.__original_attributes__.update(result.__original_attributes__)
                    self.__dirty_attributes__.clear()
            else:
                # Update existing record
                updates = self.get_dirty_attributes()
                if updates:
                    result = (
                        self.get_builder()
                        .where(self.get_primary_key(), self.get_primary_key_value())
                        .update(updates, **kwargs)
                    )
                    if result:
                        # Merge dirty attributes into main attributes
                        self.__attributes__.update(self.__dirty_attributes__)
                        self.__original_attributes__.update(self.__dirty_attributes__)
                        self.__dirty_attributes__.clear()
                else:
                    result = True  # No changes to save

            if not result:
                return False

            # Fire post-save events (these cannot cancel the operation)
            if is_new_record:
                self._fire_model_event("created")
            else:
                self._fire_model_event("updated")

            self._fire_model_event("saved")

            # Touch parent models if configured
            if hasattr(self, "__touches__") and self.__touches__:
                self._touch_parents()

            return True

        except Exception as e:
            from cara.facades import Log

            Log.error(f"Save operation failed: {e}", exc_info=True)
            return False

    def delete(self, **kwargs: Any) -> bool:
        """Delete the model from the database.

        Laravel-style delete method with event lifecycle.
        Fires: deleting -> deleted

        Returns:
            True if successful, False if cancelled by event or error occurred
        """
        # Fire deleting event - can cancel operation
        if not self._fire_model_event("deleting"):
            return False

        try:
            # Perform the actual delete operation
            result = (
                self.get_builder()
                .where(self.get_primary_key(), self.get_primary_key_value())
                .delete(**kwargs)
            )

            if result:
                # Fire deleted event (cannot cancel)
                self._fire_model_event("deleted")
                return True
            else:
                return False

        except Exception as e:
            from cara.facades import Log

            Log.error(f"Delete operation failed: {e}", exc_info=True)
            return False

    def _touch_parents(self):
        """Touch parent models listed in __touches__."""
        for relation_name in self.__touches__:
            related = getattr(self, relation_name, None)
            if related and hasattr(related, 'touch'):
                related.touch()

    def touch(self):
        """Update the model's updated_at timestamp."""
        # Get the timestamp column name
        timestamp_col = 'updated_at'
        if hasattr(self, '__timestamps__') and self.__timestamps__:
            if isinstance(self.__timestamps__, (list, tuple)):
                timestamp_col = self.__timestamps__[1] if len(self.__timestamps__) > 1 else 'updated_at'
        
        # Get the current datetime in the appropriate format
        current_time = self.get_new_datetime_string()
        
        # Update only the timestamp column
        self.update({timestamp_col: current_time})
        
        # Also update the local attribute
        self.__attributes__[timestamp_col] = current_time
        self.__original_attributes__[timestamp_col] = current_time

    @classmethod
    def get_table_name(cls) -> str:
        """Get the table name, deriving from class name via Laravel's tableize rules."""
        return cls.__table__ or tableize(cls.__name__)

    @classmethod
    def table(cls, table):
        """
        Gets the table name.

        Returns:
            str
        """
        cls.__table__ = table
        return cls

    @classmethod
    def find(
        cls: Type["Model"],
        record_id: Union[Any, List[Any], tuple],
        query: bool = False,
    ) -> Union["Model", List["Model"], QueryBuilder, None]:
        """Find a row by the primary key ID.

        Args:
            record_id: The primary key value (int, string) or list of IDs
            query: If True, return the QueryBuilder instead of executing

        Returns:
            A Model instance, Collection of models, QueryBuilder, or None if not found
        """
        if isinstance(record_id, (list, tuple)):
            if not record_id:
                return cls.new_collection([]) if not query else cls().get_builder()
            builder = cls().where_in(cls.get_primary_key(), record_id)
        else:
            builder = cls().where(cls.get_primary_key(), record_id)

        if query:
            return builder

        if isinstance(record_id, (list, tuple)):
            return builder.get()

        return builder.first()

    @classmethod
    def find_or_fail(
        cls: Type["Model"],
        record_id: Union[Any, List[Any], tuple],
        query: bool = False,
    ) -> Union["Model", List["Model"], QueryBuilder]:
        """Find a row by the primary key ID or raise ModelNotFoundException.

        Args:
            record_id: The primary key value or list of IDs
            query: If True, return the QueryBuilder instead of executing

        Returns:
            A Model instance or Collection of models

        Raises:
            ModelNotFoundException: If no model is found
        """
        result = cls.find(record_id, query)

        if not result:
            raise ModelNotFoundException(
                f"{cls.__name__} with ID {record_id} not found"
            )

        return result

    def is_loaded(self):
        return bool(self.__attributes__)

    def is_created(self):
        return self.get_primary_key() in self.__attributes__

    @classmethod
    def hydrate(cls, result, relations=None):
        """
        Takes a result and loads it into a model.

        Args:
            result ([type]): [description]
            relations (dict, optional): [description]. Defaults to {}.

        Returns:
            [type]: [description]
        """

        relations = relations or {}

        if result is None:
            return None

        if isinstance(result, (list, tuple)):
            response = []
            for element in result:
                response.append(cls.hydrate(element))
            return cls.new_collection(response)

        elif isinstance(result, dict):
            model = cls()
            dic = {}
            for key, value in result.items():
                if key in model.get_dates() and value:
                    value = model.get_new_date(value)
                dic.update({key: value})

            # Hydration logging disabled for performance
            # from cara.facades import Log
            # Log.debug(f"Hydrating Model {cls.__name__}", category="cara.eloquent.hydrate")

            model.observe_events(model, "hydrating")
            model.__attributes__.update(dic or {})
            model.__original_attributes__.update(dic or {})
            model.add_relation(relations)
            model.observe_events(model, "hydrated")
            return model

        elif hasattr(result, "serialize"):
            model = cls()
            model.__attributes__.update(result.serialize())
            model.__original_attributes__.update(result.serialize())
            return model
        else:
            model = cls()
            model.observe_events(model, "hydrating")
            model.__attributes__.update(dict(result))
            model.__original_attributes__.update(dict(result))
            model.observe_events(model, "hydrated")
            return model

    def fill(self, attributes):
        self.__attributes__.update(attributes)
        return self

    def fill_original(self, attributes):
        self.__original_attributes__.update(attributes)
        return self

    @classmethod
    def new_collection(cls, data):
        """
        Takes a result and puts it into a new collection. This is designed to be able to be
        overidden by the user.

        Args:
            data (list|dict): Could be any data type but will be loaded directly into a collection.

        Returns:
            Collection
        """
        return Collection(data)

    @classmethod
    def create(
        cls: Type["Model"],
        dictionary: Optional[Dict[str, Any]] = None,
        query: bool = False,
        cast: bool = True,
        **kwargs: Any,
    ) -> Union["Model", QueryBuilder]:
        """Create a new record in the database.

        Args:
            dictionary: Attributes for the new record
            query: If True, return the QueryBuilder instead of executing
            cast: Whether to cast attribute values
            **kwargs: Additional options passed to the query builder

        Returns:
            A new Model instance, or a QueryBuilder if query=True
        """
        if query:
            return cls().get_builder().create(
                dictionary, query=True, cast=cast, **kwargs
            )

        return cls().get_builder().create(dictionary, cast=cast, **kwargs)

    @classmethod
    def cast_value(cls, attribute: str, value: Any):
        """
        Given an attribute name and a value, casts the value using the model's registered caster.

        If no registered caster exists, returns the unmodified value.

        Supports parametrized casts like:
        - "datetime:YYYY-MM-DD HH:mm:ss"
        - "array:int"
        - "hash:bcrypt"
        """
        cast_definition = cls.__casts__.get(attribute)
        if not cast_definition:
            return value

        if value is None:
            return None

        # Use new cast registry system
        from ..casts import cast_registry as enhanced_registry

        cast_instance = enhanced_registry.get_cast_instance(cast_definition)
        if cast_instance:
            return cast_instance.set(value)

        return value

    @classmethod
    def cast_values(cls, dictionary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs provided dictionary through all model casters and returns the result.

        Does not mutate the passed dictionary.
        """
        if not dictionary:
            return {}
        return {x: cls.cast_value(x, dictionary[x]) for x in dictionary}

    def fresh(self):
        """Return a newly-loaded instance of the same record (Laravel parity)."""
        return (
            self.get_builder()
            .where(
                self.get_primary_key(),
                self.get_primary_key_value(),
            )
            .first()
        )

    def refresh(self):
        """Reload the model's attributes from the database in place.

        Laravel parity — unlike :meth:`fresh`, this mutates ``self`` and
        returns ``self`` so callers can chain or ignore the return value.
        Raises ``ValueError`` if the record no longer exists.
        """
        reloaded = self.fresh()
        if reloaded is None:
            raise ValueError(
                f"Cannot refresh {self.__class__.__name__}: record "
                f"{self.get_primary_key()}={self.get_primary_key_value()!r} "
                "was not found."
            )

        reloaded_attrs = getattr(reloaded, "__attributes__", {})
        self.__attributes__ = dict(reloaded_attrs)
        self.__original_attributes__ = dict(reloaded_attrs)
        self.__dirty_attributes__ = {}
        # Clear any cached relationship data so accessors reload fresh.
        self._relations = {}
        self._relationships = {}
        return self

    def serialize(self, exclude=None, include=None):
        """
        Convert the model instance to a serializable dictionary.
        Uses the proper cast system to handle all data types.

        Args:
            exclude (list, optional): Attributes to exclude from serialization
            include (list, optional): Only these attributes will be included

        Returns:
            dict: The model as a dictionary with all objects converted to JSON-serializable types
        """
        # Get all attributes
        data = self.__attributes__.copy()
        data.update(self.__dirty_attributes__)

        # Remove builder if present
        if "builder" in data:
            del data["builder"]

        # Apply exclude/include filters
        if include:
            data = {k: v for k, v in data.items() if k in include}
        if exclude:
            data = {k: v for k, v in data.items() if k not in exclude}

        # Apply hidden attributes
        hidden = getattr(self, "__hidden__", [])
        for hidden_key in hidden:
            data.pop(hidden_key, None)

        # Apply casts to all attributes that have them
        for key, value in data.items():
            if value is not None:
                # Check if this attribute has a cast defined
                if key in self.__casts__:
                    try:
                        # Use the proper cast system
                        data[key] = self._cast_attribute(key, value)
                    except Exception:
                        # If casting fails, keep original value
                        pass

        # Handle remaining datetime and decimal types (including casted ones)
        # This runs AFTER casting to ensure ALL Decimals are JSON-serializable
        from decimal import Decimal

        for key, value in data.items():
            if value is not None:
                final_value = data[key]
                if isinstance(final_value, datetime):
                    data[key] = final_value.isoformat()
                elif isinstance(final_value, datetimetime):
                    data[key] = final_value.strftime("%H:%M:%S")
                elif isinstance(final_value, datetimedate):
                    data[key] = final_value.strftime("%Y-%m-%d")
                elif isinstance(final_value, Decimal):
                    data[key] = float(final_value)

        # Add relationships - Laravel way: use serialize() for proper casting
        relations_dict = getattr(self, "_relations", {})
        for relation_name, relation_value in relations_dict.items():
            if relation_value is None:
                data[relation_name] = None
            elif isinstance(relation_value, list):
                # Collection of models
                from cara.support.Collection import Collection

                data[relation_name] = Collection(relation_value).serialize()
            elif hasattr(relation_value, "serialize"):
                # Single model - use serialize() for proper decimal casting
                data[relation_name] = relation_value.serialize()
            else:
                # Raw value
                data[relation_name] = relation_value

        # Add appends (computed attributes)
        appends = getattr(self, "__appends__", [])
        for append_name in appends:
            try:
                data[append_name] = getattr(self, append_name)
            except AttributeError:
                pass

        return data

    def to_array(self, exclude=None, include=None):
        """
        Laravel-style alias for serialize().

        Returns:
            dict: Same as serialize()
        """
        return self.serialize(exclude=exclude, include=include)

    def to_json(self, **kwargs):
        """
        Convert the model instance to JSON.
        Laravel-style method with options.

        Args:
            **kwargs: Additional arguments passed to json.dumps()

        Returns:
            str: JSON representation of the model
        """
        # Default JSON options
        json_options = {
            "default": str,
            "ensure_ascii": False,
            "indent": kwargs.get("indent"),
        }

        # Override with any passed kwargs
        json_options.update(kwargs)

        return json.dumps(self.to_array(), **json_options)

    def make_hidden(self, *attributes):
        """
        Make the given attributes hidden for serialization.
        Returns a new instance with updated hidden attributes.

        Args:
            *attributes: Attribute names to hide

        Returns:
            Model: New model instance with updated visibility
        """
        clone = self._clone_for_visibility()

        for attr in attributes:
            if attr not in clone.__hidden__:
                clone.__hidden__.append(attr)

        return clone

    def make_visible(self, *attributes):
        """
        Make the given hidden attributes visible for serialization.
        Returns a new instance with updated visible attributes.

        Args:
            *attributes: Attribute names to make visible

        Returns:
            Model: New model instance with updated visibility
        """
        clone = self._clone_for_visibility()

        # Remove from hidden list
        clone.__hidden__ = [attr for attr in clone.__hidden__ if attr not in attributes]

        return clone

    def set_hidden(self, hidden):
        """
        Set the hidden attributes for the model.

        Args:
            hidden (list): List of attributes to hide

        Returns:
            Model: Self for method chaining
        """
        self.__hidden__ = list(hidden) if hidden else []
        return self

    def set_visible(self, visible):
        """
        Set the visible attributes for the model.

        Args:
            visible (list): List of attributes to show

        Returns:
            Model: Self for method chaining
        """
        self.__visible__ = list(visible) if visible else []
        return self

    def append(self, *attributes):
        """
        Add attributes to the append list.

        Args:
            *attributes: Attribute names to append

        Returns:
            Model: Self for method chaining
        """
        for attr in attributes:
            if attr not in self.__appends__:
                self.__appends__.append(attr)
        return self

    def except_keys(self, *keys):
        """
        Return a new model instance excluding specified keys from serialization.
        Compatible with Collection.except_keys() for relationship serialization.

        Args:
            *keys: Attribute names to exclude

        Returns:
            Model: New model instance with updated hidden attributes
        """
        return self.make_hidden(*keys)

    def without_timestamps(self):
        """
        Return a version of this model without timestamp fields.

        Returns:
            Model: New model instance without timestamps
        """
        clone = self._clone_for_visibility()

        timestamp_fields = [self.date_created_at, self.date_updated_at]
        for field in timestamp_fields:
            if field not in clone.__hidden__:
                clone.__hidden__.append(field)

        return clone

    def _clone_for_visibility(self):
        """Create a shallow clone for visibility modifications."""
        clone = copy.copy(self)
        clone.__hidden__ = list(self.__hidden__)
        clone.__visible__ = list(self.__visible__)
        clone.__appends__ = list(self.__appends__)
        return clone

    @classmethod
    def first_or_create(cls, wheres, creates: Optional[dict] = None):
        """
        Get the first record matching the attributes or create it.

        Returns:
            Model
        """
        if creates is None:
            creates = {}
        self = cls()
        record = self.where(wheres).first()
        total = {}
        total.update(creates)
        total.update(wheres)
        if not record:
            return self.create(total, id_key=cls.get_primary_key())
        return record

    @classmethod
    def update_or_create(cls, wheres, updates):
        self = cls()
        record = self.where(wheres).first()
        total = {}
        total.update(updates)
        total.update(wheres)
        if not record:
            return self.create(total, id_key=cls.get_primary_key()).fresh()

        self.where(wheres).update(total)
        return self.where(wheres).first()

    @classmethod
    def truncate(cls, foreign_keys=False):
        """
        Laravel-style truncate method.
        Truncate the table associated with the model.

        Arguments:
            foreign_keys {bool} -- Whether to disable foreign key constraints (default: {False})

        Returns:
            int -- Number of affected rows
        """
        return cls().get_builder().truncate(foreign_keys)

    @classmethod
    def query(cls):
        """
        Laravel-style query method.
        Begin querying the model.

        Returns:
            QueryBuilder -- A new query builder instance
        """
        return cls().get_builder()

    def relations_to_dict(self):
        """
        Converts a models relationships to a dictionary.

        Returns:
            [type]: [description]
        """
        new_dic = {}
        for key, value in self._relations.items():
            if value == {}:
                new_dic.update({key: {}})
            else:
                if value is None:
                    new_dic.update({key: {}})
                    continue
                elif isinstance(value, list):
                    value = Collection(value).serialize()
                elif isinstance(value, dict):
                    pass
                else:
                    value = value.serialize()

                new_dic.update({key: value})

        return new_dic

    # NOTE: timestamp methods (touch, _update_timestamps, _current_timestamp) are now
    # provided by the HasTimestamps concern imported via TimeStampsMixin

    def __getattr__(self, attribute):
        """
        Magic method that is called when an attribute does not exist on the model.

        Args:
            attribute (string): the name of the attribute being accessed or called.

        Returns:
            mixed: Could be anything that a method can return.
        """

        # Check for @accessor decorated methods first
        accessor_method_name = f"get_{attribute}_attribute"
        # Use direct __dict__ access to avoid recursion
        if accessor_method_name in self.__class__.__dict__:
            accessor_method = self.__class__.__dict__[accessor_method_name]
            if hasattr(accessor_method, "_is_accessor"):
                # Get the raw value (dirty first, then stored, then None for virtual attributes)
                if (
                    "__dirty_attributes__" in self.__dict__
                    and attribute in self.__dict__["__dirty_attributes__"]
                ):
                    raw_value = self.__dict__["__dirty_attributes__"][attribute]
                elif (
                    "__attributes__" in self.__dict__
                    and attribute in self.__dict__["__attributes__"]
                ):
                    raw_value = self.__dict__["__attributes__"][attribute]
                else:
                    # For virtual attributes (no stored value), pass None
                    raw_value = None

                # Call the accessor with the raw value (bound method call)
                return accessor_method(self, raw_value)

        # Check for non-decorated accessor methods (Laravel-style naming convention)
        non_decorated_accessor = "get_" + attribute + "_attribute"
        if non_decorated_accessor in self.__class__.__dict__:
            accessor_method = self.__class__.__dict__[non_decorated_accessor]
            # Get the raw value for non-decorated accessors
            if (
                "__dirty_attributes__" in self.__dict__
                and attribute in self.__dict__["__dirty_attributes__"]
            ):
                raw_value = self.__dict__["__dirty_attributes__"][attribute]
            elif (
                "__attributes__" in self.__dict__
                and attribute in self.__dict__["__attributes__"]
            ):
                raw_value = self.__dict__["__attributes__"][attribute]
            else:
                # For virtual attributes (no stored value), pass None
                raw_value = None

            # Call the accessor method with raw value
            return accessor_method(self, raw_value)

        if (
            "__dirty_attributes__" in self.__dict__
            and attribute in self.__dict__["__dirty_attributes__"]
        ):
            # Always apply cast if defined for dirty attributes too
            if attribute in self.__casts__:
                return self.get_dirty_value(attribute)
            return self.get_dirty_value(attribute)

        if (
            "__attributes__" in self.__dict__
            and attribute in self.__dict__["__attributes__"]
        ):
            # Always apply cast if defined, regardless of date type
            if attribute in self.__casts__:
                return self.get_value(attribute)
            elif attribute in self.get_dates():
                return (
                    self.get_new_date(self.get_value(attribute))
                    if self.get_value(attribute)
                    else None
                )
            return self.get_value(attribute)

        if attribute in self.__passthrough__:
            # Special warning for common dict-style usage mistake
            if attribute == "get":

                def method(*args, **kwargs):
                    # Check if this looks like dict-style access
                    if len(args) >= 1 and isinstance(args[0], str) and len(args) <= 2:
                        attr_name = args[0]
                        default_value = args[1] if len(args) == 2 else None

                        # This looks like user.get('id', 'default') - common mistake!
                        raise AttributeError(
                            f"❌ Model dict-style access error!\n"
                            f"You tried: model.get('{attr_name}', {repr(default_value)})\n"
                            f"✅ Use instead: getattr(model, '{attr_name}', {repr(default_value)})\n"
                            f"   or simply: model.{attr_name}\n"
                            f"\n"
                            f"🧠 Remember: Cara Models are not dictionaries!\n"
                            f"   - model.attribute     ← ✅ Correct\n"
                            f"   - model.get('attr')   ← ❌ Wrong (calls QueryBuilder.get)\n"
                            f"   - getattr(model, 'attr', default) ← ✅ Correct with default"
                        )

                    # Not dict-style, pass to QueryBuilder
                    return getattr(self.get_builder(), attribute)(*args, **kwargs)

                return method
            else:

                def method(*args, **kwargs):
                    return getattr(self.get_builder(), attribute)(*args, **kwargs)

                return method

        if attribute in self.__dict__.get("_relations", {}):
            return self.__dict__["_relations"][attribute]

        if attribute in self.__dict__.get("_relationships", {}):
            return self.__dict__["_relationships"][attribute]

        if attribute not in self.__dict__:
            name = self.__class__.__name__

            raise AttributeError(f"class model '{name}' has no attribute {attribute}")

        return None

    def only(self, attributes: list) -> dict:
        if isinstance(attributes, str):
            attributes = [attributes]
        results: dict[str, Any] = {}
        for attribute in attributes:
            if " as " in attribute:
                attribute, alias = attribute.split(" as ")
                alias = alias.strip()
                attribute = attribute.strip()
            else:
                alias = attribute.strip()
                attribute = attribute.strip()

            results[alias] = self.get_raw_attribute(attribute)

        return results

    def __setattr__(self, attribute, value):
        # Check for @mutator decorated methods first
        mutator_method_name = f"set_{attribute}_attribute"
        # Use direct __dict__ access to avoid recursion
        if mutator_method_name in self.__class__.__dict__:
            mutator_method = self.__class__.__dict__[mutator_method_name]
            if hasattr(mutator_method, "_is_mutator"):
                # Decorated mutator (receives self and value)
                value = mutator_method(self, value)
            else:
                # Non-decorated mutator (legacy support - receives self and value)
                value = mutator_method(self, value)

        if attribute in self.__casts__:
            value = self._set_cast_attribute(attribute, value)

        if attribute in self.get_dates():
            # Convert user timezone to UTC for database storage
            value = self._convert_date_to_utc_for_database(value)
            value = self.get_new_datetime_string(value)

        try:
            if not attribute.startswith("_"):
                self.__dict__["__dirty_attributes__"].update({attribute: value})
            else:
                self.__dict__[attribute] = value
        except KeyError:
            # `__dirty_attributes__` has not been initialized yet (can happen
            # during parent ``__init__`` before our ``__init__`` runs line
            # ``self.__dirty_attributes__ = {}``). Silently dropping the value
            # — as the previous implementation did — lost writes. Instead, we
            # bootstrap the dict and retry so no attribute is lost.
            if not attribute.startswith("_"):
                self.__dict__.setdefault("__dirty_attributes__", {})[attribute] = value
            else:
                self.__dict__[attribute] = value

    def get_raw_attribute(self, attribute):
        """
        Gets an attribute without having to call the models magic methods. Gets around infinite
        recursion loops.

        Args:
            attribute (string): The attribute to fetch

        Returns:
            mixed: Any value an attribute can be.
        """
        return self.__attributes__.get(attribute)

    def is_dirty(self, *attributes: str) -> bool:
        """Return ``True`` if the model (or specific attributes) has unsaved changes.

        Mirrors Laravel's ``isDirty`` signature — with no arguments, returns
        ``True`` if any attribute is dirty; with one or more names, returns
        ``True`` only if at least one of the named attributes is dirty.
        """
        if not self.__dirty_attributes__:
            return False
        if not attributes:
            return True
        return any(name in self.__dirty_attributes__ for name in attributes)

    def is_clean(self, *attributes: str) -> bool:
        """Inverse of :meth:`is_dirty` — Laravel parity."""
        return not self.is_dirty(*attributes)

    def get_original(self, key):
        return self.__original_attributes__.get(key)

    def get_dirty_attributes(self):
        if "builder" in self.__dirty_attributes__:
            self.__dirty_attributes__.pop("builder")
        return self.__dirty_attributes__ or {}

    def get_value(self, attribute):
        """Get attribute value with cast applied."""
        value = self.__attributes__[attribute]
        if attribute in self.__casts__:
            # Import the enhanced registry that has registered casts
            from ..casts import cast_registry as enhanced_registry

            # Get cast instance and apply if found
            cast_definition = self.__casts__[attribute]
            cast_instance = enhanced_registry.get_cast_instance(cast_definition)

            if cast_instance:
                return cast_instance.get(value)
        return value

    def get_dirty_value(self, attribute):
        """Get dirty attribute value with cast applied."""
        value = self.__dirty_attributes__[attribute]
        if attribute in self.__casts__:
            # Import the enhanced registry that has registered casts
            from ..casts import cast_registry as enhanced_registry

            cast_instance = enhanced_registry.get_cast_instance(
                self.__casts__[attribute]
            )
            if cast_instance:
                return cast_instance.get(value)
        return value

    def all_attributes(self):
        attributes = {**self.__attributes__, **self.get_dirty_attributes()}
        for key, value in list(attributes.items()):
            if key in self.__casts__:
                attributes[key] = self._cast_attribute(key, value)

        return attributes

    def delete_attribute(self, key):
        if key in self.__attributes__:
            del self.__attributes__[key]
            return True

        return False

    def get_cast_map(self):
        cast_map = self.__internal_cast_map__
        cast_map.update(self.__cast_map__)
        return cast_map

    def _cast_attribute(self, attribute, value):
        cast_method = self.__casts__[attribute]
        cast_map = self.get_cast_map()

        if value is None:
            return None

        if isinstance(cast_method, str):
            # Handle parametrized casts
            if ":" in cast_method:
                cast_type, cast_params = cast_method.split(":", 1)

                if cast_type in cast_map:
                    if cast_type == "datetime":
                        parts = cast_params.split(",")
                        format_str = parts[0] if parts else None
                        timezone = parts[1].strip() if len(parts) > 1 else "UTC"
                        return cast_map[cast_type](format_str, timezone).get(value)
                    elif cast_type == "decimal":
                        precision = int(cast_params) if cast_params.isdigit() else 2
                        return cast_map[cast_type](precision).get(value)
                    # array / hash / other single-param casts share the same path
                    return cast_map[cast_type](cast_params).get(value)

            elif cast_method in cast_map:
                return cast_map[cast_method]().get(value)

        return cast_method(value)

    def __getitem__(self, attribute):
        return getattr(self, attribute)

    def get_dates(self):
        """
        Get the attributes that should be converted to dates.

        :rtype: list
        """
        defaults = [
            self.date_created_at,
            self.date_updated_at,
        ]

        return self.__dates__ + defaults

    def get_new_date(self, _datetime=None):
        """
        Get the attributes that should be converted to dates.

        :rtype: list
        """

        if not _datetime:
            return pendulum.now("UTC")
        elif isinstance(_datetime, str):
            return pendulum.parse(_datetime, tz="UTC")
        elif isinstance(_datetime, datetime):
            return pendulum.instance(_datetime, tz="UTC")
        elif isinstance(_datetime, datetimedate):
            return pendulum.datetime(
                _datetime.year,
                _datetime.month,
                _datetime.day,
                tz="UTC",
            )
        elif isinstance(_datetime, datetimetime):
            return pendulum.parse(
                f"{_datetime.hour}:{_datetime.minute}:{_datetime.second}",
                tz="UTC",
            )

        return pendulum.instance(_datetime, tz="UTC")

    def get_new_datetime_string(self, _datetime=None):
        """
        Given an optional datetime value, constructs and returns a new datetime string. If no
        datetime is specified, returns the current time.

        :rtype: list
        """
        return self.get_new_date(_datetime).to_datetime_string()

    def get_new_serialized_date(self, _datetime):
        """
        Get the attributes that should be converted to dates.

        :rtype: list
        """
        return self.get_new_date(_datetime).to_datetime_string()

    def _convert_date_to_utc_for_database(self, value):
        """
        Convert date value to UTC for database storage.

        Args:
            value: Date value in user timezone

        Returns:
            Date value converted to UTC
        """
        try:
            from cara.eloquent.utils.DateManager import DateManager

            # Get user timezone from config
            user_timezone = self._get_user_timezone()

            # Convert to UTC for database storage
            converted_date = DateManager.to_utc_for_database(value, user_timezone)

            return converted_date.to_datetime_string() if converted_date else value
        except ImportError:
            # Fallback if DateManager not available
            return value

    def _get_user_timezone(self) -> str:
        """Get user timezone from config or request context."""
        try:
            # Try to get from config first
            from config.app import APP_TIMEZONE

            return APP_TIMEZONE
        except ImportError:
            # Fallback to UTC
            return "UTC"

    def set_appends(self, appends):
        """
        Get the attributes that should be converted to dates.

        :rtype: list
        """
        self.__appends__ += appends
        return self

    def save_many(self, relation, relating_records):
        if isinstance(relating_records, Model):
            raise ValueError(
                "Saving many records requires an iterable like a collection or a list of models and not a Model object. To attach a model, use the 'attach' method."
            )

        for related_record in relating_records:
            self.attach(relation, related_record)

    def detach_many(self, relation, relating_records):
        if isinstance(relating_records, Model):
            raise ValueError(
                "Detaching many records requires an iterable like a collection or a list of models and not a Model object. To detach a model, use the 'detach' method."
            )

        related = getattr(self.__class__, relation)
        for related_record in relating_records:
            if not related_record.is_created():
                related_record = related_record.create(related_record.all_attributes())
            else:
                related_record.save()

            related.detach(self, related_record)

    def related(self, relation):
        related = getattr(self.__class__, relation)
        return related.relate(self)

    def get_related(self, relation):
        if hasattr(self, "_relations") and relation in self._relations:
            return self._relations[relation]
        return getattr(self.__class__, relation)

    def attach(self, relation, related_record):
        related = getattr(self.__class__, relation)
        return related.attach(self, related_record)

    def detach(self, relation, related_record):
        related = getattr(self.__class__, relation)

        if not related_record.is_created():
            related_record = related_record.create(related_record.all_attributes())
        else:
            related_record.save()

        return related.detach(self, related_record)

    def save_quietly(self):
        """
        This method calls the save method on a model without firing the saved & saving observer
        events. Saved/Saving are toggled back on once save_quietly has been ran.

        Instead of calling:

        User().save(...)

        you can use this:

        User.save_quietly(...)
        """
        self.without_events()
        saved = self.save()
        self.with_events()
        return saved

    def delete_quietly(self):
        """This method calls the delete method on a model without firing the delete & deleting observer events.
        Instead of calling:

        User().delete(...)

        you can use this:

        User.delete_quietly(...)

        Returns:
            self
        """
        delete = (
            self.without_events()
            .where(
                self.get_primary_key(),
                self.get_primary_key_value(),
            )
            .delete()
        )
        self.with_events()
        return delete

    def attach_related(self, relation, related_record):
        return self.attach(relation, related_record)

    @classmethod
    def filter_fillable(cls, dictionary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filters provided dictionary to only include fields specified in the model's __fillable__
        property.

        Passed dictionary is not mutated.
        """
        if cls.__fillable__ != ["*"]:
            dictionary = {x: dictionary[x] for x in cls.__fillable__ if x in dictionary}
        return dictionary

    @classmethod
    def filter_mass_assignment(cls, dictionary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filters the provided dictionary in preparation for a mass-assignment operation.

        Wrapper around filter_fillable() & filter_guarded(). Passed dictionary is not mutated.
        """
        return cls.filter_guarded(cls.filter_fillable(dictionary))

    @classmethod
    def filter_guarded(cls, dictionary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filters provided dictionary to exclude fields specified in the model's __guarded__ property.

        Passed dictionary is not mutated.
        """
        if cls.__guarded__ == ["*"]:
            # If all fields are guarded, all data should be filtered
            return {}
        return {f: dictionary[f] for f in dictionary if f not in cls.__guarded__}

    @classmethod
    def upsert(
        cls,
        values: List[Dict[str, Any]],
        unique_by: List[str],
        update: Optional[List[str]] = None,
        cast: bool = True,
    ):
        """
        Insert new records or update existing ones.

        Args:
            values: List of dictionaries with data to insert/update
            unique_by: List of column names that determine uniqueness
            update: List of column names to update on conflict (if None, updates all except unique_by)
            cast: Whether to apply model casts

        Returns:
            Number of affected rows

        Example:
            Receipt.upsert([
                {"receipt_id": "123", "status": "processed", "amount": 100},
                {"receipt_id": "124", "status": "pending", "amount": 200}
            ], unique_by=["receipt_id"], update=["status", "amount"])
        """
        # Create instance and call through passthrough mechanism
        instance = cls()

        # Use get_builder() to avoid boot() cycle and directly call upsert
        builder = instance.get_builder()
        return builder.upsert(
            values=values,
            unique_by=unique_by,
            update=update,
            cast=cast,
        )
