"""
Laravel-style SerializesModels for Cara Framework.

This mixin provides automatic serialization/deserialization of models and complex objects
for queue jobs, avoiding circular reference issues.
"""

from typing import Any, Dict


class SerializesModels:
    """
    Mixin that provides Laravel-style model serialization for queue jobs.

    Automatically serializes complex objects (models, notifications, etc.) by:
    1. Storing only essential data (type, id, simple attributes)
    2. Reconstructing objects when job runs
    3. Avoiding circular references and pickle issues
    """

    def __getstate__(self) -> Dict[str, Any]:
        """
        Custom serialization for queue jobs.

        Returns:
            Serialized state dictionary
        """
        state = {}

        for key, value in self.__dict__.items():
            state[key] = self._serialize_property(value)

        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """
        Custom deserialization for queue jobs.

        Args:
            state: Serialized state dictionary
        """
        for key, value in state.items():
            setattr(self, key, self._deserialize_property(value))

    def _serialize_property(self, value: Any) -> Any:
        """
        Serialize a single property value.

        Args:
            value: Property value to serialize

        Returns:
            Serialized value
        """
        # Handle None
        if value is None:
            return None

        # Handle basic types
        if isinstance(value, (str, int, float, bool)):
            return value

        # Handle lists and tuples
        if isinstance(value, (list, tuple)):
            return [self._serialize_property(item) for item in value]

        # Handle dictionaries
        if isinstance(value, dict):
            return {k: self._serialize_property(v) for k, v in value.items()}

        # Handle objects with notification-like interface
        if hasattr(value, "__class__") and hasattr(value, "__dict__"):
            return self._serialize_object(value)

        # Handle classes (like notification.__class__)
        if isinstance(value, type):
            return {
                "__class_module__": value.__module__,
                "__class_name__": value.__name__,
                "__is_class__": True,
            }

        # Fallback: convert to string
        try:
            return str(value)
        except:
            return None

    def _serialize_object(self, obj: Any) -> Dict[str, Any]:
        """
        Serialize a complex object.

        Args:
            obj: Object to serialize

        Returns:
            Serialized object data
        """
        serialized = {
            "__class_module__": obj.__class__.__module__,
            "__class_name__": obj.__class__.__name__,
            "__is_object__": True,
        }

        # Get object attributes, avoiding problematic ones
        for key, value in obj.__dict__.items():
            if key.startswith("_"):
                continue  # Skip private attributes

            if key in ["application", "manager", "container"]:
                continue  # Skip framework references

            try:
                serialized[key] = self._serialize_property(value)
            except:
                # Skip if can't serialize
                continue

        return serialized

    def _deserialize_property(self, value: Any) -> Any:
        """
        Deserialize a property value.

        Args:
            value: Serialized value

        Returns:
            Deserialized value
        """
        if value is None:
            return None

        # Handle basic types
        if isinstance(value, (str, int, float, bool)):
            return value

        # Handle lists
        if isinstance(value, list):
            return [self._deserialize_property(item) for item in value]

        # Handle dictionaries
        if isinstance(value, dict):
            # Check if it's a serialized class
            if value.get("__is_class__"):
                return self._deserialize_class(value)

            # Check if it's a serialized object
            if value.get("__is_object__"):
                return self._deserialize_object(value)

            # Regular dictionary
            return {k: self._deserialize_property(v) for k, v in value.items()}

        return value

    def _deserialize_class(self, data: Dict[str, Any]) -> type:
        """
        Deserialize a class reference.

        Args:
            data: Serialized class data

        Returns:
            Class object
        """
        try:
            module_name = data["__class_module__"]
            class_name = data["__class_name__"]

            module = __import__(module_name, fromlist=[class_name])
            return getattr(module, class_name)
        except:
            # Fallback: return a dummy class
            return type("DummyClass", (), {})

    def _deserialize_object(self, data: Dict[str, Any]) -> Any:
        """
        Deserialize a complex object.

        Args:
            data: Serialized object data

        Returns:
            Reconstructed object
        """
        try:
            # Get the class
            cls = self._deserialize_class(data)

            # Create instance with minimal data
            obj_data = {k: v for k, v in data.items() if not k.startswith("__")}

            # Try to create instance with constructor args
            if hasattr(cls, "__init__"):
                try:
                    # Try with empty constructor
                    obj = cls()
                except:
                    try:
                        # Try with data as dict
                        obj = cls(obj_data)
                    except:
                        # Create minimal instance
                        obj = cls.__new__(cls)

            # Set attributes
            for key, value in obj_data.items():
                try:
                    setattr(obj, key, self._deserialize_property(value))
                except:
                    continue

            return obj

        except Exception:
            # Fallback: create a mock object
            class MockObject:
                def __init__(self, data):
                    for k, v in data.items():
                        if not k.startswith("__"):
                            setattr(self, k, v)

            return MockObject(data)
