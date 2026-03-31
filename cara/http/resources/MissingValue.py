"""Sentinel type for conditional resource attributes."""


class MissingValue:
    """Represents an attribute that should be excluded from the resource output.

    Used by JsonResource.when() and JsonResource.when_loaded() to signal
    that a field should not appear in the serialized output.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __bool__(self):
        return False

    def __repr__(self):
        return "MissingValue()"
