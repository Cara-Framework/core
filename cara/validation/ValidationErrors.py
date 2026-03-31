"""
Validation Errors for the Cara framework.

This module provides the ValidationErrors class to handle validation errors in Laravel style.
"""

from typing import Dict, List


class ValidationErrors:
    """Helper class to handle validation errors in Laravel style."""

    def __init__(self, errors: Dict[str, list[str]]):
        self._errors = errors

    def first(self, field: str = None) -> str:
        """Get the first error message for a field, or the first error overall."""
        if field:
            field_errors = self._errors.get(field, [])
            return field_errors[0] if field_errors else ""

        for field_errors in self._errors.values():
            if field_errors:
                return field_errors[0]
        return ""

    def first_error(self) -> str:
        """Get the first error message overall (alias for first() without field)."""
        return self.first()

    def all(self) -> Dict[str, list[str]]:
        """Get all error messages."""
        return self._errors.copy()

    def errors(self) -> Dict[str, list[str]]:
        """Get all error messages (Laravel-style alias for all())."""
        return self.all()

    def has(self, field: str) -> bool:
        """Check if field has errors."""
        return field in self._errors and bool(self._errors[field])

    def get(self, field: str) -> List[str]:
        """Get all error messages for a specific field."""
        return self._errors.get(field, [])

    def count(self, field: str = None) -> int:
        """Count errors for a field or total error count."""
        if field:
            return len(self._errors.get(field, []))
        return sum(len(field_errors) for field_errors in self._errors.values())

    def messages(self) -> List[str]:
        """Get all error messages as a flat list."""
        all_messages = []
        for field_errors in self._errors.values():
            all_messages.extend(field_errors)
        return all_messages

    def keys(self) -> List[str]:
        """Get all field names that have errors."""
        return list(self._errors.keys())

    def empty(self) -> bool:
        """Check if there are no errors."""
        return not bool(self._errors)

    def any(self) -> bool:
        """Check if there are any errors."""
        return bool(self._errors)

    def only(self, *fields: str) -> Dict[str, List[str]]:
        """Get errors for only specified fields."""
        return {
            field: self._errors.get(field, [])
            for field in fields
            if field in self._errors
        }

    def except_(self, *fields: str) -> Dict[str, List[str]]:
        """Get errors except for specified fields."""
        return {
            field: errors for field, errors in self._errors.items() if field not in fields
        }

    def to_dict(self) -> Dict[str, List[str]]:
        """Convert to dictionary (alias for all())."""
        return self.all()

    def to_json(self) -> str:
        """Convert errors to JSON string."""
        import json

        return json.dumps(self._errors)

    def __str__(self) -> str:
        """String representation showing all errors."""
        if not self._errors:
            return "No validation errors"

        lines = []
        for field, field_errors in self._errors.items():
            for error in field_errors:
                lines.append(f"{field}: {error}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        """Representation for debugging."""
        return f"ValidationErrors({self._errors})"

    def __bool__(self) -> bool:
        """Boolean conversion - True if there are errors."""
        return bool(self._errors)

    def __len__(self) -> int:
        """Length - total number of error messages."""
        return self.count()

    def __iter__(self):
        """Iterate over field names."""
        return iter(self._errors.keys())

    def __contains__(self, field: str) -> bool:
        """Check if field has errors using 'in' operator."""
        return self.has(field)
