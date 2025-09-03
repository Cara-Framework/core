"""
HTTP Input Management Module.

This module provides classes for managing HTTP request input data in the Cara framework,
implementing input handling with support for nested data, arrays, and dot notation
access.
"""

from typing import Any, Dict, TypeVar

from cara.http.request.utils.QueryParser import QueryStringParser
from cara.support.Structures import data_get

T = TypeVar("T", bound="InputBag")


class Input:
    """
    Single input value representation.

    This class represents a single input value from an HTTP request, providing access to both the
    input name and its value.
    """

    def __init__(self, name: str, value: Any):
        """
        Initialize an input.

        Args:
            name: Input name
            value: Input value
        """
        self.name = name
        self.value = value

    def __str__(self) -> str:
        """String representation of input."""
        return f"{self.name}={self.value}"

    def __repr__(self) -> str:
        """Debug representation of input."""
        return f"Input(name='{self.name}', value={repr(self.value)})"


class InputBag:
    """
    Container for managing HTTP request input data.

    This class provides a container for managing input data from HTTP requests with support for
    nested data structures, arrays, and dot notation access.
    """

    def __init__(self):
        """Initialize an empty input bag."""
        self._data: Dict[str, Any] = {}
        self._parser = QueryStringParser()

    @classmethod
    def from_query_string(cls: type[T], query: str) -> T:
        """
        Create InputBag from query string.

        Args:
            query: Raw query string

        Returns:
            New InputBag instance with parsed query data
        """
        bag = cls()
        bag.load_query_string(query)
        return bag

    def load_query_string(self, query_string: str) -> None:
        """
        Load and parse query string parameters.

        Args:
            query_string: Raw query string from URL
        """
        if not query_string:
            return

        parsed_data = self._parser.parse(query_string)
        self._data.update(parsed_data)

    def get(
        self,
        name: str,
        default: Any = None,
        flatten: bool = True,
    ) -> Any:
        """
        Get input value by name with support for dot notation.

        Args:
            name: Input name (supports dot notation)
            default: Default value if not found
            flatten: Whether to flatten single-item lists

        Returns:
            Input value or default
        """
        cleaned_name = name[:-2] if name.endswith("[]") else name
        value = data_get(
            self._data,
            cleaned_name,
            [] if name.endswith("[]") else default,
        )

        if (
            flatten
            and isinstance(value, list)
            and len(value) == 1
            and not name.endswith("[]")
        ):
            return value[0]

        return value

    def set(self, name: str, value: Any) -> None:
        """
        Set an input value.

        Args:
            name: Input name (supports dot notation)
            value: Input value
        """
        if "." in name:
            # Handle nested setting
            keys = name.split(".")
            current = self._data
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            current[keys[-1]] = value
        else:
            self._data[name] = value

    def has(self, *names: str) -> bool:
        """
        Check if all specified inputs exist.

        Args:
            *names: Input names to check

        Returns:
            True if all inputs exist
        """
        return all(data_get(self._data, name) is not None for name in names)

    def missing(self, *names: str) -> bool:
        """
        Check if any specified inputs are missing.

        Args:
            *names: Input names to check

        Returns:
            True if any inputs are missing
        """
        return not self.has(*names)

    def filled(self, *names: str) -> bool:
        """
        Check if all specified inputs are filled (not None or empty string).

        Args:
            *names: Input names to check

        Returns:
            True if all inputs are filled
        """
        for name in names:
            value = self.get(name)
            if value is None or value == "":
                return False
        return True

    def all(self) -> Dict[str, Any]:
        """
        Get all inputs.

        Returns:
            Dictionary of all inputs
        """
        return self._data

    def all_as_values(self, internal_variables: bool = False) -> Dict[str, Any]:
        """
        Get all inputs as raw values.

        Args:
            internal_variables: Include internal vars starting with __

        Returns:
            Dictionary of input values
        """
        if not internal_variables:
            return {k: v for k, v in self._data.items() if not k.startswith("__")}
        return self._data.copy()

    def only(self, *names: str) -> Dict[str, Any]:
        """
        Get only specified inputs.

        Args:
            *names: Input names to include

        Returns:
            Dictionary with only specified inputs
        """
        result = {}
        for name in names:
            value = self.get(name)
            if value is not None:
                result[name] = value
        return result

    def except_(self, *names: str) -> Dict[str, Any]:
        """
        Get all inputs except specified ones.

        Args:
            *names: Input names to exclude

        Returns:
            Dictionary excluding specified inputs
        """
        excluded = set(names)
        return {k: v for k, v in self._data.items() if k not in excluded}

    def keys(self) -> list:
        """Get all input names."""
        return list(self._data.keys())

    def values(self) -> list:
        """Get all input values."""
        return list(self._data.values())

    def items(self) -> list:
        """Get all input items as (name, value) tuples."""
        return list(self._data.items())

    def update(self, data: Dict[str, Any]) -> None:
        """
        Update input data with deep merging.

        Args:
            data: New data to merge
        """
        self._data = self._merge_dicts(self._data, data)

    def clear(self) -> None:
        """Clear all input data."""
        self._data.clear()

    def __len__(self) -> int:
        """Return number of inputs."""
        return len(self._data)

    def __contains__(self, name: str) -> bool:
        """Check if input exists."""
        return self.has(name)

    def __getitem__(self, name: str) -> Any:
        """Get input value by name."""
        return self.get(name)

    def __setitem__(self, name: str, value: Any) -> None:
        """Set input value by name."""
        self.set(name, value)

    def __str__(self) -> str:
        """String representation of input bag."""
        return f"InputBag({len(self._data)} inputs)"

    def __repr__(self) -> str:
        """Debug representation of input bag."""
        return f"InputBag({self._data})"

    @staticmethod
    def _merge_dicts(d1: Dict[str, Any], d2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.

        Args:
            d1: First dictionary
            d2: Second dictionary to merge into first

        Returns:
            Merged dictionary
        """
        result = d1.copy()
        for k, v in d2.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = InputBag._merge_dicts(result[k], v)
            elif k in result and isinstance(result[k], list) and isinstance(v, list):
                result[k].extend(v)
            else:
                result[k] = v
        return result
