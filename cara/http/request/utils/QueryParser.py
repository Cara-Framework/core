"""
Query String Parser Utility.

This module provides a utility class for parsing query strings with support for nested data
structures, arrays, and Laravel-style bracket notation.
"""

import re
from collections import defaultdict
from typing import Any, Dict
from urllib.parse import unquote_plus


class QueryStringParser:
    """
    Utility class for parsing query strings into nested data structures.

    Supports Laravel-style bracket notation for arrays and nested objects:
    - foo[bar]=baz -> {'foo': {'bar': 'baz'}}
    - items[]=a&items[]=b -> {'items': ['a', 'b']}
    - tags[0]=python&tags[1]=web -> {'tags': ['python', 'web']}
    """

    def parse(self, query_string: str) -> Dict[str, Any]:
        """
        Parse query string into nested data structure.

        Args:
            query_string: Raw query string from URL

        Returns:
            Parsed data as nested dictionary
        """
        if not query_string:
            return {}

        # Split into key-value pairs
        pairs = []
        for pair in query_string.split("&"):
            if "=" in pair:
                key, value = pair.split("=", 1)
                # URL decode key and value
                key = unquote_plus(key)
                value = unquote_plus(value)
                pairs.append((key, value))

        # Group values by key
        flat_dict = defaultdict(list)
        for key, value in pairs:
            flat_dict[key].append(value)

        # Process array notation and single values
        processed_dict: Dict[str, Any] = {}
        for key, values in flat_dict.items():
            if key.endswith("[]"):
                # Array syntax: key[] => always store as list
                processed_dict[key] = values
            else:
                # Single value takes the first item
                processed_dict[key] = values[0]

        # Parse nested structures
        return self._parse_nested_input(processed_dict)

    def _parse_nested_input(self, flat_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse flat dictionary with bracket notation into nested structure.

        Converts:
            {'foo[bar]': 'baz'} -> {'foo': {'bar': 'baz'}}
            {'items[]': ['a','b']} -> {'items': ['a', 'b']}
            {'tag[]': 'python'} -> {'tag': ['python']}
            {'users[0][name]': 'John'} -> {'users': [{'name': 'John'}]}

        Args:
            flat_dict: Flat dictionary with bracket notation

        Returns:
            Nested dictionary structure
        """
        result: Dict[str, Any] = {}

        for key, value in flat_dict.items():
            self._set_nested_value(result, key, value)

        return result

    def _set_nested_value(self, result: Dict[str, Any], key: str, value: Any) -> None:
        """
        Set a nested value in the result dictionary based on the key path.

        Args:
            result: Target dictionary to set value in
            key: Key path (may contain bracket notation)
            value: Value to set
        """
        # Extract path parts using regex
        parts = self._parse_key_path(key)
        if not parts:
            return

        current = result

        # Navigate through all parts except the last
        for i, part in enumerate(parts[:-1]):
            next_part = parts[i + 1] if i + 1 < len(parts) else None

            if part.is_array_key:
                # Current part is an array
                if part.name not in current:
                    current[part.name] = []

                # Ensure we have enough array elements
                while len(current[part.name]) <= part.index:
                    current[part.name].append({})

                current = current[part.name][part.index]

            else:
                # Current part is an object key
                if part.name not in current:
                    # Determine what type to create based on next part
                    if next_part and next_part.is_array_key:
                        current[part.name] = []
                    else:
                        current[part.name] = {}

                current = current[part.name]

        # Set the final value
        last_part = parts[-1]
        is_array = key.endswith("[]")

        if last_part.is_array_key or is_array:
            # Final part is an array
            if last_part.name not in current:
                current[last_part.name] = []

            if is_array:
                # Array notation without index (key[])
                if isinstance(value, list):
                    current[last_part.name].extend(value)
                else:
                    current[last_part.name].append(value)
            else:
                # Array notation with index (key[0])
                while len(current[last_part.name]) <= last_part.index:
                    current[last_part.name].append(None)
                current[last_part.name][last_part.index] = value
        else:
            # Final part is a regular key
            if (
                last_part.name in current
                and isinstance(current[last_part.name], dict)
                and isinstance(value, dict)
            ):
                current[last_part.name].update(value)
            else:
                current[last_part.name] = value

    def _parse_key_path(self, key: str) -> list:
        """
        Parse a key path into structured parts.

        Args:
            key: Key path like 'foo[bar][0][baz]' or 'items[]'

        Returns:
            List of KeyPart objects representing the path
        """
        parts = []

        # Handle array notation at the end (key[])
        if key.endswith("[]"):
            key = key[:-2]
            parts.append(KeyPart(key, is_array_key=True, index=0))
            return parts

        # Use regex to find all key parts
        pattern = r"([^\[\]]+)(?:\[([^\[\]]*)\])?"
        matches = re.findall(pattern, key)

        for match in matches:
            name, index_str = match
            if index_str.isdigit():
                # Numeric index
                parts.append(KeyPart(name, is_array_key=True, index=int(index_str)))
            elif index_str == "":
                # Empty brackets (array notation)
                parts.append(KeyPart(name, is_array_key=True, index=0))
            elif index_str:
                # Non-numeric index (treat as nested object key)
                parts.append(KeyPart(name, is_array_key=False))
                parts.append(KeyPart(index_str, is_array_key=False))
            else:
                # No brackets (regular key)
                parts.append(KeyPart(name, is_array_key=False))

        return parts


class KeyPart:
    """
    Represents a part of a key path in query string parsing.

    Examples:
    - 'foo' -> KeyPart(name='foo', is_array_key=False)
    - 'items[0]' -> KeyPart(name='items', is_array_key=True, index=0)
    """

    def __init__(self, name: str, is_array_key: bool = False, index: int = 0):
        self.name = name
        self.is_array_key = is_array_key
        self.index = index

    def __repr__(self) -> str:
        if self.is_array_key:
            return f"KeyPart(name='{self.name}', array[{self.index}])"
        return f"KeyPart(name='{self.name}', object)"
