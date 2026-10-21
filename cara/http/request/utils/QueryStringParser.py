"""
Query String Parser Utility.

This module provides a utility class for parsing query strings with support for nested data
structures, arrays, and Laravel-style bracket notation.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any
from urllib.parse import unquote_plus

from .KeyPart import KeyPart

# Upper bound on list PADDING per parse. An index past the end of its
# list (``tags[3]`` before ``tags[0..2]``) pads the gap so the value
# lands at its position — allocation no input byte pays for. A
# per-segment cap alone still let one chained key
# (``a[10000]a[10000]...``) or many keys (``k0[10000]=1&k1[10000]=1``)
# allocate 10k slots per segment: a 15 KB query cost ~1.4 GB before
# routing. One budget per parse bounds the total; an index whose gap
# exceeds what is left is kept as an object key (``{'5000': x}``), so
# the cost stays linear in the input.
_MAX_PAD_SLOTS = 1_000

# A longer digit run is an object key, never a list position: no body
# under the request size limit holds a billion list entries.
_MAX_INDEX_DIGITS = 9


class QueryStringParser:
    """
    Utility class for parsing query strings into nested data structures.

    Supports Laravel-style bracket notation for arrays and nested objects:
    - foo[bar]=baz -> {'foo': {'bar': 'baz'}}
    - items[]=a&items[]=b -> {'items': ['a', 'b']}
    - tags[0]=python&tags[1]=web -> {'tags': ['python', 'web']}
    """

    def parse(self, query_string: str) -> dict[str, Any]:
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
        processed_dict: dict[str, Any] = {}
        for key, values in flat_dict.items():
            if key.endswith("[]"):
                # Array syntax: key[] => always store as list
                processed_dict[key] = values
            else:
                # Single value takes the first item
                processed_dict[key] = values[0]

        # Parse nested structures
        return self._parse_nested_input(processed_dict)

    def _parse_nested_input(self, flat_dict: dict[str, Any]) -> dict[str, Any]:
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
        result: dict[str, Any] = {}

        pad_budget = _MAX_PAD_SLOTS
        for key, value in flat_dict.items():
            pad_budget = self._set_nested_value(result, key, value, pad_budget)

        return result

    def _set_nested_value(
        self, result: dict[str, Any], key: str, value: Any, pad_budget: int
    ) -> int:
        """
        Set a nested value in the result dictionary based on the key path.

        Args:
            result: Target dictionary to set value in
            key: Key path (may contain bracket notation)
            value: Value to set
            pad_budget: List padding slots this parse may still allocate

        Returns:
            The padding budget left after this key
        """
        # Extract path parts using regex
        parts = self._parse_key_path(key)
        if not parts:
            return pad_budget

        current = result

        # Navigate through all parts except the last. Every step guards
        # against a TYPE CONFLICT with an earlier key: a crafted query
        # like ``?a=1&a[b]=2`` used to descend into the string "1" and
        # blow up with an unhandled TypeError on assignment — a 500 on
        # any endpoint. Conflicting structure is replaced (last key
        # wins, PHP/Laravel semantics), never assigned into.
        for part in parts[:-1]:
            if not part.is_array_key:
                current = self._descend(current, part.name)
                continue

            padding = self._padding_for(current.get(part.name), part.index)
            if padding > pad_budget:
                # Over budget: the index is an object key (_MAX_PAD_SLOTS).
                current = self._descend(
                    self._descend(current, part.name), str(part.index)
                )
                continue

            pad_budget -= padding
            if not isinstance(current.get(part.name), list):
                current[part.name] = []
            items = current[part.name]
            while len(items) <= part.index:
                items.append({})

            node = items[part.index]
            if not isinstance(node, dict):
                node = {}
                items[part.index] = node
            current = node

        # Set the final value
        last_part = parts[-1]
        is_array = key.endswith("[]")

        if last_part.is_array_key and not is_array:
            padding = self._padding_for(current.get(last_part.name), last_part.index)
            if padding > pad_budget:
                # Over budget: the index is an object key (_MAX_PAD_SLOTS).
                self._descend(current, last_part.name)[str(last_part.index)] = value
                return pad_budget
            pad_budget -= padding

        if last_part.is_array_key or is_array:
            # Final part is an array
            if not isinstance(current.get(last_part.name), list):
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

        return pad_budget

    @staticmethod
    def _padding_for(existing: Any, index: int) -> int:
        """Return the filler slots placing ``index`` in ``existing`` costs."""
        length = len(existing) if isinstance(existing, list) else 0
        return max(0, index - length)

    @staticmethod
    def _descend(current: dict[str, Any], name: str) -> dict[str, Any]:
        """Return the object node under ``name``, replacing a conflicting value."""
        node = current.get(name)
        if not isinstance(node, dict):
            node = {}
            current[name] = node
        return node

    def _parse_key_path(self, key: str) -> list:
        """
        Parse a key path into structured parts.

        Args:
            key: Key path like 'foo[bar][0][baz]' or 'items[]'

        Returns:
            List of KeyPart objects representing the path
        """
        parts = []

        # Trailing array notation (key[]): strip it and parse the base
        # normally — treating the whole base as ONE part swallowed any
        # nesting (``filters[brand][]`` stayed a literal 'filters[brand]'
        # top-level key). The trailing [] itself is applied by
        # _set_nested_value via ``key.endswith("[]")``.
        if key.endswith("[]"):
            key = key[:-2]
            if not key:
                return parts

        # finditer (not findall): findall collapses "no bracket group" to
        # the same "" as literal empty brackets, which misparsed a bare
        # trailing segment like ``users[0][name]`` into an ARRAY part —
        # values landed wrapped in a stray list ({'name': ['John']}).
        for match in re.finditer(r"([^\[\]]+)(?:\[([^\[\]]*)\])?", key):
            name = match.group(1)
            index_str = match.group(2)
            if index_str is None:
                # No brackets (regular key)
                parts.append(KeyPart(name, is_array_key=False))
            elif (
                index_str.isascii()
                and index_str.isdigit()
                and len(index_str) <= _MAX_INDEX_DIGITS
            ):
                # Numeric index. ASCII only and length-capped: ``isdigit``
                # also accepts digits ``int`` rejects (``a[²]``), and
                # ``int`` refuses 4300+ digits — both raised ValueError
                # before routing. A large index costs nothing here;
                # padding is budgeted where it is allocated.
                parts.append(KeyPart(name, is_array_key=True, index=int(index_str)))
            elif index_str == "":
                # Empty brackets (array notation)
                parts.append(KeyPart(name, is_array_key=True, index=0))
            else:
                # Non-numeric index (treat as nested object key)
                parts.append(KeyPart(name, is_array_key=False))
                parts.append(KeyPart(index_str, is_array_key=False))

        return parts
