"""Filtering and nested-value operations for ``Collection``."""

from __future__ import annotations

import builtins
import operator

from dotty_dict import Dotty

from cara.exceptions import InvalidArgumentException
from cara.support.Structures import data_get


def _collection_transform(self, callback):
    """
    Transforms each item in the collection using the callback.

    Args:
        callback: The transformation callback.

    Returns:
        The collection instance.
    """
    self._check_is_callable(callback)

    for i, item in enumerate(self._items):
        self._items[i] = callback(item)

    return self


def _collection_unique(self, key=None):
    """
    Returns all of the unique items in the collection.

    Args:
        key: The key to check uniqueness by.

    Returns:
        A new Collection instance with the unique items.
    """
    if not key:
        items = list(set(self._items))
        return self.__class__(items)

    keys = set()
    items = []
    if isinstance(self.all(), dict):
        return self

    for item in self:
        if isinstance(item, dict):
            comparison = item.get(key)
        elif isinstance(item, str):
            comparison = item
        else:
            comparison = getattr(item, key)
        if comparison not in keys:
            items.append(item)
            keys.add(comparison)

    return self.__class__(items)


def _collection_duplicates(self, key=None):
    """
    Returns all duplicate items in the collection.

    Args:
        key: The key to check duplicates by.

    Returns:
        A new Collection instance with the duplicate items.
    """
    if not key:
        seen = set()
        duplicates = set()
        for item in self._items:
            if item in seen:
                duplicates.add(item)
            else:
                seen.add(item)
        return self.__class__(list(duplicates))

    seen = set()
    duplicates = []
    duplicate_keys = set()

    for item in self:
        if isinstance(item, dict):
            comparison = item.get(key)
        elif isinstance(item, str):
            comparison = item
        else:
            comparison = getattr(item, key, None)

        if comparison in seen and comparison not in duplicate_keys:
            duplicates.append(item)
            duplicate_keys.add(comparison)
        else:
            seen.add(comparison)

    return self.__class__(duplicates)


def _collection_where(self, key, *args):
    """
    Filters the collection by the given key-value pair.

    Supports dot notation and wildcards like "product.*.name" or "*.price".

    Args:
        key: The key to filter by (supports dot notation and wildcards).
        *args: The operator and value to filter by.

    Returns:
        A new Collection instance with the filtered items.
    """
    op = "=="
    value = args[0] if args else None

    if len(args) >= 2:
        op = args[0]
        value = args[1]

    attributes = []

    for item in self._items:
        if "*" in str(key):
            # Handle wildcards - check if any wildcard match satisfies the condition
            wildcard_values = self._data_get_with_wildcards(item, key)
            if any(self._make_comparison(val, value, op) for val in wildcard_values):
                attributes.append(item)
        else:
            # Regular path
            comparison = self._data_get(item, key)
            if self._make_comparison(comparison, value, op):
                attributes.append(item)

    return self.__class__(attributes)


def _collection_where_in(self, key, values):
    """
    Filters the collection by the given key-value pairs.

    Supports dot notation and wildcards like "product.*.name" or "*.price".

    Args:
        key: The key to filter by (supports dot notation and wildcards).
        values: The values to filter by.

    Returns:
        A new Collection instance with the filtered items.
    """
    values = self._get_items(values)

    if "*" in str(key):
        # Handle wildcards
        return self.__class__(
            [
                item
                for item in self._items
                if any(val in values for val in self._data_get_with_wildcards(item, key))
            ]
        )
    else:
        # Regular path
        return self.__class__(
            [item for item in self._items if self._data_get(item, key) in values]
        )


def _collection_where_not_in(self, key, values):
    """
    Filters the collection by the given key-value pairs, removing matching items.

    Supports dot notation and wildcards like "product.*.name" or "*.price".

    Args:
        key: The key to filter by (supports dot notation and wildcards).
        values: The values to filter by.

    Returns:
        A new Collection instance with the filtered items.
    """
    values = self._get_items(values)

    if "*" in str(key):
        # Handle wildcards - exclude items where ANY wildcard value is in the values list
        return self.__class__(
            [
                item
                for item in self._items
                if not any(
                    val in values for val in self._data_get_with_wildcards(item, key)
                )
            ]
        )
    else:
        # Regular path
        return self.__class__(
            [item for item in self._items if self._data_get(item, key) not in values]
        )


def _collection_where_between(self, key, values):
    """
    Filters the collection by determining if a specified item value is within a given range.

    Args:
        key: The key to filter by.
        values: The range of values.

    Returns:
        A new Collection instance with the filtered items.
    """
    if len(values) != 2:
        raise InvalidArgumentException("Values must be an array with exactly 2 elements")

    min_value, max_value = values

    return self.__class__(
        [
            item
            for item in self._items
            if min_value <= self._data_get(item, key) <= max_value
        ]
    )


def _collection_where_not_between(self, key, values):
    """
    Filters the collection by determining if a specified item value is outside a given range.

    Args:
        key: The key to filter by.
        values: The range of values.

    Returns:
        A new Collection instance with the filtered items.
    """
    if len(values) != 2:
        raise InvalidArgumentException("Values must be an array with exactly 2 elements")

    min_value, max_value = values

    return self.__class__(
        [
            item
            for item in self._items
            if self._data_get(item, key) < min_value
            or self._data_get(item, key) > max_value
        ]
    )


def _collection_where_null(self, key=None):
    """
    Filter items where the given key is null.

    Args:
        key: The key to check for null values.

    Returns:
        A new Collection instance with the filtered items.
    """
    if key is None:
        return self.__class__([item for item in self._items if item is None])

    return self.__class__(
        [item for item in self._items if self._data_get(item, key) is None]
    )


def _collection_where_not_null(self, key=None):
    """
    Filter items where the given key is not null.

    Args:
        key: The key to check for not null values.

    Returns:
        A new Collection instance with the filtered items.
    """
    if key is None:
        return self.__class__([item for item in self._items if item is not None])

    return self.__class__(
        [item for item in self._items if self._data_get(item, key) is not None]
    )


def _collection_zip(self, items):
    """
    Merges the collection with the given items.

    Args:
        items: The items to merge with.

    Returns:
        A new Collection instance with the merged items.
    """
    items = self._get_items(items)
    if not isinstance(items, list):
        raise InvalidArgumentException(
            "The 'items' parameter must be a list or a Collection"
        )

    _items = []
    for x, y in builtins.zip(self, items, strict=False):
        _items.append([x, y])
    return self.__class__(_items)


def _collection_set_appends(self, appends):
    """
    Set the attributes that should be appended to the Collection.

    Args:
        appends: The attributes to append.

    Returns:
        The collection instance.
    """
    self.__appends__ += appends
    return self


def _get_value(self, key):
    """
    Gets the value for the given key from each item in the collection.

    Supports dot notation and wildcards like "product.*.name" or "*.price".

    Args:
        key: The key to get (supports dot notation and wildcards).

    Returns:
        A list of values.
    """
    if not key:
        return None

    items = []
    for item in self:
        if isinstance(key, str):
            if "*" in key:
                # Handle wildcards
                wildcard_values = self._data_get_with_wildcards(item, key)
                items.extend(wildcard_values)
            else:
                # Regular path with dot notation support
                value = self._data_get(item, key)
                if value is not None:
                    items.append(value)
        elif callable(key):
            result = key(item)
            if result:
                items.append(result)
    return items


def _data_get(self, item, key, default=None):
    """
    Gets an item from an array or object using "dot" notation.

    Args:
        item: The item to get from.
        key: The key to get.
        default: The default value to return if the key doesn't exist.

    Returns:
        The item at the key or the default value.
    """
    try:
        if isinstance(item, (list, tuple)):
            item = item[key]
        elif isinstance(item, (dict, Dotty)):
            item = data_get(item, key, default)
        elif isinstance(item, object):
            item = getattr(item, key)
    except (
        IndexError,
        AttributeError,
        KeyError,
        TypeError,
    ):
        return self._value(default)

    return item


def _data_get_with_wildcards(self, item, path, default=None):
    """
    Gets items from an array or object using "dot" notation with wildcard support.

    Supports wildcards (*) in paths like "products.*.name" or "*.price".

    Args:
        item: The item to get from.
        path: The path with potential wildcards.
        default: The default value to return if the path doesn't exist.

    Returns:
        List of values found at the path, or default if nothing found.
    """
    if "*" not in path:
        # No wildcards, use regular data_get
        result = self._data_get(item, path, default)
        return [result] if result is not None else []

    return self._extract_wildcard_path(item, path.split("."))


def _extract_wildcard_path(self, data, segments):
    """
    Recursively extract values from nested data structure using wildcard segments.

    Args:
        data: The data to extract from.
        segments: List of path segments (some may be '*').

    Returns:
        List of extracted values.
    """
    if not segments:
        return [data]

    segment = segments[0]
    remaining = segments[1:]

    if segment == "*":
        # Wildcard - iterate through all keys/indices
        results = []
        if isinstance(data, dict):
            for value in data.values():
                results.extend(self._extract_wildcard_path(value, remaining))
        elif isinstance(data, (list, tuple)):
            for value in data:
                results.extend(self._extract_wildcard_path(value, remaining))
        return results
    else:
        # Regular segment
        try:
            if isinstance(data, dict):
                next_data = data.get(segment)
            elif isinstance(data, (list, tuple)) and segment.isdigit():
                next_data = data[int(segment)]
            elif hasattr(data, segment):
                next_data = getattr(data, segment)
            else:
                return []

            if next_data is not None:
                return self._extract_wildcard_path(next_data, remaining)
        except KeyError, IndexError, AttributeError, TypeError, ValueError:
            pass

        return []


def _value(self, value):
    """
    Gets the value of a callable or returns the value.

    Args:
        value: The value to get.

    Returns:
        The value.
    """
    if callable(value):
        return value()
    return value


def _check_is_callable(self, callback, raise_exception=True):
    """
    Checks if the given callback is callable.

    Args:
        callback: The callback to check.
        raise_exception: Whether to raise an exception if the callback is not callable.

    Returns:
        True if the callback is callable, False otherwise.

    Raises:
        ValueError: If the callback is not callable and raise_exception is True.
    """
    if not callable(callback):
        if not raise_exception:
            return False
        raise InvalidArgumentException("The 'callback' should be a function")
    return True


def _make_comparison(self, a, b, op):
    """
    Makes a comparison between two values using the given operator.

    Args:
        a: The first value.
        b: The second value.
        op: The operator to use.

    Returns:
        The result of the comparison.
    """
    # Match Laravel's Collection::operatorForWhere vocabulary (and this
    # framework's own QueryBuilder, which accepts "=" / "<>"): a caller who
    # learned "=" works on the DB builder must not hit a bare KeyError when
    # using the same operator on an in-memory Collection.
    operators = {
        "=": operator.eq,
        "==": operator.eq,
        "===": operator.eq,
        "<": operator.lt,
        "<=": operator.le,
        "!=": operator.ne,
        "<>": operator.ne,
        "!==": operator.ne,
        ">": operator.gt,
        ">=": operator.ge,
    }
    try:
        return operators[op](a, b)
    except KeyError as exc:
        raise InvalidArgumentException(
            f"Unsupported Collection.where operator: {op!r}"
        ) from exc
