"""
Collection Module for Data Manipulation.

This module provides a powerful collection system for the Cara framework, implementing Laravel-style
collection methods with support for mapping, filtering, sorting, and aggregation operations.
"""

from __future__ import annotations

import json
import operator
import random
from collections import Counter
from functools import reduce
from itertools import groupby

from dotty_dict import Dotty

from cara.exceptions import InvalidArgumentException
from cara.support.Macroable import Macroable
from cara.support.Structures import data_get


class Collection(Macroable):
    """
    Collection class for fluent data manipulation.

    This class provides a fluent interface for working with arrays and data sets, implementing
    Laravel-style collection methods for transforming and manipulating data with method chaining.
    """

    def __init__(self, items=None):
        """
        Initialize a new Collection instance.

        Args:
            items: The items to be collected. Defaults to an empty list if None.
        """
        # ``items if items is not None else []`` — NOT ``items or []``. An empty
        # DICT (or empty list) is falsy, so ``or []`` silently coerced a
        # dict-backed Collection with no entries — exactly what ``group_by`` /
        # ``key_by`` / ``map_with_keys`` return for an EMPTY source — into a
        # ``[]``. ``.all()`` then handed back a list and ``.get(key)`` blew up
        # with "'list' object has no attribute 'get'". Only ``None`` should
        # default to ``[]``; an empty dict must stay a dict.
        self._items = items if items is not None else []
        self.__appends__ = []

    def take(self, number: int):
        """
        Takes a specific number of results from the items.

        Takes the specified number of items from the collection. If a negative number
        is passed, it will take that many items from the end of the collection.

        Args:
            number: The number of results to take.

        Returns:
            A new Collection instance with the taken items.
        """
        if number < 0:
            return self[number:]

        return self[:number]

    def skip(self, number: int):
        """
        Skips the specified number of items from the beginning of the collection.

        Args:
            number: The number of items to skip.

        Returns:
            A new Collection instance with the remaining items.
        """
        if number < 0:
            return self.__class__(self._items)

        return self.__class__(self._items[number:])

    def first(self, callback=None, default=None):
        """
        Takes the first result in the items.

        If a callback is given then the first result will be the result after the filter.
        If the collection is empty or no items match the callback, the default value is returned.

        Args:
            callback: Used to filter the results before returning the first item.
            default: Default value to return if no items match or collection is empty.

        Returns:
            The first item in the collection or default if the collection is empty.
        """
        filtered = self
        if callback:
            filtered = self.filter(callback)

        if not filtered:
            return default if default is not None else None

        return filtered[0]

    def last(self, callback=None, default=None):
        """
        Takes the last result in the items.

        If a callback is given then the last result will be the result after the filter.
        If the collection is empty or no items match the callback, the default value is returned.

        Args:
            callback: Used to filter the results before returning the last item.
            default: Default value to return if no items match or collection is empty.

        Returns:
            The last item in the collection or default if the collection is empty.
        """
        filtered = self
        if callback:
            filtered = self.filter(callback)

        if not filtered:
            return default if default is not None else None

        return filtered[-1]

    def sole(self, callback=None):
        """
        Returns the sole item in the collection that matches the given callback.

        Raises an exception if the collection is empty or has more than one item.

        Args:
            callback: Used to filter the results.

        Returns:
            The sole item in the collection.

        Raises:
            ValueError: If the collection doesn't contain exactly one item.
        """
        filtered = self
        if callback:
            filtered = self.filter(callback)

        count = len(filtered)

        if count == 0:
            raise InvalidArgumentException("Collection is empty")
        elif count > 1:
            raise InvalidArgumentException("Collection contains more than one item")

        return filtered[0]

    def first_where(self, key, operator="==", value=None):
        """
        Returns the first item where the given key's value matches the criteria.

        Supports dot notation and wildcards like "product.*.name" or "*.price".

        Args:
            key: The key to check (supports dot notation and wildcards).
            operator: The comparison operator.
            value: The value to compare against.

        Returns:
            The first matching item or None.
        """
        if value is None and operator != "==":
            value = operator
            operator = "=="

        filtered = self.where(key, operator, value)
        return filtered.first() if filtered else None

    def all(self):
        """
        Returns all the items in the collection.

        Returns:
            All items in the collection.
        """
        return self._items

    def avg(self, key=None):
        """
        Returns the average of the items.

        If a key is given it will return the average of all the values of the key.

        Args:
            key: The key to use to find the average of all the values of that key.

        Returns:
            The average value.
        """
        result = 0
        items = self._get_value(key) or self._items
        try:
            result = sum(items) / len(items)
        except (TypeError, ZeroDivisionError):
            pass
        return result

    def median(self, key=None):
        """
        Returns the median value of the items.

        If a key is given it will return the median of all the values of the key.

        Args:
            key: The key to use to find the median of all the values of that key.

        Returns:
            The median value.
        """
        items = self._get_value(key) or self._items

        try:
            # Sort the items
            sorted_items = sorted(items)
            count = len(sorted_items)

            if count == 0:
                return 0

            # Get the middle index
            middle = count // 2

            if count % 2 == 0:
                # If even number of items, average the two middle values
                return (sorted_items[middle - 1] + sorted_items[middle]) / 2
            else:
                # If odd number of items, return the middle value
                return sorted_items[middle]
        except (TypeError, ValueError):
            return 0

    def mode(self, key=None):
        """
        Returns the most frequent value in the collection.

        If a key is given it will return the mode of all the values of the key.

        Args:
            key: The key to use to find the mode of all the values of that key.

        Returns:
            The mode value or None if the collection is empty.
        """
        items = self._get_value(key) or self._items

        if not items:
            return None

        try:
            # Count occurrences of each value
            counts = {}
            for item in items:
                if item in counts:
                    counts[item] += 1
                else:
                    counts[item] = 1

            # Find the value with the highest count
            max_count = 0
            mode_value = None

            for value, count in counts.items():
                if count > max_count:
                    max_count = count
                    mode_value = value

            return mode_value
        except (TypeError, ValueError):
            return None

    def max(self, key=None):
        """
        Returns the maximum value of the items.

        If a key is given it will return the maximum of all the values of the key.

        Args:
            key: The key to use to find the maximum of all the values of that key.

        Returns:
            The maximum value.
        """
        result = 0
        items = self._get_value(key) or self._items

        try:
            if not items:
                return 0
            return max(items)
        except (TypeError, ValueError):
            pass
        return result

    def min(self, key=None):
        """
        Returns the minimum value of the items.

        If a key is given it will return the minimum of all the values of the key.

        Args:
            key: The key to use to find the minimum of all the values of that key.

        Returns:
            The minimum value.
        """
        items = self._get_value(key) or self._items

        try:
            if not items:
                return 0
            return min(items)
        except (TypeError, ValueError):
            return 0

    def chunk(self, size: int):
        """
        Chunks the items into smaller collections of a given size.

        Args:
            size: The number of values in each chunk.

        Returns:
            A new Collection instance containing the chunked items.
        """
        items = []
        for i in range(0, self.count(), size):
            items.append(self[i : i + size])
        return self.__class__(items)

    def split_in(self, groups: int):
        """
        Splits the collection into the given number of groups.

        Args:
            groups: The number of groups to split into.

        Returns:
            A new Collection instance containing the grouped items.
        """
        if groups <= 0:
            return self.__class__([])

        size = len(self._items)
        base_size = size // groups
        extra = size % groups

        result = []
        start = 0

        for i in range(groups):
            group_size = base_size + (1 if i < extra else 0)
            if group_size == 0:
                result.append(self.__class__([]))
            else:
                result.append(self.__class__(self._items[start : start + group_size]))
                start += group_size

        return self.__class__(result)

    def collapse(self):
        """
        Collapses a collection of arrays into a single, flat collection.

        Returns:
            A new Collection instance with the collapsed items.
        """
        items = []
        for item in self:
            items += self.__get_items(item)
        return self.__class__(items)

    def contains(self, key, value=None):
        """
        Determines if the collection contains a given item or key-value pair.

        Args:
            key: The key or callback to check.
            value: The value to check if key is a property name.

        Returns:
            True if the collection contains the item, False otherwise.
        """
        if value is not None:
            return self.contains(lambda x: self._data_get(x, key) == value)

        if self._check_is_callable(key, raise_exception=False):
            return self.first(key) is not None

        return key in self

    def doesnt_contain(self, key, value=None):
        """
        Determines if the collection does not contain a given item or key-value pair.

        Args:
            key: The key or callback to check.
            value: The value to check if key is a property name.

        Returns:
            True if the collection does not contain the item, False otherwise.
        """
        return not self.contains(key, value)

    def count(self):
        """
        Returns the total number of items in the collection.

        Returns:
            The count of items in the collection.
        """
        return len(self._items)

    def count_by(self, callback=None):
        """
        Counts the occurrences of values in the collection.

        Args:
            callback: The callback to determine the counting value.

        Returns:
            A new Collection instance with the counts.
        """
        if callback is None:
            return self.__class__(dict(Counter(self._items)))

        counts = {}
        for item in self._items:
            key = callback(item) if callable(callback) else self._data_get(item, callback)
            if key in counts:
                counts[key] += 1
            else:
                counts[key] = 1

        return self.__class__(counts)

    def diff(self, items):
        """
        Returns the items in the collection that are not present in the given items.

        Args:
            items: The items to compare against.

        Returns:
            A new Collection instance with the differing items.
        """
        items = self.__get_items(items)
        return self.__class__([x for x in self if x not in items])

    def diff_assoc(self, items):
        """
        Returns the items in the collection whose keys and values are not present in the given
        items.

        Args:
            items: The items to compare against.

        Returns:
            A new Collection instance with the differing items.
        """
        items = self.__get_items(items)

        if not isinstance(self._items, dict) or not isinstance(items, dict):
            return self.diff(items)

        return self.__class__(
            {k: v for k, v in self._items.items() if k not in items or items[k] != v}
        )

    def diff_keys(self, items):
        """
        Returns the items in the collection whose keys are not present in the given items.

        Args:
            items: The items to compare against.

        Returns:
            A new Collection instance with the differing items.
        """
        items = self.__get_items(items)

        if not isinstance(self._items, dict) or not isinstance(items, dict):
            return self.diff(items)

        return self.__class__({k: v for k, v in self._items.items() if k not in items})

    def each(self, callback):
        """
        Iterates over the items in the collection and applies the callback to each item.

        Args:
            callback: The callback to apply to each item.

        Returns:
            The collection instance.
        """
        self._check_is_callable(callback)

        for k, v in enumerate(self):
            result = callback(v, k)
            if result is False:
                break
            elif result is not None:
                self[k] = result

        return self

    def every(self, callback):
        """
        Determines if all items in the collection pass the given truth test.

        Args:
            callback: The truth test callback.

        Returns:
            True if all items pass the test, False otherwise.
        """
        self._check_is_callable(callback)
        return all([callback(x) for x in self])

    def filter(self, callback=None):
        """
        Filters the collection using the given callback.

        Args:
            callback: The truth test callback.

        Returns:
            A new Collection instance with the filtered items.
        """
        if callback is None:
            return self.__class__([x for x in self if x])

        self._check_is_callable(callback)
        return self.__class__(list(filter(callback, self)))

    def when(self, value, callback, default=None):
        """
        Apply the callback if the given value is truthy.

        Args:
            value: The value to check.
            callback: The callback to apply if the value is truthy.
            default: The callback to apply if the value is falsy.

        Returns:
            The collection instance.
        """
        if value:
            return callback(self, value)
        elif default:
            return default(self, value)

        return self

    def unless(self, value, callback, default=None):
        """
        Apply the callback if the given value is falsy.

        Args:
            value: The value to check.
            callback: The callback to apply if the value is falsy.
            default: The callback to apply if the value is truthy.

        Returns:
            The collection instance.
        """
        return self.when(not value, callback, default)

    def flatten(self, depth=float("inf")):
        """
        Flattens a multi-dimensional collection into a single dimension.

        Args:
            depth: The maximum depth to flatten.

        Returns:
            A new Collection instance with the flattened items.
        """

        def _flatten(items, current_depth=0):
            if current_depth >= depth:
                yield items
                return

            if isinstance(items, dict):
                for v in items.values():
                    for x in _flatten(v, current_depth + 1):
                        yield x
            elif isinstance(items, (list, tuple)):
                for i in items:
                    for j in _flatten(i, current_depth + 1):
                        yield j
            else:
                yield items

        return self.__class__(list(_flatten(self._items)))

    def forget(self, *keys):
        """
        Removes the specified items from the collection by key.

        Args:
            *keys: The keys to remove.

        Returns:
            The collection instance.
        """
        keys = reversed(sorted(keys))

        for key in keys:
            del self[key]

        return self

    def only(self, *keys):
        """
        Get the items with the specified keys only.

        Args:
            *keys: The keys to include.

        Returns:
            A new Collection instance with only the specified keys.
        """
        if not isinstance(self._items, dict):
            return self.__class__([])

        result = {}
        for key in keys:
            if key in self._items:
                result[key] = self._items[key]

        return self.__class__(result)

    def except_keys(self, *keys):
        """
        Get all items except those with the specified keys.

        Args:
            *keys: The keys to exclude.

        Returns:
            A new Collection instance without the specified keys.
        """
        if not isinstance(self._items, dict):
            return self.__class__(self._items)

        result = {k: v for k, v in self._items.items() if k not in keys}
        return self.__class__(result)

    def for_page(self, page, per_page):
        """
        Returns a slice of items for a given page and number of items per page.

        Args:
            page: The page number (1-based).
            per_page: The number of items per page.

        Returns:
            A new Collection instance with the paginated items.
        """
        if page < 1 or per_page < 1:
            return self.__class__([])

        offset = (page - 1) * per_page

        return self.slice(offset, per_page)

    def slice(self, offset, length=None):
        """
        Returns a slice of items starting at the specified index.

        Args:
            offset: The starting index.
            length: The length of the slice.

        Returns:
            A new Collection instance with the sliced items.
        """
        if offset < 0:
            offset = len(self._items) + offset

        if length is None:
            return self.__class__(self._items[offset:])

        return self.__class__(self._items[offset : offset + length])

    def keys(self):
        """
        Get all the keys of the collection items.

        Returns:
            A new Collection instance with the keys.
        """
        if isinstance(self._items, dict):
            return self.__class__(list(self._items.keys()))

        return self.__class__(list(range(len(self._items))))

    def values(self):
        """
        Get all the values of the collection items.

        Returns:
            A new Collection instance with the values.
        """
        if isinstance(self._items, dict):
            return self.__class__(list(self._items.values()))

        return self.__class__(self._items)

    def get(self, key, default=None):
        """
        Returns the item at the specified key or the default value.

        Args:
            key: The key to get.
            default: The default value to return if the key doesn't exist.

        Returns:
            The item at the key or the default value.
        """
        try:
            return self[key]
        except (IndexError, KeyError):
            pass

        return self._value(default)

    def implode(self, glue=",", key=None):
        """
        Joins the items in the collection with a string.

        Args:
            glue: The string to join the items with.
            key: The key to pluck from the items before joining.

        Returns:
            The joined string.
        """
        first = self.first()
        if not isinstance(first, str) and key:
            return glue.join(self.pluck(key))
        return glue.join([str(x) for x in self])

    def is_empty(self):
        """
        Determines if the collection is empty.

        Returns:
            True if the collection is empty, False otherwise.
        """
        return not self

    def is_not_empty(self):
        """
        Determines if the collection is not empty.

        Returns:
            True if the collection is not empty, False otherwise.
        """
        return not self.is_empty()

    def map(self, callback):
        """
        Maps each item in the collection to a new value using the callback.

        Args:
            callback: The mapping callback.

        Returns:
            A new Collection instance with the mapped items.
        """
        self._check_is_callable(callback)
        items = [callback(x) for x in self]
        return self.__class__(items)

    def map_with_keys(self, callback):
        """
        Maps each item in the collection to a key-value pair using the callback.

        Args:
            callback: The mapping callback.

        Returns:
            A new Collection instance with the mapped items.
        """
        self._check_is_callable(callback)

        result = {}
        for item in self:
            key_value = callback(item)
            if isinstance(key_value, tuple) and len(key_value) == 2:
                result[key_value[0]] = key_value[1]

        return self.__class__(result)

    def map_into(self, cls, method=None, **kwargs):
        """
        Maps each item in the collection into a new class instance.

        Args:
            cls: The class to map into.
            method: The method to call on the class.
            **kwargs: Additional arguments to pass to the class or method.

        Returns:
            A new Collection instance with the mapped items.
        """
        results = []
        for item in self:
            if method:
                results.append(getattr(cls, method)(item, **kwargs))
            else:
                results.append(cls(item))

        return self.__class__(results)

    def merge(self, items):
        """
        Merges the given items into the collection.

        Args:
            items: The items to merge.

        Returns:
            The collection instance.
        """
        items = self.__get_items(items)

        if isinstance(self._items, dict) and isinstance(items, dict):
            self._items.update(items)
            return self

        if not isinstance(items, list):
            raise InvalidArgumentException("Unable to merge incompatible types")

        self._items += items
        return self

    def combine(self, values):
        """
        Combines the keys of the collection with the values of another collection.

        Args:
            values: The values to combine with the keys.

        Returns:
            A new Collection instance with the combined items.
        """
        values = self.__get_items(values)

        if len(self._items) != len(values):
            raise InvalidArgumentException("The number of keys must match the number of values")

        return self.__class__(dict(zip(self._items, values, strict=False)))

    def pluck(self, value, key=None, keep_nulls=True):
        """
        Retrieves all of the values for a given key.

        Supports dot notation and wildcards like "product.*.name" or "*.price".

        Args:
            value: The key to pluck (supports dot notation and wildcards).
            key: The key to use as the collection key (also supports dot notation).
            keep_nulls: When ``False`` (and no ``key`` is given), ``None`` values
                are dropped from the resulting list. The polymorphic relations
                (Morph* ``get_related``) rely on this to avoid feeding ``None``
                ids into a ``where_in`` — without it those eager-loads raised
                ``TypeError: pluck() got an unexpected keyword argument``.
                Defaults to ``True`` so every existing caller is unaffected.

        Returns:
            A new Collection instance with the plucked values.
        """
        if key:
            attributes = {}
        else:
            attributes = []

        for item in self:
            # Handle wildcards in value path
            if "*" in str(value):
                item_values = self._data_get_with_wildcards(item, value)
                if key:
                    # If key is specified, we need to extract it too
                    if "*" in str(key):
                        item_keys = self._data_get_with_wildcards(item, key)
                        # Match keys with values
                        for i, val in enumerate(item_values):
                            if i < len(item_keys):
                                attributes[item_keys[i]] = val
                    else:
                        item_key = self._data_get(item, key)
                        for val in item_values:
                            if item_key is not None:
                                if item_key not in attributes:
                                    attributes[item_key] = []
                                if isinstance(attributes[item_key], list):
                                    attributes[item_key].append(val)
                                else:
                                    attributes[item_key] = [attributes[item_key], val]
                else:
                    # No key specified, just extend the list
                    attributes.extend(item_values)
            else:
                # Regular path without wildcards
                item_value = self._data_get(item, value)

                if key:
                    if "*" in str(key):
                        item_keys = self._data_get_with_wildcards(item, key)
                        for item_key in item_keys:
                            attributes[item_key] = item_value
                    else:
                        item_key = self._data_get(item, key)
                        if item_key is not None:
                            attributes[item_key] = item_value
                else:
                    attributes.append(item_value)

        if not key and not keep_nulls:
            attributes = [v for v in attributes if v is not None]

        return Collection(attributes)

    def pop(self):
        """
        Removes and returns the last item from the collection.

        Returns:
            The last item in the collection.
        """
        last = self._items.pop()
        return last

    def prepend(self, value, key=None):
        """
        Adds an item to the beginning of the collection.

        Args:
            value: The item to add.
            key: The key to use for the item.

        Returns:
            The collection instance.
        """
        if key is not None and isinstance(self._items, dict):
            self._items = {key: value, **self._items}
            return self

        self._items.insert(0, value)
        return self

    def pull(self, key):
        """
        Removes and returns an item from the collection by key.

        Args:
            key: The key to remove.

        Returns:
            The removed item.
        """
        value = self.get(key)
        self.forget(key)
        return value

    def push(self, value):
        """
        Adds an item to the end of the collection.

        Args:
            value: The item to add.

        Returns:
            The collection instance.
        """
        self._items.append(value)
        return self

    def put(self, key, value):
        """
        Sets the given key and value in the collection.

        Args:
            key: The key to set.
            value: The value to set.

        Returns:
            The collection instance.
        """
        self[key] = value
        return self

    def random(self, count=None):
        """
        Returns a random item or items from the collection.

        Args:
            count: The number of random items to return.

        Returns:
            A random item or a new Collection instance with random items.
        """
        collection_count = self.count()
        if collection_count == 0:
            return None
        elif count and count > collection_count:
            raise InvalidArgumentException("count argument must be inferior to collection length.")
        elif count:
            items = random.sample(self._items, k=count)
            return self.__class__(items)
        else:
            return random.choice(self._items)

    def reduce(self, callback, initial=0):
        """
        Reduces the collection to a single value using the callback.

        Args:
            callback: The reduction callback.
            initial: The initial value.

        Returns:
            The reduced value.
        """
        return reduce(callback, self, initial)

    def reject(self, callback):
        """
        Filters the collection using the given callback, removing items that pass.

        Args:
            callback: The truth test callback.

        Returns:
            A new Collection instance with the filtered items.
        """
        self._check_is_callable(callback)
        return self.__class__([x for x in self if not callback(x)])

    def reverse(self):
        """
        Reverses the order of the collection's items.

        Returns:
            The collection instance.
        """
        self._items = self._items[::-1]
        return self

    def search(self, value, strict=False):
        """
        Searches the collection for a given value and returns the key of the first match.

        Args:
            value: The value to search for.
            strict: Whether to use strict comparison.

        Returns:
            The key of the first matching item or False if no match is found.
        """
        if callable(value):
            for key, item in enumerate(self):
                if value(item):
                    return key
        else:
            for key, item in enumerate(self):
                if (strict and item == value) or (not strict and item == value):
                    return key

        return False

    def serialize(self):
        """
        Converts the collection into a serialized array of items.

        Returns:
            The serialized array.
        """

        def _serialize(item):
            if self.__appends__ and hasattr(item, "set_appends"):
                item.set_appends(self.__appends__)

            if hasattr(item, "serialize"):
                return item.serialize()
            elif hasattr(item, "to_dict"):
                return item.to_dict()
            return item

        return list(map(_serialize, self))

    def add_relation(self, result=None):
        """
        Adds a relationship to each item in the collection.

        Args:
            result: The relationship to add.

        Returns:
            The collection instance.
        """
        for model in self._items:
            if hasattr(model, "add_relations"):
                model.add_relations(result or {})
            elif hasattr(model, "add_relation"):
                model.add_relation(result or {})

        return self

    def shift(self):
        """
        Removes and returns the first item from the collection.

        Returns:
            The first item in the collection.
        """
        return self.pull(0)

    def sort(self, key=None):
        """
        Sorts the collection by the given key.

        Args:
            key: The key to sort by.

        Returns:
            The collection instance.
        """
        if key:
            self._items.sort(
                key=lambda x: x[key] if isinstance(x, dict) else getattr(x, key, x),
                reverse=False,
            )
            return self

        self._items = sorted(self)
        return self

    def sort_by(self, callback=None):
        """
        Sorts the collection by the given callback or key.

        Supports dot notation and wildcards like "product.*.name" or "*.price".

        Args:
            callback: The callback or key to sort by (supports dot notation and wildcards).

        Returns:
            A new Collection instance with the sorted items.
        """
        if callback is None:
            return self.__class__(sorted(self._items))

        if callable(callback):
            return self.__class__(sorted(self._items, key=callback))

        if "*" in str(callback):
            # Handle wildcards - use the first wildcard match for sorting
            return self.__class__(
                sorted(
                    self._items,
                    key=lambda x: (self._data_get_with_wildcards(x, callback) or [None])[
                        0
                    ],
                )
            )
        else:
            # Regular path with dot notation support
            return self.__class__(
                sorted(
                    self._items,
                    key=lambda x: self._data_get(x, callback),
                )
            )

    def sort_by_desc(self, callback=None):
        """
        Sorts the collection in descending order by the given callback or key.

        Supports dot notation and wildcards like "product.*.name" or "*.price".

        Args:
            callback: The callback or key to sort by (supports dot notation and wildcards).

        Returns:
            A new Collection instance with the sorted items.
        """
        if callback is None:
            return self.__class__(sorted(self._items, reverse=True))

        if callable(callback):
            return self.__class__(sorted(self._items, key=callback, reverse=True))

        if "*" in str(callback):
            # Handle wildcards - use the first wildcard match for sorting
            return self.__class__(
                sorted(
                    self._items,
                    key=lambda x: (self._data_get_with_wildcards(x, callback) or [None])[
                        0
                    ],
                    reverse=True,
                )
            )
        else:
            # Regular path with dot notation support
            return self.__class__(
                sorted(
                    self._items,
                    key=lambda x: self._data_get(x, callback),
                    reverse=True,
                )
            )

    def sum(self, key=None):
        """
        Returns the sum of all items in the collection.

        Args:
            key: The key to sum by.

        Returns:
            The sum of the items.
        """
        result = 0
        items = self._get_value(key) or self._items
        try:
            result = sum(items)
        except TypeError:
            pass
        return result

    def to_json(self, **kwargs):
        """
        Converts the collection to JSON.

        Args:
            **kwargs: Additional arguments to pass to json.dumps.

        Returns:
            The JSON string.
        """
        return json.dumps(self.serialize(), **kwargs)

    def to_array(self):
        """
        Converts the collection to a plain array.

        Returns:
            A plain array of the collection's items.
        """
        return self.serialize()

    def group_by(self, key):
        """
        Groups the collection's items by the given key.

        Args:
            key: The key to group by.

        Returns:
            A new Collection instance with the grouped items.
        """
        if callable(key):
            grouper = key
        else:

            def grouper(x):
                """Resolve the group key from ``x`` via dotted-path lookup."""
                return self._data_get(x, key)

        results = {}

        for k, group in groupby(sorted(self._items, key=grouper), key=grouper):
            results[k] = list(group)

        return Collection(results)

    def transform(self, callback):
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

    def unique(self, key=None):
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

    def duplicates(self, key=None):
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

    def where(self, key, *args):
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

    def where_in(self, key, values):
        """
        Filters the collection by the given key-value pairs.

        Supports dot notation and wildcards like "product.*.name" or "*.price".

        Args:
            key: The key to filter by (supports dot notation and wildcards).
            values: The values to filter by.

        Returns:
            A new Collection instance with the filtered items.
        """
        values = self.__get_items(values)

        if "*" in str(key):
            # Handle wildcards
            return self.__class__(
                [
                    item
                    for item in self._items
                    if any(
                        val in values for val in self._data_get_with_wildcards(item, key)
                    )
                ]
            )
        else:
            # Regular path
            return self.__class__(
                [item for item in self._items if self._data_get(item, key) in values]
            )

    def where_not_in(self, key, values):
        """
        Filters the collection by the given key-value pairs, removing matching items.

        Supports dot notation and wildcards like "product.*.name" or "*.price".

        Args:
            key: The key to filter by (supports dot notation and wildcards).
            values: The values to filter by.

        Returns:
            A new Collection instance with the filtered items.
        """
        values = self.__get_items(values)

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

    def where_between(self, key, values):
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

    def where_not_between(self, key, values):
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

    def where_null(self, key=None):
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

    def where_not_null(self, key=None):
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

    def zip(self, items):
        """
        Merges the collection with the given items.

        Args:
            items: The items to merge with.

        Returns:
            A new Collection instance with the merged items.
        """
        items = self.__get_items(items)
        if not isinstance(items, list):
            raise InvalidArgumentException("The 'items' parameter must be a list or a Collection")

        _items = []
        for x, y in zip(self, items, strict=False):
            _items.append([x, y])
        return self.__class__(_items)

    def set_appends(self, appends):
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
            except (KeyError, IndexError, AttributeError, TypeError, ValueError):
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
        operators = {
            "<": operator.lt,
            "<=": operator.le,
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            ">=": operator.ge,
        }
        return operators[op](a, b)

    def __iter__(self):
        """
        Allows the collection to be iterated over.

        Yields:
            Each item in the collection.
        """
        for item in self._items:
            yield item

    def __eq__(self, other):
        """
        Determines if the collection is equal to the given value.

        Args:
            other: The value to compare with.

        Returns:
            True if the collection is equal to the value, False otherwise.
        """
        if isinstance(other, Collection):
            return self._items == other.all()
        return other == self._items

    def __getitem__(self, item):
        """
        Gets an item from the collection by key.

        Args:
            item: The key to get.

        Returns:
            The item at the key.
        """
        if isinstance(item, slice):
            return self.__class__(self._items[item])

        return self._items[item]

    def __setitem__(self, key, value):
        """
        Sets an item in the collection by key.

        Args:
            key: The key to set.
            value: The value to set.
        """
        self._items[key] = value

    def __delitem__(self, key):
        """
        Removes an item from the collection by key.

        Args:
            key: The key to remove.
        """
        del self._items[key]

    def __ne__(self, other):
        """
        Determines if the collection is not equal to the given value.

        Args:
            other: The value to compare with.

        Returns:
            True if the collection is not equal to the value, False otherwise.
        """
        other = self.__get_items(other)
        return other != self._items

    def __len__(self):
        """
        Gets the number of items in the collection.

        Returns:
            The number of items in the collection.
        """
        return len(self._items)

    def __le__(self, other):
        """
        Determines if the collection is less than or equal to the given value.

        Args:
            other: The value to compare with.

        Returns:
            True if the collection is less than or equal to the value, False otherwise.
        """
        other = self.__get_items(other)
        return self._items <= other

    def __lt__(self, other):
        """
        Determines if the collection is less than the given value.

        Args:
            other: The value to compare with.

        Returns:
            True if the collection is less than the value, False otherwise.
        """
        other = self.__get_items(other)
        return self._items < other

    def __ge__(self, other):
        """
        Determines if the collection is greater than or equal to the given value.

        Args:
            other: The value to compare with.

        Returns:
            True if the collection is greater than or equal to the value, False otherwise.
        """
        other = self.__get_items(other)
        return self._items >= other

    def __gt__(self, other):
        """
        Determines if the collection is greater than the given value.

        Args:
            other: The value to compare with.

        Returns:
            True if the collection is greater than the value, False otherwise.
        """
        other = self.__get_items(other)
        return self._items > other

    @classmethod
    def __get_items(cls, items):
        """
        Gets the items from a collection or returns the items.

        Args:
            items: The items to get.

        Returns:
            The items.
        """
        if isinstance(items, Collection):
            items = items.all()

        return items

    def partition(self, callback):
        """
        Split the collection into two collections based on the given callback.

        Args:
            callback: Function that returns True/False to determine the partition

        Returns:
            List containing two collections - items that passed and failed the test
        """
        passed = []
        failed = []

        for item in self:
            if callback(item):
                passed.append(item)
            else:
                failed.append(item)

        return [
            self.__class__(passed),
            self.__class__(failed),
        ]

    def pipe(self, callback):
        """
        Pass the collection through the given callback and return the result.

        Args:
            callback: The callback to process the collection

        Returns:
            The result of the callback
        """
        return callback(self)

    def tap(self, callback):
        """
        Pass the collection to the callback and return the collection.

        Args:
            callback: The callback to receive the collection

        Returns:
            The collection instance
        """
        callback(self)
        return self

    def nth(self, step, offset=0):
        """
        Create a new collection consisting of every n-th element.

        Args:
            step: The step value
            offset: The starting offset

        Returns:
            New collection with every n-th element
        """
        items = []
        for i in range(offset, len(self._items), step):
            items.append(self._items[i])
        return self.__class__(items)

    def sliding(self, size=2, step=1):
        """
        Create a sliding window of the given size.

        Args:
            size: The size of each window
            step: The step between windows

        Returns:
            New collection with sliding windows
        """
        if size <= 0:
            return self.__class__([])

        windows = []
        for i in range(0, len(self._items) - size + 1, step):
            windows.append(self.__class__(self._items[i : i + size]))

        return self.__class__(windows)

    def pad(self, target_size, value):
        """
        Pad the collection to a specified length with a value.

        Args:
            target_size: The desired size
            value: The value to pad with

        Returns:
            New collection padded to the target size
        """
        current_size = len(self._items)
        if target_size <= current_size:
            return self.__class__(self._items[:])

        padding_size = target_size - current_size
        if padding_size > 0:
            padding = [value] * padding_size
            if target_size > 0:
                return self.__class__(self._items + padding)
            else:
                return self.__class__(padding + self._items)

        return self.__class__(self._items)

    def items(self):
        """
        Get the underlying items as dictionary items.

        Returns:
            Dictionary items if collection is a dict, otherwise the items themselves.
        """
        if isinstance(self._items, dict):
            return self._items.items()
        return self._items

    def ensure(self, *types):
        """
        Ensures all items in the collection are of the specified types.

        Args:
            *types: The types to check against.

        Returns:
            The collection instance.

        Raises:
            ValueError: If any item is not of the specified types.
        """
        for item in self._items:
            if not isinstance(item, types):
                raise InvalidArgumentException(f"Item {item} is not of type {types}")

        return self

    def key_by(self, key):
        """
        Re-keys the collection's items by the given key.

        The key may be a dotted-path string or a callable receiving each item. When
        two items resolve to the same key, the later item wins (overwrites the earlier).

        Args:
            key: The key to re-key by (callable or dotted-path string).

        Returns:
            A new dict-backed Collection instance keyed by the resolved value.
        """
        if callable(key):
            resolver = key
        else:

            def resolver(item):
                """Resolve the new key from ``item`` via dotted-path lookup."""
                return self._data_get(item, key)

        results = {}
        for item in self:
            results[resolver(item)] = item

        return self.__class__(results)

    def flat_map(self, callback):
        """
        Maps each item using the callback then collapses the result one level.

        Args:
            callback: The mapping callback applied to each item.

        Returns:
            A new Collection instance with the mapped-and-collapsed items.
        """
        self._check_is_callable(callback)
        return self.map(callback).collapse()

    def concat(self, items):
        """
        Appends the given items onto the end of the collection.

        Unlike ``merge``, this never overwrites by key — values are always appended,
        and a new Collection is returned (the original is left untouched).

        Args:
            items: The items to append (an iterable or another Collection).

        Returns:
            A new Collection instance with the concatenated items.
        """
        items = self.__get_items(items)

        if isinstance(items, dict):
            appended = list(items.values())
        else:
            appended = list(items)

        return self.__class__(list(self._items) + appended)

    def when_empty(self, callback, default=None):
        """
        Applies the callback when the collection is empty.

        Args:
            callback: The callback to apply if the collection is empty.
            default: The callback to apply if the collection is not empty.

        Returns:
            The result of the chosen callback, or the collection instance.
        """
        return self.when(self.is_empty(), callback, default)

    def when_not_empty(self, callback, default=None):
        """
        Applies the callback when the collection is not empty.

        Args:
            callback: The callback to apply if the collection is not empty.
            default: The callback to apply if the collection is empty.

        Returns:
            The result of the chosen callback, or the collection instance.
        """
        return self.when(self.is_not_empty(), callback, default)

    def has(self, *keys):
        """
        Determines whether the collection contains every given key or index.

        Args:
            *keys: The keys (dict-backed) or indices (list-backed) to check.

        Returns:
            True if every key/index exists, False otherwise.
        """
        for key in keys:
            if isinstance(self._items, dict):
                if key not in self._items:
                    return False
            else:
                try:
                    self._items[key]
                except (IndexError, KeyError, TypeError):
                    return False

        return True

    def value(self, key, default=None):
        """
        Retrieves the value at the given key from the first item.

        Args:
            key: The key to resolve (callable or dotted-path string).
            default: The value to return if the collection is empty.

        Returns:
            The resolved value from the first item, or the default.
        """
        first = self.first()
        if first is None:
            return self._value(default)

        return self._data_get(first, key, default)

    @classmethod
    def make(cls, items=None):
        """
        Creates a new Collection instance from the given items.

        Args:
            items: The items to wrap. Defaults to an empty collection if None.

        Returns:
            A new Collection instance.
        """
        return cls(items)

    @classmethod
    def wrap(cls, value):
        """
        Wraps the given value in a Collection if it is not already one.

        A Collection is returned as-is, a list/tuple/dict is wrapped directly, and any
        other scalar value is wrapped as a single-item collection.

        Args:
            value: The value to wrap.

        Returns:
            A Collection instance.
        """
        if isinstance(value, Collection):
            return value

        if isinstance(value, (list, tuple, dict)):
            return cls(list(value) if isinstance(value, tuple) else value)

        return cls([value])

    @classmethod
    def unwrap(cls, value):
        """
        Returns the underlying items of a Collection, or the value unchanged.

        Args:
            value: The value to unwrap.

        Returns:
            The underlying items if ``value`` is a Collection, otherwise ``value``.
        """
        if isinstance(value, Collection):
            return value.all()

        return value

    def skip_while(self, callback):
        """
        Skips items while the callback returns truthy, keeping the rest.

        Args:
            callback: A callable predicate, or a value to compare each item against.

        Returns:
            A new Collection instance with the remaining items.
        """
        predicate = callback if callable(callback) else lambda item: item == callback

        items = list(self._items)
        index = 0
        while index < len(items) and predicate(items[index]):
            index += 1

        return self.__class__(items[index:])

    def skip_until(self, callback):
        """
        Skips items until the callback returns truthy, keeping the rest.

        Args:
            callback: A callable predicate, or a value to compare each item against.

        Returns:
            A new Collection instance with the remaining items.
        """
        predicate = callback if callable(callback) else lambda item: item == callback

        items = list(self._items)
        index = 0
        while index < len(items) and not predicate(items[index]):
            index += 1

        return self.__class__(items[index:])

    def take_while(self, callback):
        """
        Takes items while the callback returns truthy, stopping at the first failure.

        Args:
            callback: A callable predicate, or a value to compare each item against.

        Returns:
            A new Collection instance with the leading items that matched.
        """
        predicate = callback if callable(callback) else lambda item: item == callback

        taken = []
        for item in self._items:
            if not predicate(item):
                break
            taken.append(item)

        return self.__class__(taken)

    def take_until(self, callback):
        """
        Takes items until the callback returns truthy, stopping at the first match.

        Args:
            callback: A callable predicate, or a value to compare each item against.

        Returns:
            A new Collection instance with the leading items before the match.
        """
        predicate = callback if callable(callback) else lambda item: item == callback

        taken = []
        for item in self._items:
            if predicate(item):
                break
            taken.append(item)

        return self.__class__(taken)

    def chunk_while(self, callback):
        """
        Chunks the collection into runs while the callback holds for the run.

        The callback receives ``(current_item, current_chunk)`` where ``current_chunk``
        is the Collection built so far; a falsy result starts a new chunk.

        Args:
            callback: The callback deciding whether the item joins the current chunk.

        Returns:
            A new Collection of Collection chunks.
        """
        self._check_is_callable(callback)

        chunks = []
        current = None

        for item in self._items:
            if current is None:
                current = [item]
            elif callback(item, self.__class__(current)):
                current.append(item)
            else:
                chunks.append(self.__class__(current))
                current = [item]

        if current is not None:
            chunks.append(self.__class__(current))

        return self.__class__(chunks)

    def split(self, groups: int):
        """
        Splits the collection into the given number of roughly-even groups.

        Earlier groups receive the extra items when the count does not divide evenly.

        Args:
            groups: The number of groups to split into.

        Returns:
            A new Collection of Collection groups.
        """
        return self.split_in(groups)

    def sort_keys(self, desc=False):
        """
        Sorts a dict-backed collection by its keys.

        For a list-backed collection this is a no-op copy (keys are positional).

        Args:
            desc: Whether to sort the keys in descending order.

        Returns:
            A new Collection instance sorted by key.
        """
        if isinstance(self._items, dict):
            ordered = sorted(self._items.keys(), reverse=desc)
            return self.__class__({key: self._items[key] for key in ordered})

        return self.__class__(list(self._items))

    def sort_keys_desc(self):
        """
        Sorts a dict-backed collection by its keys in descending order.

        Returns:
            A new Collection instance sorted by key descending.
        """
        return self.sort_keys(desc=True)

    def after(self, value, strict=False):
        """
        Returns the item that comes after the first match of the given value.

        Args:
            value: A callable predicate, or a value to match against each item.
            strict: Whether to use identity comparison for value matching.

        Returns:
            The following item, or None if there is no match or it is the last item.
        """
        items = list(self._items)

        for index, item in enumerate(items):
            if callable(value):
                matched = value(item)
            elif strict:
                matched = item is value or item == value and type(item) is type(value)
            else:
                matched = item == value

            if matched:
                if index + 1 < len(items):
                    return items[index + 1]
                return None

        return None

    def before(self, value, strict=False):
        """
        Returns the item that comes before the first match of the given value.

        Args:
            value: A callable predicate, or a value to match against each item.
            strict: Whether to use identity comparison for value matching.

        Returns:
            The preceding item, or None if there is no match or it is the first item.
        """
        items = list(self._items)

        for index, item in enumerate(items):
            if callable(value):
                matched = value(item)
            elif strict:
                matched = item is value or item == value and type(item) is type(value)
            else:
                matched = item == value

            if matched:
                if index - 1 >= 0:
                    return items[index - 1]
                return None

        return None

    def contains_one_item(self):
        """
        Determines whether the collection holds exactly one item.

        Returns:
            True if the collection contains exactly one item, False otherwise.
        """
        return self.count() == 1

    def replace(self, items):
        """
        Overlays the given items onto the collection by key.

        For dict-backed collections, matching keys are overwritten; for list-backed
        collections, matching positional indices are replaced.

        Args:
            items: The replacement items (a dict, iterable, or Collection).

        Returns:
            A new Collection instance with the overlaid items.
        """
        items = self.__get_items(items)

        if isinstance(self._items, dict) or isinstance(items, dict):
            base = dict(self._items) if isinstance(self._items, dict) else dict(enumerate(self._items))
            overlay = items if isinstance(items, dict) else dict(enumerate(items))
            base.update(overlay)
            return self.__class__(base)

        result = list(self._items)
        for index, value in enumerate(items):
            if index < len(result):
                result[index] = value
            else:
                result.append(value)

        return self.__class__(result)

    def map_spread(self, callback):
        """
        Maps over the collection, spreading each item as positional arguments.

        Each item is expected to be a tuple or list, unpacked into the callback.

        Args:
            callback: The callback receiving each item's elements as arguments.

        Returns:
            A new Collection instance with the mapped items.
        """
        self._check_is_callable(callback)
        return self.__class__([callback(*item) for item in self._items])

    def where_instance_of(self, types):
        """
        Keeps only the items that are instances of the given type(s).

        Args:
            types: A type or tuple of types to filter by.

        Returns:
            A new Collection instance with the matching items.
        """
        if not isinstance(types, tuple):
            types = (types,)

        return self.__class__([item for item in self._items if isinstance(item, types)])

    def cross_join(self, *lists):
        """
        Produces the cartesian product of the collection with the given lists.

        Args:
            *lists: The iterables (or Collections) to cross-join with.

        Returns:
            A new Collection of lists, one per combination.
        """
        sequences = [list(self._items)]
        for other in lists:
            sequences.append(list(self.__get_items(other)))

        results = [[]]
        for sequence in sequences:
            results = [combo + [item] for combo in results for item in sequence]

        return self.__class__(results)

    @classmethod
    def range(cls, start, stop):
        """
        Creates a Collection of integers from start to stop inclusive.

        Args:
            start: The first value of the range.
            stop: The last value of the range (inclusive).

        Returns:
            A new Collection instance of the integer range.
        """
        if start <= stop:
            return cls(list(range(start, stop + 1)))

        return cls(list(range(start, stop - 1, -1)))

    @classmethod
    def times(cls, number, callback=None):
        """
        Creates a Collection by invoking the callback ``number`` times.

        The callback receives the 1-based iteration index. With no callback the
        collection holds the integers ``1..number``.

        Args:
            number: The number of items to create.
            callback: An optional callback receiving each 1-based index.

        Returns:
            A new Collection instance.
        """
        if number < 1:
            return cls([])

        if callback is None:
            return cls(list(range(1, number + 1)))

        return cls([callback(index) for index in range(1, number + 1)])


def collect(iterable=None):
    """
    Transform an iterable into a collection.

    This function creates a new Collection instance from the given iterable.

    Args:
        iterable: The iterable to collect.

    Returns:
        A new Collection instance with the items from the iterable.
    """
    return Collection(iterable)


def flatten(iterable):
    """
    Flatten all sub-iterables of an iterable structure (recursively).

    This function flattens a multi-dimensional iterable into a single dimension.

    Args:
        iterable: The iterable to flatten.

    Returns:
        A flattened list.
    """
    flat_list = []
    for item in iterable:
        if isinstance(item, list):
            for subitem in flatten(item):
                flat_list.append(subitem)
        else:
            flat_list.append(item)

    return flat_list
