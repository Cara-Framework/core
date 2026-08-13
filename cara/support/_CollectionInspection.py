"""Selection and aggregate operations composed into ``Collection``."""

from __future__ import annotations

import builtins
from collections import Counter

from cara.exceptions import InvalidArgumentException


def _collection_take(self, number: int):
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


def _collection_skip(self, number: int):
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


def _collection_first(self, callback=None, default=None):
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


def _collection_last(self, callback=None, default=None):
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


def _collection_sole(self, callback=None):
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


def _collection_first_where(self, key, operator="==", value=None):
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


def _collection_all(self):
    """
    Returns all the items in the collection.

    Returns:
        All items in the collection.
    """
    return self._items


def _collection_avg(self, key=None):
    """
    Returns the average of the items.

    If a key is given it will return the average of all the values of the key.

    Args:
        key: The key to use to find the average of all the values of that key.

    Returns:
        The average value, or ``None`` for an empty collection — the same
        contract ``QueryBuilder.avg`` documents, so an aggregate answers
        the same way whether it was pushed to SQL or computed in Python.

    Raises:
        TypeError: when the items are not mutually summable.

    Pre-fix this returned ``0`` on both paths: an empty cohort reported an
    average of zero that averaged into every roll-up above it, and a
    ``contextlib.suppress(TypeError, ZeroDivisionError)`` turned a
    mixed ``Decimal``/``float`` money collection into a silent zero.
    Unknown is ``NULL``, never ``0``.
    """
    items = (self._get_value(key) or []) if key is not None else self._items
    if not items:
        return None
    return sum(items) / len(items)


def _collection_median(self, key=None):
    """
    Returns the median value of the items.

    If a key is given it will return the median of all the values of the key.

    Args:
        key: The key to use to find the median of all the values of that key.

    Returns:
        The median value, or ``None`` for an empty collection — matching
        the empty-set semantics ``QueryBuilder.avg`` / ``min`` / ``max``
        document.

    Raises:
        TypeError: when the items are not mutually comparable.

    Pre-fix both the empty case and an unorderable collection returned the
    literal ``0``, so "no data" and "this data is not comparable" were
    reported as a real measurement of zero.
    """
    items = (self._get_value(key) or []) if key is not None else self._items

    if not items:
        return None

    sorted_items = sorted(items)
    count = len(sorted_items)

    # Get the middle index
    middle = count // 2

    if count % 2 == 0:
        # If even number of items, average the two middle values
        return (sorted_items[middle - 1] + sorted_items[middle]) / 2
    else:
        # If odd number of items, return the middle value
        return sorted_items[middle]


def _collection_mode(self, key=None):
    """
    Returns the most frequent value in the collection.

    If a key is given it will return the mode of all the values of the key.

    Args:
        key: The key to use to find the mode of all the values of that key.

    Returns:
        The mode value or None if the collection is empty.
    """
    items = (self._get_value(key) or []) if key is not None else self._items

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
    except TypeError, ValueError:
        return None


def _collection_max(self, key=None):
    """
    Returns the maximum value of the items.

    If a key is given it will return the maximum of all the values of the key.

    Args:
        key: The key to use to find the maximum of all the values of that key.

    Returns:
        The maximum value, or ``None`` for an empty collection — the same
        contract ``QueryBuilder.max`` documents.

    Raises:
        TypeError: when the items are not mutually comparable.

    Pre-fix an empty or unorderable collection answered ``0``, which reads
    as a genuine ceiling of zero to every caller above it.
    """
    items = (self._get_value(key) or []) if key is not None else self._items

    if not items:
        return None
    return builtins.max(items)


def _collection_min(self, key=None):
    """
    Returns the minimum value of the items.

    If a key is given it will return the minimum of all the values of the key.

    Args:
        key: The key to use to find the minimum of all the values of that key.

    Returns:
        The minimum value, or ``None`` for an empty collection — the same
        contract ``QueryBuilder.min`` documents.

    Raises:
        TypeError: when the items are not mutually comparable.

    Pre-fix an empty or unorderable collection answered ``0``, which reads
    as a genuine floor of zero to every caller above it.
    """
    items = (self._get_value(key) or []) if key is not None else self._items

    if not items:
        return None
    return builtins.min(items)


def _collection_chunk(self, size: int):
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


def _collection_split_in(self, groups: int):
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


def _collection_collapse(self):
    """
    Collapses a collection of arrays into a single, flat collection.

    Returns:
        A new Collection instance with the collapsed items.
    """
    items = []
    for item in self:
        items += self._get_items(item)
    return self.__class__(items)


def _collection_contains(self, key, value=None):
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


def _collection_doesnt_contain(self, key, value=None):
    """
    Determines if the collection does not contain a given item or key-value pair.

    Args:
        key: The key or callback to check.
        value: The value to check if key is a property name.

    Returns:
        True if the collection does not contain the item, False otherwise.
    """
    return not self.contains(key, value)


def _collection_count(self):
    """
    Returns the total number of items in the collection.

    Returns:
        The count of items in the collection.
    """
    return len(self._items)


def _collection_count_by(self, callback=None):
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


def _collection_diff(self, items):
    """
    Returns the items in the collection that are not present in the given items.

    Args:
        items: The items to compare against.

    Returns:
        A new Collection instance with the differing items.
    """
    items = self._get_items(items)
    return self.__class__([x for x in self if x not in items])


def _collection_diff_assoc(self, items):
    """
    Returns the items in the collection whose keys and values are not present in the given
    items.

    Args:
        items: The items to compare against.

    Returns:
        A new Collection instance with the differing items.
    """
    items = self._get_items(items)

    if not isinstance(self._items, dict) or not isinstance(items, dict):
        return self.diff(items)

    return self.__class__(
        {k: v for k, v in self._items.items() if k not in items or items[k] != v}
    )


def _collection_diff_keys(self, items):
    """
    Returns the items in the collection whose keys are not present in the given items.

    Args:
        items: The items to compare against.

    Returns:
        A new Collection instance with the differing items.
    """
    items = self._get_items(items)

    if not isinstance(self._items, dict) or not isinstance(items, dict):
        return self.diff(items)

    return self.__class__({k: v for k, v in self._items.items() if k not in items})


def _collection_each(self, callback):
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
