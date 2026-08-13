"""Transformation operations composed into ``Collection``."""

from __future__ import annotations

import builtins

from cara.exceptions import InvalidArgumentException


def _collection_every(self, callback):
    """
    Determines if all items in the collection pass the given truth test.

    Args:
        callback: The truth test callback.

    Returns:
        True if all items pass the test, False otherwise.
    """
    self._check_is_callable(callback)
    return all([callback(x) for x in self])


def _collection_filter(self, callback=None):
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
    return self.__class__(list(builtins.filter(callback, self)))


def _collection_when(self, value, callback, default=None):
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


def _collection_unless(self, value, callback, default=None):
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


def _collection_flatten(self, depth=float("inf")):
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
                yield from _flatten(v, current_depth + 1)
        elif isinstance(items, (list, tuple)):
            for i in items:
                yield from _flatten(i, current_depth + 1)
        else:
            yield items

    return self.__class__(list(_flatten(self._items)))


def _collection_forget(self, *keys):
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


def _collection_only(self, *keys):
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


def _collection_except_keys(self, *keys):
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


def _collection_for_page(self, page, per_page):
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


def _collection_slice(self, offset, length=None):
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


def _collection_keys(self):
    """
    Get all the keys of the collection items.

    Returns:
        A new Collection instance with the keys.
    """
    if isinstance(self._items, dict):
        return self.__class__(list(self._items.keys()))

    return self.__class__(list(range(len(self._items))))


def _collection_values(self):
    """
    Get all the values of the collection items.

    Returns:
        A new Collection instance with the values.
    """
    if isinstance(self._items, dict):
        return self.__class__(list(self._items.values()))

    return self.__class__(self._items)


def _collection_get(self, key, default=None):
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
    except IndexError, KeyError:
        pass

    return self._value(default)


def _collection_implode(self, glue=",", key=None):
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


def _collection_is_empty(self):
    """
    Determines if the collection is empty.

    Returns:
        True if the collection is empty, False otherwise.
    """
    return not self


def _collection_is_not_empty(self):
    """
    Determines if the collection is not empty.

    Returns:
        True if the collection is not empty, False otherwise.
    """
    return not self.is_empty()


def _collection_map(self, callback):
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


def _collection_map_with_keys(self, callback):
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


def _collection_map_into(self, cls, method=None, **kwargs):
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


def _collection_merge(self, items):
    """
    Merges the given items into the collection.

    Args:
        items: The items to merge.

    Returns:
        The collection instance.
    """
    items = self._get_items(items)

    if isinstance(self._items, dict) and isinstance(items, dict):
        self._items.update(items)
        return self

    if not isinstance(items, list):
        raise InvalidArgumentException("Unable to merge incompatible types")

    self._items += items
    return self


def _collection_combine(self, values):
    """
    Combines the keys of the collection with the values of another collection.

    Args:
        values: The values to combine with the keys.

    Returns:
        A new Collection instance with the combined items.
    """
    values = self._get_items(values)

    if len(self._items) != len(values):
        raise InvalidArgumentException(
            "The number of keys must match the number of values"
        )

    return self.__class__(dict(zip(self._items, values, strict=False)))
