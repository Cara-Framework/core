"""Python protocol and pipeline operations for ``Collection``."""

from __future__ import annotations

from cara.exceptions import InvalidArgumentException


def _collection_iter(self):
    """
    Allows the collection to be iterated over.

    Yields:
        Each item in the collection.
    """
    yield from self._items


def _collection_getitem(self, item):
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


def _collection_setitem(self, key, value):
    """
    Sets an item in the collection by key.

    Args:
        key: The key to set.
        value: The value to set.
    """
    self._items[key] = value


def _collection_delitem(self, key):
    """
    Removes an item from the collection by key.

    Args:
        key: The key to remove.
    """
    del self._items[key]


def _collection_ne(self, other):
    """
    Determines if the collection is not equal to the given value.

    Args:
        other: The value to compare with.

    Returns:
        True if the collection is not equal to the value, False otherwise.
    """
    other = self._get_items(other)
    return other != self._items


def _collection_len(self):
    """
    Gets the number of items in the collection.

    Returns:
        The number of items in the collection.
    """
    return len(self._items)


def _collection_le(self, other):
    """
    Determines if the collection is less than or equal to the given value.

    Args:
        other: The value to compare with.

    Returns:
        True if the collection is less than or equal to the value, False otherwise.
    """
    other = self._get_items(other)
    return self._items <= other


def _collection_lt(self, other):
    """
    Determines if the collection is less than the given value.

    Args:
        other: The value to compare with.

    Returns:
        True if the collection is less than the value, False otherwise.
    """
    other = self._get_items(other)
    return self._items < other


def _collection_ge(self, other):
    """
    Determines if the collection is greater than or equal to the given value.

    Args:
        other: The value to compare with.

    Returns:
        True if the collection is greater than or equal to the value, False otherwise.
    """
    other = self._get_items(other)
    return self._items >= other


def _collection_gt(self, other):
    """
    Determines if the collection is greater than the given value.

    Args:
        other: The value to compare with.

    Returns:
        True if the collection is greater than the value, False otherwise.
    """
    other = self._get_items(other)
    return self._items > other


def _collection_partition(self, callback):
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


def _collection_pipe(self, callback):
    """
    Pass the collection through the given callback and return the result.

    Args:
        callback: The callback to process the collection

    Returns:
        The result of the callback
    """
    return callback(self)


def _collection_tap(self, callback):
    """
    Pass the collection to the callback and return the collection.

    Args:
        callback: The callback to receive the collection

    Returns:
        The collection instance
    """
    callback(self)
    return self


def _collection_nth(self, step, offset=0):
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


def _collection_sliding(self, size=2, step=1):
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


def _collection_pad(self, target_size, value):
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


def _collection_items(self):
    """
    Get the underlying items as dictionary items.

    Returns:
        Dictionary items if collection is a dict, otherwise the items themselves.
    """
    if isinstance(self._items, dict):
        return self._items.items()
    return self._items


def _collection_ensure(self, *types):
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


def _collection_key_by(self, key):
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


def _collection_flat_map(self, callback):
    """
    Maps each item using the callback then collapses the result one level.

    Args:
        callback: The mapping callback applied to each item.

    Returns:
        A new Collection instance with the mapped-and-collapsed items.
    """
    self._check_is_callable(callback)
    return self.map(callback).collapse()


def _collection_concat(self, items):
    """
    Appends the given items onto the end of the collection.

    Unlike ``merge``, this never overwrites by key — values are always appended,
    and a new Collection is returned (the original is left untouched).

    Args:
        items: The items to append (an iterable or another Collection).

    Returns:
        A new Collection instance with the concatenated items.
    """
    items = self._get_items(items)

    if isinstance(items, dict):
        appended = list(items.values())
    else:
        appended = list(items)

    return self.__class__(list(self._items) + appended)


def _collection_when_empty(self, callback, default=None):
    """
    Applies the callback when the collection is empty.

    Args:
        callback: The callback to apply if the collection is empty.
        default: The callback to apply if the collection is not empty.

    Returns:
        The result of the chosen callback, or the collection instance.
    """
    return self.when(self.is_empty(), callback, default)


def _collection_when_not_empty(self, callback, default=None):
    """
    Applies the callback when the collection is not empty.

    Args:
        callback: The callback to apply if the collection is not empty.
        default: The callback to apply if the collection is empty.

    Returns:
        The result of the chosen callback, or the collection instance.
    """
    return self.when(self.is_not_empty(), callback, default)


def _collection_has(self, *keys):
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
            except IndexError, KeyError, TypeError:
                return False

    return True


def _collection_value(self, key, default=None):
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
