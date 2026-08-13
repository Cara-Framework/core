"""Windowing and sequence operations for ``Collection``."""

from __future__ import annotations

import builtins


def _collection_make(cls, items=None):
    """
    Creates a new Collection instance from the given items.

    Args:
        items: The items to wrap. Defaults to an empty collection if None.

    Returns:
        A new Collection instance.
    """
    return cls(items)


def _collection_skip_while(self, callback):
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


def _collection_skip_until(self, callback):
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


def _collection_take_while(self, callback):
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


def _collection_take_until(self, callback):
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


def _collection_chunk_while(self, callback):
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


def _collection_split(self, groups: int):
    """
    Splits the collection into the given number of roughly-even groups.

    Earlier groups receive the extra items when the count does not divide evenly.

    Args:
        groups: The number of groups to split into.

    Returns:
        A new Collection of Collection groups.
    """
    return self.split_in(groups)


def _collection_sort_keys(self, desc=False):
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


def _collection_sort_keys_desc(self):
    """
    Sorts a dict-backed collection by its keys in descending order.

    Returns:
        A new Collection instance sorted by key descending.
    """
    return self.sort_keys(desc=True)


def _collection_after(self, value, strict=False):
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


def _collection_before(self, value, strict=False):
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


def _collection_contains_one_item(self):
    """
    Determines whether the collection holds exactly one item.

    Returns:
        True if the collection contains exactly one item, False otherwise.
    """
    return self.count() == 1


def _collection_replace(self, items):
    """
    Overlays the given items onto the collection by key.

    For dict-backed collections, matching keys are overwritten; for list-backed
    collections, matching positional indices are replaced.

    Args:
        items: The replacement items (a dict, iterable, or Collection).

    Returns:
        A new Collection instance with the overlaid items.
    """
    items = self._get_items(items)

    if isinstance(self._items, dict) or isinstance(items, dict):
        base = (
            dict(self._items)
            if isinstance(self._items, dict)
            else dict(enumerate(self._items))
        )
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


def _collection_map_spread(self, callback):
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


def _collection_where_instance_of(self, types):
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


def _collection_cross_join(self, *lists):
    """
    Produces the cartesian product of the collection with the given lists.

    Args:
        *lists: The iterables (or Collections) to cross-join with.

    Returns:
        A new Collection of lists, one per combination.
    """
    sequences = [list(self._items)]
    for other in lists:
        sequences.append(list(self._get_items(other)))

    results = [[]]
    for sequence in sequences:
        results = [combo + [item] for combo in results for item in sequence]

    return self.__class__(results)


def _collection_range(cls, start, stop):
    """
    Creates a Collection of integers from start to stop inclusive.

    Args:
        start: The first value of the range.
        stop: The last value of the range (inclusive).

    Returns:
        A new Collection instance of the integer range.
    """
    if start <= stop:
        return cls(list(builtins.range(start, stop + 1)))

    return cls(list(builtins.range(start, stop - 1, -1)))


def _collection_times(cls, number, callback=None):
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
        return cls(list(builtins.range(1, number + 1)))

    return cls([callback(index) for index in builtins.range(1, number + 1)])
