"""Mutation, ordering and serialization operations for ``Collection``."""

from __future__ import annotations

import builtins
import json
import random as random_module
from functools import reduce as functools_reduce

from cara.exceptions import InvalidArgumentException


def _collection_pop(self):
    """
    Removes and returns the last item from the collection.

    Returns:
        The last item in the collection.
    """
    last = self._items.pop()
    return last


def _collection_prepend(self, value, key=None):
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


def _collection_pull(self, key):
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


def _collection_push(self, value):
    """
    Adds an item to the end of the collection.

    Args:
        value: The item to add.

    Returns:
        The collection instance.
    """
    self._items.append(value)
    return self


def _collection_put(self, key, value):
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


def _collection_random(self, count=None):
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
        raise InvalidArgumentException(
            "count argument must be inferior to collection length."
        )
    elif count:
        items = random_module.sample(self._items, k=count)
        return self.__class__(items)
    else:
        return random_module.choice(self._items)


def _collection_reduce(self, callback, initial=0):
    """
    Reduces the collection to a single value using the callback.

    Args:
        callback: The reduction callback.
        initial: The initial value.

    Returns:
        The reduced value.
    """
    return functools_reduce(callback, self, initial)


def _collection_reject(self, callback):
    """
    Filters the collection using the given callback, removing items that pass.

    Args:
        callback: The truth test callback.

    Returns:
        A new Collection instance with the filtered items.
    """
    self._check_is_callable(callback)
    return self.__class__([x for x in self if not callback(x)])


def _collection_reverse(self):
    """
    Reverses the order of the collection's items.

    Returns:
        The collection instance.
    """
    self._items = self._items[::-1]
    return self


def _collection_search(self, value, strict=False):
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
            # strict: type must match too (Laravel's === semantics) —
            # the two branches used to be identical, making the
            # parameter a no-op.
            if strict:
                if type(item) is type(value) and item == value:
                    return key
            elif item == value:
                return key

    return False


def _collection_serialize(self):
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


def _collection_add_relation(self, result=None):
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


def _collection_shift(self):
    """
    Removes and returns the first item from the collection.

    Returns:
        The first item in the collection.
    """
    return self.pull(0)


def _collection_sort(self, key=None):
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


def _collection_sort_by(self, callback=None):
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
                key=lambda x: (self._data_get_with_wildcards(x, callback) or [None])[0],
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


def _collection_sort_by_desc(self, callback=None):
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
                key=lambda x: (self._data_get_with_wildcards(x, callback) or [None])[0],
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


def _collection_sum(self, key=None):
    """
    Returns the sum of all items in the collection.

    Args:
        key: The key to sum by.

    Returns:
        The sum of the items, or ``0`` for an empty collection — the empty
        sum is the additive identity in Python and in Laravel, and this is
        the one aggregate where SQL's ``NULL`` is not the useful answer
        (see ``QueryBuilder.sum`` for the SQL-side contract).

    Raises:
        TypeError: when the items are not mutually summable.

    Pre-fix a ``contextlib.suppress(TypeError)`` wrapped the addition, so a
    single ``float`` leaking into a list of ``Decimal`` money — one un-cast
    column, one hand-built dict — turned a revenue total into ``0`` with no
    log line and no exception. Money is ``Decimal`` end-to-end; a
    collection that cannot be summed must say so, loudly.
    """
    items = (self._get_value(key) or []) if key is not None else self._items
    if not items:
        return 0
    return builtins.sum(items)


def _collection_to_json(self, **kwargs):
    """
    Converts the collection to JSON.

    Args:
        **kwargs: Additional arguments to pass to json.dumps.

    Returns:
        The JSON string.
    """
    return json.dumps(self.serialize(), **kwargs)


def _collection_to_array(self):
    """
    Converts the collection to a plain array.

    Returns:
        A plain array of the collection's items.
    """
    return self.serialize()
