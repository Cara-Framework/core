"""
Collection Module for Data Manipulation.

This module provides a powerful collection system for the Cara framework, implementing Laravel-style
collection methods with support for mapping, filtering, sorting, and aggregation operations.
"""

from __future__ import annotations

from itertools import groupby

from cara.support.Macroable import Macroable

from . import (
    _CollectionFiltering,
    _CollectionInspection,
    _CollectionMutation,
    _CollectionProtocol,
    _CollectionSequences,
    _CollectionTransforms,
)


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

    take = _CollectionInspection._collection_take
    skip = _CollectionInspection._collection_skip
    first = _CollectionInspection._collection_first
    last = _CollectionInspection._collection_last
    sole = _CollectionInspection._collection_sole
    first_where = _CollectionInspection._collection_first_where
    all = _CollectionInspection._collection_all
    avg = _CollectionInspection._collection_avg
    median = _CollectionInspection._collection_median
    mode = _CollectionInspection._collection_mode
    max = _CollectionInspection._collection_max
    min = _CollectionInspection._collection_min
    chunk = _CollectionInspection._collection_chunk
    split_in = _CollectionInspection._collection_split_in
    collapse = _CollectionInspection._collection_collapse
    contains = _CollectionInspection._collection_contains
    doesnt_contain = _CollectionInspection._collection_doesnt_contain
    count = _CollectionInspection._collection_count
    count_by = _CollectionInspection._collection_count_by
    diff = _CollectionInspection._collection_diff
    diff_assoc = _CollectionInspection._collection_diff_assoc
    diff_keys = _CollectionInspection._collection_diff_keys
    each = _CollectionInspection._collection_each

    every = _CollectionTransforms._collection_every
    filter = _CollectionTransforms._collection_filter
    when = _CollectionTransforms._collection_when
    unless = _CollectionTransforms._collection_unless
    flatten = _CollectionTransforms._collection_flatten
    forget = _CollectionTransforms._collection_forget
    only = _CollectionTransforms._collection_only
    except_keys = _CollectionTransforms._collection_except_keys
    for_page = _CollectionTransforms._collection_for_page
    slice = _CollectionTransforms._collection_slice
    keys = _CollectionTransforms._collection_keys
    values = _CollectionTransforms._collection_values
    get = _CollectionTransforms._collection_get
    implode = _CollectionTransforms._collection_implode
    is_empty = _CollectionTransforms._collection_is_empty
    is_not_empty = _CollectionTransforms._collection_is_not_empty
    map = _CollectionTransforms._collection_map
    map_with_keys = _CollectionTransforms._collection_map_with_keys
    map_into = _CollectionTransforms._collection_map_into
    merge = _CollectionTransforms._collection_merge
    combine = _CollectionTransforms._collection_combine

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

    pop = _CollectionMutation._collection_pop
    prepend = _CollectionMutation._collection_prepend
    pull = _CollectionMutation._collection_pull
    push = _CollectionMutation._collection_push
    put = _CollectionMutation._collection_put
    random = _CollectionMutation._collection_random
    reduce = _CollectionMutation._collection_reduce
    reject = _CollectionMutation._collection_reject
    reverse = _CollectionMutation._collection_reverse
    search = _CollectionMutation._collection_search
    serialize = _CollectionMutation._collection_serialize
    add_relation = _CollectionMutation._collection_add_relation
    shift = _CollectionMutation._collection_shift
    sort = _CollectionMutation._collection_sort
    sort_by = _CollectionMutation._collection_sort_by
    sort_by_desc = _CollectionMutation._collection_sort_by_desc
    sum = _CollectionMutation._collection_sum
    to_json = _CollectionMutation._collection_to_json
    to_array = _CollectionMutation._collection_to_array

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

    transform = _CollectionFiltering._collection_transform
    unique = _CollectionFiltering._collection_unique
    duplicates = _CollectionFiltering._collection_duplicates
    where = _CollectionFiltering._collection_where
    where_in = _CollectionFiltering._collection_where_in
    where_not_in = _CollectionFiltering._collection_where_not_in
    where_between = _CollectionFiltering._collection_where_between
    where_not_between = _CollectionFiltering._collection_where_not_between
    where_null = _CollectionFiltering._collection_where_null
    where_not_null = _CollectionFiltering._collection_where_not_null
    zip = _CollectionFiltering._collection_zip
    set_appends = _CollectionFiltering._collection_set_appends
    _get_value = _CollectionFiltering._get_value
    _data_get = _CollectionFiltering._data_get
    _data_get_with_wildcards = _CollectionFiltering._data_get_with_wildcards
    _extract_wildcard_path = _CollectionFiltering._extract_wildcard_path
    _value = _CollectionFiltering._value
    _check_is_callable = _CollectionFiltering._check_is_callable
    _make_comparison = _CollectionFiltering._make_comparison

    __iter__ = _CollectionProtocol._collection_iter

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

    __getitem__ = _CollectionProtocol._collection_getitem

    __setitem__ = _CollectionProtocol._collection_setitem

    __delitem__ = _CollectionProtocol._collection_delitem

    __ne__ = _CollectionProtocol._collection_ne

    __len__ = _CollectionProtocol._collection_len

    __le__ = _CollectionProtocol._collection_le

    __lt__ = _CollectionProtocol._collection_lt

    __ge__ = _CollectionProtocol._collection_ge

    __gt__ = _CollectionProtocol._collection_gt

    @classmethod
    def _get_items(cls, items):
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

    partition = _CollectionProtocol._collection_partition

    pipe = _CollectionProtocol._collection_pipe

    tap = _CollectionProtocol._collection_tap

    nth = _CollectionProtocol._collection_nth

    sliding = _CollectionProtocol._collection_sliding

    pad = _CollectionProtocol._collection_pad

    items = _CollectionProtocol._collection_items

    ensure = _CollectionProtocol._collection_ensure

    key_by = _CollectionProtocol._collection_key_by

    flat_map = _CollectionProtocol._collection_flat_map

    concat = _CollectionProtocol._collection_concat

    when_empty = _CollectionProtocol._collection_when_empty

    when_not_empty = _CollectionProtocol._collection_when_not_empty

    has = _CollectionProtocol._collection_has

    value = _CollectionProtocol._collection_value

    make = classmethod(_CollectionSequences._collection_make)

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

    skip_while = _CollectionSequences._collection_skip_while

    skip_until = _CollectionSequences._collection_skip_until

    take_while = _CollectionSequences._collection_take_while

    take_until = _CollectionSequences._collection_take_until

    chunk_while = _CollectionSequences._collection_chunk_while

    split = _CollectionSequences._collection_split

    sort_keys = _CollectionSequences._collection_sort_keys

    sort_keys_desc = _CollectionSequences._collection_sort_keys_desc

    after = _CollectionSequences._collection_after

    before = _CollectionSequences._collection_before

    contains_one_item = _CollectionSequences._collection_contains_one_item

    replace = _CollectionSequences._collection_replace

    map_spread = _CollectionSequences._collection_map_spread

    where_instance_of = _CollectionSequences._collection_where_instance_of

    cross_join = _CollectionSequences._collection_cross_join

    range = classmethod(_CollectionSequences._collection_range)

    times = classmethod(_CollectionSequences._collection_times)


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
