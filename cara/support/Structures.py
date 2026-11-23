"""Helpers for multiple data structures."""

from __future__ import annotations

import re

from dotty_dict import dotty

from cara.exceptions import InvalidArgumentException

#: A path separator: a dot not escaped as ``\\.`` (dotty's escape for a key
#: that itself contains a dot).
_SEPARATOR = re.compile(r"(?<!\\)\.")


def data(dictionary=None):
    """
    Transform the given dictionary to be read/written with dot notation.

    Arguments:
        dictionary {dict} -- a dictionary structure

    Returns:
        {dict} -- A dot dictionary
    """
    return dotty(dictionary if dictionary is not None else {})


def data_get(dictionary, key, default=None):
    """
    Read dictionary value from key using nested notation.

    Arguments:
        dictionary {dict} -- a dictionary structure
        key {str} -- the dotted (or not) key to look for
        default {object} -- the default value to return if the key does not exist (None)

    Returns:
        value {object}
    """
    # dotty dict uses : instead of * for wildcards
    dotty_key = key.replace("*", ":")
    dotted = data(dictionary)
    try:
        value = dotted.get(dotty_key, default)
    except TypeError:
        # A scalar on the path (``{"address": 5}`` read as ``address.city``)
        # has no children: the key is absent. Dotty tests ``"city" in 5`` and
        # raised, which turned a malformed request body into a 500 at
        # ``request.input("address.city")``.
        return default
    if value is None and _parent_is_none(dotted, dotty_key):
        # Dotty stops at a None PARENT and returns that None as if it were the
        # value: ``{"a": None}`` read as ``a.b`` answered None, not the
        # default. A None parent has no children, so the key is absent.
        return default
    return value


def _parent_is_none(dotted, dotty_key: str) -> bool:
    """Whether some proper prefix of the dotted path resolves to None."""
    for match in _SEPARATOR.finditer(dotty_key):
        try:
            if dotted.get(dotty_key[: match.start()]) is None:
                return True
        except TypeError:
            return False
    return False


def data_set(dictionary, key, value, overwrite=True):
    """
    Set dictionary value at key using nested notation. Values are overridden by default but this
    behaviour can be changed by passing overwrite=False. The dictionary is edited in place but is
    also returned.

    Arguments:
        dictionary {dict} -- a dictionary structure
        key {str} -- the dotted (or not) key to set
        value {object} -- the value to set at key
        overwrite {bool} -- override the value if key exists in dictionary (True)

    Returns:
        dictionary {dict} -- the edited dictionary
    """
    if "*" in key:
        raise InvalidArgumentException("You cannot set values with wildcards *")
    if not overwrite and data_get(dictionary, key):
        return
    data(dictionary)[key] = value
    return dictionary
