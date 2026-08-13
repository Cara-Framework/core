"""KeyPart."""

from __future__ import annotations


class KeyPart:
    """
    Represents a part of a key path in query string parsing.

    Examples:
    - 'foo' -> KeyPart(name='foo', is_array_key=False)
    - 'items[0]' -> KeyPart(name='items', is_array_key=True, index=0)
    """

    def __init__(self, name: str, is_array_key: bool = False, index: int = 0):
        self.name = name
        self.is_array_key = is_array_key
        self.index = index

    def __repr__(self) -> str:
        if self.is_array_key:
            return f"KeyPart(name='{self.name}', array[{self.index}])"
        return f"KeyPart(name='{self.name}', object)"
