"""
HTTP Header Management Module.

This module provides classes for managing HTTP headers in the Cara framework, implementing case-
insensitive header handling, header bags, and proper header formatting for ASGI compatibility.
"""

from typing import Dict, List, Optional, Tuple

from inflection import titleize


class Header:
    """
    Single HTTP header representation.

    This class represents a single HTTP header with proper name and value handling. It ensures
    header names are case-insensitive and values are properly encoded.
    """

    def __init__(self, name: str, value: str):
        """
        Initialize a header.

        Args:
            name: Header name
            value: Header value
        """
        self.name = name
        self.value = value

    def __str__(self) -> str:
        """String representation of header."""
        return f"{self.name}: {self.value}"

    def __repr__(self) -> str:
        """Debug representation of header."""
        return f"Header(name='{self.name}', value='{self.value}')"


class HeaderBag:
    """
    Container for managing HTTP headers.

    This class provides a container for managing multiple HTTP headers with case-insensitive access,
    duplication handling, and proper ASGI formatting.
    """

    def __init__(self):
        """Initialize an empty header bag."""
        self.bag: Dict[str, Header] = {}

    def add(self, header: "Header") -> None:
        """
        Add or update a header in the bag.

        If a header with the same name already exists, its value will be updated
        instead of creating a duplicate entry.

        Args:
            header: Header object to add/update
        """
        name_lower = header.name.lower()
        if name_lower in self.bag:
            self.bag[name_lower].value = header.value
        else:
            self.bag[name_lower] = header

    def set(self, name: str, value: str) -> None:
        """
        Set a header by name and value.

        Args:
            name: Header name
            value: Header value
        """
        self.add(Header(name, value))

    def add_if_not_exists(self, header: "Header") -> None:
        """
        Add a header only if it doesn't exist.

        Args:
            header: Header object to add
        """
        name_lower = header.name.lower()
        self.bag.setdefault(name_lower, header)

    def get_raw(self, name: str) -> Optional["Header"]:
        """
        Get a raw header object by name.

        Args:
            name: Header name to get

        Returns:
            Header object if found, None otherwise
        """
        return self.bag.get(name.lower())

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a header value by name with optional default.

        Args:
            name: Header name to get
            default: Default value if header not found

        Returns:
            Header value if found, default otherwise
        """
        header = self.get_raw(name)
        return header.value if header else default

    def has(self, name: str) -> bool:
        """
        Check if a header exists.

        Args:
            name: Header name to check

        Returns:
            True if header exists, False otherwise
        """
        return name.lower() in self.bag

    def remove(self, name: str) -> bool:
        """
        Remove a header by name.

        Args:
            name: Header name to remove

        Returns:
            True if header was removed, False if it didn't exist
        """
        name_lower = name.lower()
        if name_lower in self.bag:
            del self.bag[name_lower]
            return True
        return False

    def all(self) -> Dict[str, str]:
        """
        Get all headers as a dictionary.

        Returns:
            Dictionary of header names and values
        """
        return {name: header.value for name, header in self.bag.items()}

    def keys(self) -> List[str]:
        """
        Get all header names.

        Returns:
            List of header names
        """
        return list(self.bag.keys())

    def values(self) -> List[str]:
        """
        Get all header values.

        Returns:
            List of header values
        """
        return [header.value for header in self.bag.values()]

    def items(self) -> List[Tuple[str, str]]:
        """
        Get all headers as tuples.

        Returns:
            List of (name, value) tuples
        """
        return [(name, header.value) for name, header in self.bag.items()]

    def render(self) -> List[Tuple[bytes, bytes]]:
        """
        Render headers in ASGI format.

        Returns:
            List of tuples containing header name and value as bytes
        """
        return [
            (
                name.encode("utf-8", errors="replace"),
                header.value.encode("utf-8", errors="replace"),
            )
            for name, header in self.bag.items()
        ]

    def __contains__(self, name: str) -> bool:
        """
        Check if a header exists.

        Args:
            name: Header name to check

        Returns:
            True if header exists, False otherwise
        """
        return name.lower() in self.bag

    def __len__(self) -> int:
        """Return number of headers."""
        return len(self.bag)

    def __str__(self) -> str:
        """String representation of header bag."""
        return "\n".join(str(header) for header in self.bag.values())

    def __repr__(self) -> str:
        """Debug representation of header bag."""
        return f"HeaderBag({len(self.bag)} headers)"

    # -----------------------
    # Header Name Conversion Utilities
    # -----------------------

    @staticmethod
    def normalize_name(name: str) -> str:
        """
        Normalize header name to standard format.

        Example: content-type -> Content-Type
        """
        return titleize(name).replace(" ", "-")

    @staticmethod
    def to_server_format(name: str) -> str:
        """
        Convert header name to server format.

        Example: X-Rate-Limited -> HTTP_X_RATE_LIMITED
        """
        if name.lower() == "content-type":
            return "CONTENT_TYPE"
        elif name.lower() == "content-length":
            return "CONTENT_LENGTH"
        else:
            return "HTTP_" + name.replace("-", "_").upper()

    @staticmethod
    def from_server_format(name: str) -> str:
        """
        Convert server header name to standard format.

        Example: HTTP_X_RATE_LIMITED -> X-Rate-Limited
        """
        if name == "CONTENT_TYPE":
            return "Content-Type"
        elif name == "CONTENT_LENGTH":
            return "Content-Length"
        elif name.startswith("HTTP_"):
            return titleize(name[5:].replace("_", " ")).replace(" ", "-")
        else:
            return titleize(name.replace("_", " ")).replace(" ", "-")

    def load(self, headers: Dict[str, str]) -> None:
        """
        Load headers from a dictionary.

        Args:
            headers: Dictionary of headers to load
        """
        for name, value in headers.items():
            name_lower = name.lower()
            if isinstance(value, bytes):
                value = value.decode("latin-1")
            self.add(Header(name_lower, value))

    def to_dict(self) -> Dict[str, str]:
        """
        Convert headers to a dictionary.

        Returns:
            Dictionary of header names and values
        """
        return self.all()

    def copy(self) -> "HeaderBag":
        """
        Create a deep copy of the header bag.

        Returns:
            A new HeaderBag instance with copied headers
        """
        new_bag = HeaderBag()
        for header in self.bag.values():
            new_bag.add(Header(header.name, header.value))
        return new_bag
