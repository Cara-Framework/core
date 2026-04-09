"""String generators and helpers."""

import random
import re
import string
from typing import Any
from urllib import parse


def random_string(length=4):
    """
    Generate a random string based on the given length.

    Keyword Arguments:
        length {int} -- The amount of the characters to generate (default: {4})

    Returns:
        string
    """
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def modularize(file_path, suffix=".py"):
    """
    Transforms a file path to a dotted path. On UNIX paths contains / and on Windows \\.

    Keyword Arguments:
        file_path {str} -- A file path such app/controllers

    Returns:
        value {str} -- a dotted path such as app.controllers
    """
    # if the file had the .py extension remove it as it's not needed for a module
    return removesuffix(
        file_path.replace("/", ".").replace("\\", "."),
        suffix,
    )


def as_filepath(dotted_path):
    """
    Inverse of modularize, transforms a dotted path to a file path (with /).

    Keyword Arguments:
        dotted_path {str} -- A dotted path such app.controllers

    Returns:
        value {str} -- a file path such as app/controllers
    """
    return dotted_path.replace(".", "/")


def removeprefix(string, prefix):
    """Implementation of str.removeprefix() function available for Python versions lower than
    3.9."""
    if string.startswith(prefix):
        return string[len(prefix) :]
    else:
        return string


def removesuffix(string, suffix):
    """Implementation of str.removesuffix() function available for Python versions lower than
    3.9."""
    if suffix and string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        return string


def match(string: str, ref_string: str) -> str:
    """
    Check if a given string matches a reference string.

    Wildcard '*' can be used at start, end or middle of the string.
    """
    if ref_string.startswith("*"):
        ref_string = ref_string.replace("*", "")
        return string.endswith(ref_string)
    elif ref_string.endswith("*"):
        ref_string = ref_string.replace("*", "")
        return string.startswith(ref_string)
    elif "*" in ref_string:
        split_search = ref_string.split("*")
        return string.startswith(split_search[0]) and string.endswith(split_search[1])
    else:
        return ref_string == string


def add_query_params(url: str, query_params: dict) -> str:
    """Add query params dict to a given url (which can already contain some query parameters)."""
    path_result = parse.urlsplit(url)

    base_url = (
        f"{path_result.scheme}://{path_result.hostname}" if path_result.hostname else ""
    )
    base_path = path_result.path

    # parse existing query parameters if any
    existing_query_params = dict(parse.parse_qsl(path_result.query))
    all_query_params = {
        **existing_query_params,
        **query_params,
    }

    # add query parameters to url if any
    if all_query_params:
        base_path += "?" + parse.urlencode(all_query_params)

    return f"{base_url}{base_path}"


def get_controller_name(controller: "str|Any") -> str:
    """Get a controller string name from a controller argument used in routes."""
    # controller is a class or class.method
    if hasattr(controller, "__qualname__"):
        if "." in controller.__qualname__:
            controller_str = controller.__qualname__.replace(".", "@")
        else:
            controller_str = f"{controller.__qualname__}@__call__"
    # controller is an instance, so the method will automatically be __call__
    elif not isinstance(controller, str):
        controller_str = f"{controller.__class__.__qualname__}@__call__"
    # controller is anything else: "Controller@method"
    else:
        controller_str = str(controller)
    return controller_str


def slugify(text: str, separator: str = "-") -> str:
    """Convert a string to a URL-friendly slug.

    Handles common Unicode transliterations (Turkish chars, accented letters).
    Non-alphanumeric characters become the separator. Leading/trailing
    separators and consecutive separators are removed.

    Returns empty string for empty/whitespace-only input.
    """
    if not text or text.isspace():
        return ""

    # Common character transliterations
    char_map = {
        "ç": "c", "ğ": "g", "ı": "i", "ş": "s", "ö": "o", "ü": "u",
        "Ç": "C", "Ğ": "G", "İ": "I", "Ş": "S", "Ö": "O", "Ü": "U",
        "à": "a", "á": "a", "â": "a", "ã": "a", "ä": "a",
        "è": "e", "é": "e", "ê": "e", "ë": "e",
        "ì": "i", "í": "i", "î": "i", "ï": "i",
        "ò": "o", "ó": "o", "ô": "o", "õ": "o",
        "ù": "u", "ú": "u", "û": "u",
        "ñ": "n", "ß": "ss",
    }
    for char, replacement in char_map.items():
        text = text.replace(char, replacement)

    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", separator, text)
    text = re.sub(rf"{re.escape(separator)}+", separator, text)
    text = text.strip(separator)
    return text


def normalize_email(email: str) -> str:
    """Normalize an email address by lowercasing and stripping whitespace.

    Returns empty string for empty/whitespace-only input.
    """
    if not email or not email.strip():
        return ""
    return email.strip().lower()


def format_money(cents: int, currency: str = "USD") -> str:
    """Format an integer cent amount as a currency string.

    Raises TypeError if cents is not an int.
    Raises ValueError if cents is negative or currency is unsupported.

    Supported currencies: USD ($), EUR (€), GBP (£), TRY (₺), AUD (A$), CAD (C$).
    Output: "<symbol><whole>.<frac>" with comma thousands separator.
    """
    if not isinstance(cents, int):
        raise TypeError("cents must be an integer")
    if cents < 0:
        raise ValueError("cents must be non-negative")

    currency = currency.upper()
    symbols = {
        "USD": "$", "EUR": "€", "GBP": "£", "TRY": "₺",
        "AUD": "A$", "CAD": "C$",
    }
    if currency not in symbols:
        raise ValueError(f"unsupported currency: {currency}")

    symbol = symbols[currency]
    whole = cents // 100
    frac = cents % 100
    return f"{symbol}{whole:,}.{frac:02d}"
