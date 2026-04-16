"""String generators and helpers."""

import re
import secrets
import string
from typing import Any
from urllib import parse


def random_string(length: int = 4) -> str:
    """
    Generate a random alphanumeric string of ``length`` characters.

    Uses :mod:`secrets` (CSPRNG) because callers often rely on this helper
    for tokens, nonces, or IDs where predictability would be a security risk.

    Keyword Arguments:
        length {int} -- Number of characters to generate (default: 4).

    Returns:
        string
    """
    if length < 0:
        raise ValueError("length must be non-negative")
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


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
    if isinstance(cents, bool):
        raise TypeError("cents must be an integer")
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


def truncate(text: str, limit: int, suffix: str = "...") -> str:
    """Truncate a string to a given length and append a suffix if needed.

    If the text is shorter than or equal to the limit, it is returned unchanged.
    Otherwise, it is truncated to the limit and the suffix is appended.

    Returns empty string for empty/None input.
    """
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + suffix


def title_case(text: str) -> str:
    """Convert a string to title case (capitalize first letter of each word).

    Splits on whitespace, underscores, and hyphens. Each word's first letter
    is capitalized and the rest are lowercased. Words are joined with spaces.

    Returns empty string for empty/None input or strings made entirely of separators.
    """
    if not text:
        return ""
    # Drop empty tokens so leading/trailing separators don't produce extra spaces
    # (e.g. "  foo_bar  " -> "Foo Bar").
    words = [w for w in re.split(r"[\s_-]+", text) if w]
    if not words:
        return ""
    return " ".join(word.capitalize() for word in words)


def snake_case(text: str) -> str:
    """Convert a string to snake_case.

    Inserts underscores before uppercase letters (camelCase → camel_Case),
    replaces hyphens/spaces/consecutive non-alphanumeric chars with single underscore,
    lowercases everything, and strips leading/trailing underscores.

    Returns empty string for empty/None input.
    """
    if not text:
        return ""
    # Insert underscores before uppercase letters
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    # Replace non-alphanumeric characters with underscores
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text)
    # Lowercase and strip leading/trailing underscores
    return text.lower().strip("_")


def kebab_case(text: str) -> str:
    """Convert a string to kebab-case.

    Same logic as snake_case but uses hyphens instead of underscores.

    Returns empty string for empty/None input.
    """
    if not text:
        return ""
    # Insert hyphens before uppercase letters
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", text)
    # Replace non-alphanumeric characters with hyphens
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text)
    # Lowercase and strip leading/trailing hyphens
    return text.lower().strip("-")


def camel_case(text: str) -> str:
    """Convert a string to camelCase.

    Splits on whitespace, underscores, and hyphens. The first word is fully
    lowercased; subsequent words have their first letter capitalized and the
    rest lowercased. Words are joined without any separator.

    Returns empty string for empty/None input.
    """
    if not text:
        return ""
    # Drop empty tokens so leading/trailing separators don't produce a
    # capitalized first word (e.g. "  foo_bar  " -> "fooBar").
    words = [w for w in re.split(r"[\s_-]+", text) if w]
    if not words:
        return ""
    first_word = words[0].lower()
    other_words = "".join(word.capitalize() for word in words[1:])
    return first_word + other_words


def studly_case(text: str) -> str:
    """Convert a string to StudlyCase (PascalCase).

    Splits on whitespace, underscores, and hyphens. Each word has its first
    letter capitalized and the rest lowercased. Words are joined without any
    separator.

    Returns empty string for empty/None input.
    """
    if not text:
        return ""
    # Drop empty tokens so leading/trailing separators don't produce a
    # capitalized first word (e.g. "  foo_bar  " -> "FooBar").
    words = [w for w in re.split(r"[\s_-]+", text) if w]
    if not words:
        return ""
    return "".join(word.capitalize() for word in words)


def pluralize(word: str) -> str:
    """Pluralize an English word using simple rule-based heuristics.

    Rules (applied in order):
    - Empty/None -> empty string.
    - consonant + 'y' -> replace 'y' with 'ies' (baby -> babies).
    - vowel + 'y' -> add 's' (day -> days).
    - ends with 's', 'x', 'z', 'ch', 'sh' -> add 'es' (box -> boxes).
    - consonant + 'o' -> add 'es' (hero -> heroes).
    - otherwise -> add 's'.

    Intentionally dumb — does not handle irregular nouns (child, foot).
    Returns empty string for empty/None input.
    """
    if not word:
        return ""
    vowels = "aeiou"
    lower = word.lower()
    if len(lower) >= 2 and lower[-1] == "y" and lower[-2] not in vowels:
        return word[:-1] + "ies"
    if lower.endswith(("s", "x", "z")) or lower.endswith(("ch", "sh")):
        return word + "es"
    if len(lower) >= 2 and lower[-1] == "o" and lower[-2] not in vowels:
        return word + "es"
    return word + "s"
