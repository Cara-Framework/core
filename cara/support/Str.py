"""String generators and helpers."""

import html
import re
import secrets
import string
import unicodedata
from typing import Any
from urllib import parse


# --- Sanitization ---------------------------------------------------------
# These patterns target the "user-supplied free text → JSON → HTML context"
# pipeline. Reviews, comments, profile bios etc. should never carry markup
# through to a browser. We strip tags at input time as defense in depth —
# frontend escaping remains the primary protection.
_TAG_RE = re.compile(r"<[^>]+>")
# Script/style bodies — strip tags AND contents since the content itself
# is dangerous (event handlers, inline JS).
_SCRIPT_BLOCK_RE = re.compile(
    r"<(script|style|iframe|object|embed)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)
# Control characters except \t \n \r — these are never legitimate in text
# input and often used to evade filters.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Whitespace normalization — collapse runs of whitespace into single space
# but preserve paragraph breaks (double newlines → newline).
_PARAGRAPH_BREAK_RE = re.compile(r"\n\s*\n")
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")


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


def email_mask(email: str) -> str:
    """Mask the local part of an email address for privacy.

    Used for PII redaction in logs / public surfaces (notification
    digests, exposed audit trails) where the user's identity should
    be partially obscured but the domain stays visible. Generic — the
    framework owns it because every app eventually needs the same
    "show ``j****@example.com`` not ``john@example.com``" rendering.

    Args:
        email: Raw email address. Empty / ``None`` / strings without
            ``@`` return ``""`` so callers can chain without guards.

    Returns:
        ``"<local-mask>@<domain>"``. Local parts of length ≤ 2 are
        fully masked (no leak of first letter); longer locals show
        only the first character followed by six asterisks.

    Examples:
        >>> email_mask("john@example.com")
        'j******@example.com'
        >>> email_mask("ab@example.com")
        '**@example.com'
        >>> email_mask("")
        ''
    """
    if not email or "@" not in email:
        return ""
    local_part, domain = email.split("@", 1)
    if len(local_part) <= 2:
        masked_local = "*" * len(local_part)
    else:
        masked_local = local_part[0] + "******"
    return f"{masked_local}@{domain}"


def normalize_email(email: str) -> str:
    """Normalize an email address by lowercasing and stripping whitespace.

    Returns empty string for empty/whitespace-only input.
    """
    if not email or not email.strip():
        return ""
    return email.strip().lower()


def format_money_cents(cents: int, currency: str = "USD") -> str:
    """Format an integer cent amount as a currency string.

    Strict integer-cent variant — raises on non-int input or unsupported
    currency. Useful where you want crash-on-bad-input semantics
    (payment / billing serialization). For the lenient float-based
    renderer used in storefront / notifications / email digests, see
    :func:`cara.support.Currency.format_money`.

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


def strip_tags(text: str) -> str:
    """Strip HTML/XML tags and dangerous block contents from ``text``.

    Removes <script>/<style>/<iframe>/<object>/<embed> blocks entirely
    (tags + contents), then strips remaining tags. Safe for user-entered
    free text before it's stored or echoed back.
    """
    if not text:
        return ""
    out = _SCRIPT_BLOCK_RE.sub("", text)
    out = _TAG_RE.sub("", out)
    # Decode any HTML entities that were smuggled in, so the storage is
    # canonicalized and downstream escaping only happens once.
    out = html.unescape(out)
    return out


def sanitize_text(text: Any, max_length: int = 0) -> str:
    """Sanitize user-supplied free text for safe storage.

    Guarantees:
      - No HTML tags (content of script/style blocks dropped too).
      - No HTML entities (already unescaped).
      - No control chars other than tab/newline/CR.
      - Unicode NFKC-normalized (defeats look-alike/zero-width evasion).
      - Whitespace normalized: tabs → spaces, runs collapsed, 3+ blank
        lines clamped to 2, outer whitespace stripped.
      - Truncated to max_length (>0) if given.

    Returns empty string for empty/None input. Never returns None so
    callers can chain without guards.
    """
    if text is None:
        return ""
    s = str(text)
    if not s:
        return ""
    s = strip_tags(s)
    s = unicodedata.normalize("NFKC", s)
    s = _CONTROL_CHARS_RE.sub("", s)
    s = _MULTI_SPACE_RE.sub(" ", s)
    s = _MULTI_NEWLINE_RE.sub("\n\n", s)
    s = s.strip()
    if max_length and len(s) > max_length:
        s = s[:max_length].rstrip()
    return s


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


# ── Laravel-parity helpers ────────────────────────────────────────────────
# UUID / ULID generation, substring splits (before/after/between), and
# the predicate trio (starts_with / ends_with / contains) with iterable
# needle support so callers can pass a list of candidates instead of
# rolling their own ``any()``.

def uuid() -> str:
    """Generate a random UUID v4 string — Laravel ``Str::uuid()`` parity.

    Uses ``uuid.uuid4()`` (random) — for sortable identifiers prefer
    :func:`ulid` instead. The returned string is the canonical 36-char
    hyphenated form (``"xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx"``).
    """
    import uuid as _uuid

    return str(_uuid.uuid4())


def ulid() -> str:
    """Generate a 26-char ULID (Crockford-base32, time-sortable).

    Mirrors Laravel ``Str::ulid()``. Falls back to UUID v4 when the
    ``python-ulid`` package isn't installed — preserves uniqueness
    but loses time-sortability; callers that need ordering should
    pin ``python-ulid`` as a dependency.
    """
    try:
        import ulid as _ulid_mod  # type: ignore
    except ImportError:
        return uuid()
    return str(_ulid_mod.new())


def starts_with(haystack: str, needles) -> bool:
    """Return True if ``haystack`` starts with any of ``needles``.

    ``needles`` may be a single string or any iterable of strings —
    matches Laravel's ``Str::startsWith`` overload.
    """
    if haystack is None:
        return False
    if isinstance(needles, str):
        return haystack.startswith(needles)
    for needle in needles or ():
        if needle != "" and haystack.startswith(needle):
            return True
    return False


def ends_with(haystack: str, needles) -> bool:
    """Return True if ``haystack`` ends with any of ``needles``.

    ``needles`` may be a single string or any iterable of strings —
    matches Laravel's ``Str::endsWith`` overload.
    """
    if haystack is None:
        return False
    if isinstance(needles, str):
        return haystack.endswith(needles)
    for needle in needles or ():
        if needle != "" and haystack.endswith(needle):
            return True
    return False


def contains(haystack: str, needles, *, ignore_case: bool = False) -> bool:
    """Return True if ``haystack`` contains any of ``needles``.

    With ``ignore_case=True`` the comparison is case-insensitive on
    both sides. Mirrors Laravel's ``Str::contains`` overload.
    """
    if haystack is None:
        return False
    h = haystack.lower() if ignore_case else haystack
    if isinstance(needles, str):
        n = needles.lower() if ignore_case else needles
        return n != "" and n in h
    for needle in needles or ():
        if needle == "":
            continue
        n = needle.lower() if ignore_case else needle
        if n in h:
            return True
    return False


def before(haystack: str, needle: str) -> str:
    """Return the substring of ``haystack`` BEFORE the first ``needle``.

    Returns the full ``haystack`` when ``needle`` is empty or absent —
    matches Laravel's ``Str::before`` semantics.
    """
    if not haystack or not needle:
        return haystack or ""
    idx = haystack.find(needle)
    return haystack if idx < 0 else haystack[:idx]


def after(haystack: str, needle: str) -> str:
    """Return the substring of ``haystack`` AFTER the first ``needle``.

    Returns the full ``haystack`` when ``needle`` is empty or absent —
    matches Laravel's ``Str::after`` semantics.
    """
    if not haystack or not needle:
        return haystack or ""
    idx = haystack.find(needle)
    return haystack if idx < 0 else haystack[idx + len(needle):]


def between(haystack: str, start: str, end: str) -> str:
    """Return the substring of ``haystack`` between ``start`` and ``end``.

    Returns the full ``haystack`` when either delimiter is empty or
    not found — matches Laravel's ``Str::between`` semantics.
    """
    if not haystack or not start or not end:
        return haystack or ""
    after_start = after(haystack, start)
    if after_start == haystack:
        return haystack
    return before(after_start, end)


def mask(value: str, char: str, index: int, length: int = 0) -> str:
    """Mask ``length`` chars of ``value`` starting at ``index`` with ``char``.

    Negative ``index`` counts from the end. ``length=0`` masks to the
    end of the string. Mirrors Laravel's ``Str::mask`` for opaque
    PII redaction (credit-card middle digits, phone numbers, etc.).
    """
    if not value:
        return ""
    n = len(value)
    if index < 0:
        index = max(0, n + index)
    index = min(index, n)
    end = n if length <= 0 else min(n, index + length)
    return value[:index] + char * (end - index) + value[end:]
