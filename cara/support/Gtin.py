"""GTIN / UPC / EAN / ISBN normalization and validation.

The same barcode reaches an application in different shapes: ``190198001443``
from one source, ``0190198001443`` from another, ``00190198001443`` from a
third. They are ONE GTIN-14 written with different zero-padding. This module
folds every valid variant onto the 14-digit canonical form with a verified
Mod-10 check digit, so equality on the normalized value is a safe identity
test regardless of which source produced it.

Two rules keep that canonical form trustworthy:

* **Length floor.** GTIN-8 is the shortest form the standard defines, so an
  input carrying fewer than 8 digits is not a barcode and normalizes to
  ``None``. Left-padding is only ever RECOGNISED, never invented — a 2-digit
  number is not "a GTIN-8 with six leading zeros", and pretending otherwise
  mints identity keys out of junk numeric fields.
* **Placeholder rejection.** All-zero, all-same-digit and
  run-of-consecutive-digits values satisfy the Mod-10 check by construction
  and are exactly what sources emit when they have no real identifier.
  Treating them as identities collapses unrelated records onto one key, so
  they normalize to ``None``.

Functions:
    gtin_check_digit(body) -> Mod-10 check digit for a body string
    is_valid_gtin(raw)     -> True iff raw is a valid GTIN-8/12/13/14
    normalize_gtin(raw)    -> 14-digit string, or None when not a valid GTIN
    normalize_isbn(raw)    -> canonical ISBN-13 string, or None
    coerce_to_gtin_14(...) -> best valid GTIN-14 across several input fields
"""

from __future__ import annotations

import re

_DIGIT_ONLY = re.compile(r"\D+")

#: Shortest form the GTIN standard defines. An input with fewer raw digits
#: than this is not a barcode — see the module docstring's length floor.
MIN_GTIN_DIGITS = 8

#: The canonical GTIN lengths, ascending. Normalization picks the shortest
#: one that fits, then zero-pads the result to 14.
_GTIN_LENGTHS = (8, 12, 13, 14)

_KNOWN_BAD_GTINS: frozenset[str] = frozenset(
    {
        "00000000",
        "000000000000",
        "0000000000000",
        "00000000000000",
        "11111111111111",
        "22222222222222",
        "33333333333333",
        "44444444444444",
        "55555555555555",
        "66666666666666",
        "77777777777777",
        "88888888888888",
        "99999999999999",
        "11111111",
        "22222222",
        "33333333",
        "44444444",
        "55555555",
        "66666666",
        "77777777",
        "88888888",
        "99999999",
        "0123456789012",
        "00123456789012",
        "1234567890123",
        "01234567890123",
        "12345678901231",
        "9876543210123",
        "09876543210123",
        "0987654321098",
        "00987654321098",
        "12345678",
        "00000012345678",
        "00000099999999",
        "01010101010101",
        "10101010101010",
        "12121212121212",
        "13131313131313",
    }
)

_ALL_SAME_DIGIT_FORMS: frozenset[str] = frozenset(
    digit * length for digit in "0123456789" for length in _GTIN_LENGTHS
)


def _strip(raw: str | None) -> str:
    if raw is None:
        return ""
    return _DIGIT_ONLY.sub("", str(raw))


def _is_placeholder_pattern(s: str) -> bool:
    """True iff ``s`` is a placeholder rather than a real identifier."""
    if not s or not s.isdigit():
        return False
    if s in _KNOWN_BAD_GTINS or s in _ALL_SAME_DIGIT_FORMS or len(set(s)) == 1:
        return True
    stripped = s.lstrip("0")
    if len(stripped) >= 4 and all(
        (int(stripped[i + 1]) - int(stripped[i])) % 10 == 1
        for i in range(len(stripped) - 1)
    ):
        return True
    return len(stripped) >= 4 and all(
        (int(stripped[i]) - int(stripped[i + 1])) % 10 == 1
        for i in range(len(stripped) - 1)
    )


def gtin_check_digit(body: str) -> int | None:
    """Mod-10 check digit for a 7/11/12/13-digit body.

    Returns:
        Integer check digit 0-9, or None if body isn't valid digits.
    """
    if not body or not body.isdigit():
        return None
    total = 0
    for i, ch in enumerate(reversed(body)):
        d = int(ch)
        total += d * (3 if i % 2 == 0 else 1)
    return (10 - (total % 10)) % 10


def is_valid_gtin(raw: str | None) -> bool:
    """True iff ``raw`` (after stripping non-digits) is a valid GTIN-8/12/13/14."""
    s = _strip(raw)
    if len(s) not in _GTIN_LENGTHS:
        return False
    body, check = s[:-1], int(s[-1])
    expected = gtin_check_digit(body)
    return expected is not None and expected == check


def normalize_gtin(raw: str | None) -> str | None:
    """Normalize any GTIN-family identifier to a 14-digit string.

    Accepts UPC-A (12), EAN-13 (13), GTIN-8, GTIN-14 and their
    zero-padded variants — the Mod-10 check is invariant under left
    zero-padding, so every padding of one barcode verifies alike and
    collapses onto the same GTIN-14.

    Returns ``None`` when the input carries fewer than
    :data:`MIN_GTIN_DIGITS` digits, when no canonical length verifies its
    check digit, or when the value is a placeholder pattern.
    """
    s = _strip(raw)
    if len(s) < MIN_GTIN_DIGITS:
        return None

    stripped = s.lstrip("0")
    if not stripped:
        return None

    for length in _GTIN_LENGTHS:
        if len(stripped) <= length:
            candidate = stripped.zfill(length)
            if is_valid_gtin(candidate):
                normalized = candidate.zfill(14)
                if _is_placeholder_pattern(normalized) or _is_placeholder_pattern(
                    candidate
                ):
                    return None
                return normalized

    return None


def normalize_isbn(raw: str | None) -> str | None:
    """Normalize ISBN-10 or ISBN-13 to ISBN-13.

    ISBN-13 is itself a GTIN-13 starting with 978 or 979, so we route
    through the same check-digit rules once a 10-digit input is upgraded.

    Returns:
        13-digit ISBN-13 string, or None if invalid.
    """
    s = _strip(raw)
    raw_str = "" if raw is None else str(raw).upper()
    if not s and "X" not in raw_str:
        return None

    last_char = (raw_str.replace("-", "").replace(" ", "") or " ")[-1]
    is_isbn10_shape = (len(s) == 9 and last_char == "X") or (
        len(s) == 10 and last_char.isdigit()
    )
    if is_isbn10_shape:
        body = f"978{s[:9]}"[:12]
        check = gtin_check_digit(body)
        if check is None:
            return None
        candidate = f"{body}{check}"
        if is_valid_gtin(candidate):
            return candidate

    if len(s) == 13 and is_valid_gtin(s):
        return s

    if len(s) == 14 and s.startswith("0") and is_valid_gtin(s[1:]):
        return s[1:]

    return None


def coerce_to_gtin_14(
    *,
    gtin: str | None = None,
    upc: str | None = None,
    ean: str | None = None,
    isbn: str | None = None,
) -> str | None:
    """Pick the best valid GTIN-family input and return its GTIN-14 form.

    Prefers the BASE CONSUMER UNIT (indicator digit ``0``) — the barcode
    printed on the single retail item rather than on a case or pallet, and
    therefore the value every source is most likely to publish for the same
    item, which makes it the reliable cross-source match anchor.

    An ISBN is folded into the SAME GTIN-14 space because an ISBN-13 *is* a
    GTIN-13 (978/979 prefix). :func:`normalize_isbn` first upgrades an
    ISBN-10 or a hyphenated/spaced value to the canonical ISBN-13, so the
    same book published with an ISBN-10 in one record and an ISBN-13 (or a
    bare EAN) in another collapses to one key. An explicit barcode still
    wins — ISBN is considered last, and the indicator-``0`` preference keeps
    the real base-unit anchor.
    """
    candidates: list[str] = []
    for raw in (gtin, ean, upc):
        normalized = normalize_gtin(raw)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    if isbn is not None:
        isbn13 = normalize_isbn(isbn)
        if isbn13:
            normalized = normalize_gtin(isbn13)
            if normalized and normalized not in candidates:
                candidates.append(normalized)
    if not candidates:
        return None
    for candidate in candidates:
        if candidate[0] == "0":
            return candidate
    return candidates[0]


__all__ = [
    "MIN_GTIN_DIGITS",
    "coerce_to_gtin_14",
    "gtin_check_digit",
    "is_valid_gtin",
    "normalize_gtin",
    "normalize_isbn",
]
