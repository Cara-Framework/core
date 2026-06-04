"""
Email Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a valid email address.
"""

from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules import BaseRule


class EmailRule(BaseRule):
    """Validates that a string is a well‐formed email address.

    Refinements over the original greedy pattern:

    * **No consecutive dots** in either local-part or domain. RFC
      5321 forbids ``..``. The pre-fix ``[\\w\\.\\-]+`` swallowed any
      number of dots, so ``user..name@example.com`` and
      ``user@example..com`` slipped through and only bounced at the
      SMTP submission layer.

    * **Hyphen allowed in the TLD group** so punycode IDN TLDs like
      ``xn--p1ai`` (Russian .рф) validate. Pre-fix the TLD pattern
      was ``\\w+`` — ``\\w`` excludes ``-``, so every IDN domain on
      the SMTP transit form was rejected, locking out
      Cyrillic / Devanagari / Hebrew / etc.-script address users.

    * **No trailing newline.** Python's default-mode regex ``$``
      matches end-of-string OR just before a final ``\\n`` —
      ``user@example.com\\n`` would otherwise pass and feed into
      downstream SMTP / log / webhook code as a header-injection
      precursor. ``fullmatch`` + an explicit ``"\\n" in value``
      reject closes the gap. The EmailChannel has its own
      ``_strip_header_crlf`` defence, but the input boundary is
      the right place to defend other call sites that don't.

    * **RFC 5321 length caps.** Local-part max 64, total max 254.
      The pre-fix regex had no upper bound, so a 100k-character
      "valid" email passed — bloated user rows, wasted CPU on every
      downstream regex, and was a sign of malformed input every
      time it appeared. The caps mirror what ``email.utils`` /
      Django / Laravel enforce.
    """

    # Local-part: one or more ``label`` groups separated by single dots,
    # where a label is at least one of ``[\w\-\+]`` (no dot inside a
    # label → no ``..`` run).
    # Domain: same shape, then a literal ``.``, then a TLD that allows
    # ``[\w\-]+`` so punycode IDN TLDs (``xn--p1ai``) match.
    _LOCAL = r"[\w\-\+]+(?:\.[\w\-\+]+)*"
    _DOMAIN = r"[\w\-]+(?:\.[\w\-]+)*"
    _TLD = r"[\w\-]+"
    _pattern = re.compile(rf"^{_LOCAL}@{_DOMAIN}\.{_TLD}$")

    # RFC 5321 §4.5.3.1 — local-part 64 octets, full path 254 octets
    # (the standard quotes 256 minus the ``<>`` mailbox brackets).
    MAX_LOCAL_PART_LENGTH = 64
    MAX_TOTAL_LENGTH = 254

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        """Check if value is a valid email format."""
        if value is None or not isinstance(value, str):
            return False
        # Cheap rejects before the regex: any control / line-ending
        # character is grounds for refusal regardless of where it
        # sits. Catches ``user@example.com\n`` (the default-mode
        # ``$`` quirk) plus any other ``\r``/``\x00`` slips.
        if "\n" in value or "\r" in value or "\x00" in value:
            return False
        if len(value) > self.MAX_TOTAL_LENGTH:
            return False
        # Split once — ``rsplit`` so a quoted local-part containing
        # ``@`` (rare but legal) still measures the *envelope*
        # local-part correctly even though our regex doesn't accept
        # quoted forms today.
        if "@" not in value:
            return False
        local_part, _ = value.rsplit("@", 1)
        if len(local_part) > self.MAX_LOCAL_PART_LENGTH:
            return False
        # ``fullmatch`` instead of ``match`` defends against the
        # default-mode ``$`` quirk in one place — the input was
        # already screened for control chars above, but using
        # ``fullmatch`` removes the implicit "trailing newline is
        # fine" rule the regex engine carries.
        return bool(self._pattern.fullmatch(value))

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        """Return default email validation message."""
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be a valid email address."
