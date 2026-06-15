"""
URL Validation Rule for the Cara framework.

This module provides a validation rule that checks if a value is a valid URL.
"""

from __future__ import annotations

import re
from typing import Any

from cara.validation import MessageFormatter
from cara.validation.rules.BaseRule import BaseRule


class URLRule(BaseRule):
    """Validates that a string is a well-formed URL.

    Two refinements over the original pattern:

    * **Punycode IDN TLDs accepted.** Pre-fix the TLD group was
      ``[A-Z]{2,6}`` — no hyphen, max length 6, so every punycode
      TLD (``xn--p1ai`` for ``.рф``, ``xn--80akhbyknj4f`` for ``.бг``,
      ``xn--fiqs8s`` for ``.中国``) failed validation. The TLD now
      uses ``[A-Z0-9-]{2,63}`` matching the domain-label rules.

    * **No trailing newline.** Python's default-mode regex ``$``
      matches end-of-string OR just before a final ``\\n``. A URL
      with a trailing ``\\n`` (a CRLF-injection precursor for any
      downstream ``Location:`` / ``Link:`` header) would otherwise
      slip past. ``fullmatch`` + an explicit control-character
      reject closes the gap.
    """

    _pattern = re.compile(
        r"^https?://"
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z0-9-]{2,63}\.?|"
        r"localhost|"
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        r"(?::\d+)?"
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )

    def validate(self, field: str, value: Any, params: dict[str, Any]) -> bool:
        if value is None or not isinstance(value, str):
            return False
        # Control characters — newline, carriage return, null — never
        # belong in a URL and act as CRLF-injection precursors if the
        # value flows into HTTP headers downstream. Reject at the
        # input boundary so every caller benefits.
        if "\n" in value or "\r" in value or "\x00" in value:
            return False
        return bool(self._pattern.fullmatch(value))

    def default_message(self, field: str, params: dict[str, Any]) -> str:
        attribute = MessageFormatter.format_attribute_name(field)
        return f"The {attribute.lower()} field must be a valid URL."
