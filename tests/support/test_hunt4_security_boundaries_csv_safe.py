"""LF belongs with TAB and CR among the leading characters ``CsvSafe`` defuses.

``CsvSafe`` neutralizes a leading TAB and CR "because spreadsheet importers may
discard them first", and OWASP's CSV-injection list names Line Feed beside
them. A cell opening with a newline reaches the same importers untouched.
"""

from __future__ import annotations

from cara.support.CsvSafe import defuse_csv_cell, restore_csv_cell


def test_a_cell_opening_with_a_line_feed_is_defused():
    payload = '\n=HYPERLINK("https://evil.example","x")'

    assert defuse_csv_cell(payload) == f"'{payload}"


def test_the_defused_line_feed_round_trips_on_import():
    payload = "\n=1+1"

    assert restore_csv_cell(defuse_csv_cell(payload)) == payload
