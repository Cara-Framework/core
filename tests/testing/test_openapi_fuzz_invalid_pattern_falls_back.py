"""A schema ``pattern`` Python cannot compile falls back to free text.

The live defect: ``st.from_regex`` is lazy — a JS-only pattern (``\\p{L}``)
raises only when an example is DRAWN, so the broad ``except`` wrapped around
building the strategy never fired, and the operation's whole fuzz run died
with ``re.error`` instead of falling back to bounded free text.
"""

from __future__ import annotations

import re

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cara.testing.fuzz import OpenApiRequests

FAST = settings(
    max_examples=30,
    database=None,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
REQUESTS = OpenApiRequests({"openapi": "3.1.0", "paths": {}})
JS_ONLY = {"type": "string", "pattern": r"^\p{L}+$", "maxLength": 6}
PYTHON = {"type": "string", "pattern": r"[A-Z]{3}-\d{2}"}


@FAST
@given(st.data())
def test_a_js_only_pattern_draws_bounded_free_text(data):
    value = data.draw(REQUESTS.valid_value(JS_ONLY, "title"))

    assert isinstance(value, str)
    assert len(value) <= 6


@FAST
@given(st.data())
def test_a_python_pattern_still_drives_generation(data):
    value = data.draw(REQUESTS.valid_value(PYTHON, "code"))

    assert re.fullmatch(PYTHON["pattern"], value)
