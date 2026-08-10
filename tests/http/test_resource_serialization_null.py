"""Resource serialization helpers must not manufacture a known value.

§7 says unknown is ``NULL``, never ``0`` or ``""``. The framework's own
``opt_*`` helpers used to violate it in both directions, in the same file
that ``opt_float``/``opt_int`` state the rule correctly:

* ``opt_str(None)`` returned ``""`` — a client could not tell "we have no
  tracking number" from "the carrier returned a blank one".
* ``opt_bool(None)`` returned ``False`` — a nullable ``is_verified``
  rendered as "not verified" fact, defeating the fail-closed capability
  rule at the serialization layer rather than in the UI.
* ``opt_list([])`` returned ``None`` — a legitimately empty list was
  serialized as "unknown", so the API claimed ignorance when it knew.

Because every resource in every product reaches these through
``JsonResource``, a manufactured floor here is inherited everywhere.

Everything above the ``TestTheHopToTheWire`` section calls the helpers
directly, which proves what the FUNCTIONS return and nothing about what a
caller receives. That gap is not theoretical here: ``opt_float`` returned
``nan`` unchanged, every direct-call assertion about it passed, and the
value only failed once ``json_dumps`` refused it — inside
``JsonResource.to_response``, as a 500 for the entire payload. So the last
section builds a real ``JsonResource``, renders it through a real
``Response``, and reads the actual bytes. Nothing there hand-assembles the
payload dict.
"""

from __future__ import annotations

import inspect
import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cara.http import Response
from cara.http.resources import JsonResource, Serialization
from cara.http.resources.Serialization import opt_bool, opt_float, opt_list, opt_str


def _public_helpers():
    """Every public coercion helper this module owns, by introspection.

    Deliberately not a hard-coded list: a NEW helper added with a
    manufactured floor must fail this guard without anyone remembering to
    extend it.
    """
    return [
        (name, fn)
        for name, fn in inspect.getmembers(Serialization, inspect.isfunction)
        if not name.startswith("_") and fn.__module__ == Serialization.__name__
    ]


class TestUnknownStaysUnknown:
    def test_the_module_exposes_the_helpers_under_test(self) -> None:
        assert {name for name, _ in _public_helpers()} >= {
            "opt_bool",
            "opt_datetime",
            "opt_float",
            "opt_int",
            "opt_list",
            "opt_str",
        }

    @pytest.mark.parametrize(
        "name, fn", _public_helpers(), ids=[name for name, _ in _public_helpers()]
    )
    def test_every_helper_preserves_none(self, name: str, fn) -> None:
        """None in, None out — for every helper, including future ones."""
        assert fn(None) is None, f"{name}(None) manufactured a value out of unknown"

    def test_opt_str_does_not_invent_an_empty_string(self) -> None:
        assert opt_str(None) is None

    def test_opt_str_collapses_whitespace_only_to_unknown(self) -> None:
        """ "" and "   " are both indistinguishable from absent at the wire."""
        assert opt_str("   ") is None

    def test_opt_str_collapses_a_known_empty_string_to_unknown(self) -> None:
        """CHARACTERISATION, not a regression — this behaviour is unchanged
        and deliberate. Pinned because it is the one place the module
        deliberately does NOT mirror ``opt_list``, and the asymmetry reads
        like an oversight without a test naming it.

        An empty list has a cardinality, so it is knowledge. An empty string
        asserts nothing: ``""`` is how an import, a trimmed form field or a
        legacy default writes "no value", and shipping two spellings of
        absence would force every client to test for both. See
        ``opt_str``'s docstring for the argument in full.
        """
        assert opt_str("") is None

    def test_opt_bool_does_not_invent_false(self) -> None:
        """The pinned regression: ``opt_bool(None) is False`` said "we
        checked and it is not true" for a column meaning "never checked"."""
        assert opt_bool(None) is None

    def test_opt_list_keeps_a_known_empty_list(self) -> None:
        """Known-empty is knowledge, not absence."""
        assert opt_list([]) == []
        assert opt_list(()) == []


class TestOptListRefusesAStringInsteadOfShreddingIt:
    """A ``str`` is a sequence, so ``list("SKU-1")`` is five characters.

    A text column routed through ``opt_list`` by mistake reached the wire as
    ``["S", "K", "U", "-", "1"]`` with nothing raised anywhere — the one
    failure mode this helper has that nobody notices, because the output is
    a perfectly well-formed JSON array. ``opt_list(0)`` has always raised;
    a string escaped only by being iterable.
    """

    @pytest.mark.parametrize(
        "value",
        ["SKU-1", "", b"SKU-1", bytearray(b"x")],
        ids=["str", "empty", "bytes", "bytearray"],
    )
    def test_a_string_is_refused(self, value) -> None:
        with pytest.raises(TypeError) as excinfo:
            opt_list(value)

        assert "opt_list received" in str(excinfo.value)

    def test_a_non_iterable_is_still_refused(self) -> None:
        """Characterisation — the behaviour the string refusal is matched to."""
        with pytest.raises(TypeError):
            opt_list(0)

    def test_a_genuine_sequence_is_untouched(self) -> None:
        assert opt_list(["SKU-1"]) == ["SKU-1"]
        assert opt_list(("a", "b")) == ["a", "b"]


class TestNonFiniteFloatsAreUnknown:
    """``nan``/``±inf`` are not JSON, and ``opt_int`` already knew that.

    ``int(nan)`` raises ``ValueError`` straight into ``opt_int``'s
    ``except``, so ``opt_int`` has always answered ``None``. ``opt_float``
    returned the value unchanged, and the two helpers reading the same
    ``numeric`` column disagreed. See ``TestTheHopToTheWire`` for what that
    actually cost — this class only pins the decision.
    """

    @pytest.mark.parametrize(
        "value",
        [float("nan"), float("inf"), float("-inf"), Decimal("NaN"), Decimal("Infinity")],
        ids=["nan", "inf", "-inf", "decimal-nan", "decimal-inf"],
    )
    def test_a_non_finite_value_is_null(self, value) -> None:
        assert opt_float(value) is None

    def test_finite_values_are_untouched(self) -> None:
        assert opt_float(Decimal("19.99")) == 19.99
        assert opt_float(0) == 0.0
        assert opt_float("-3.5") == -3.5


class TestExplicitFloorsStillWork:
    """A caller that genuinely wants a floor asks for it at the call site,
    where the meaning of the field is known."""

    def test_opt_str_honours_an_explicit_default(self) -> None:
        assert opt_str(None, "") == ""
        assert opt_str(None, "pending") == "pending"
        assert opt_str("   ", "pending") == "pending"

    def test_opt_bool_honours_an_explicit_default(self) -> None:
        assert opt_bool(None, False) is False
        assert opt_bool(None, True) is True

    def test_known_values_are_unchanged(self) -> None:
        assert opt_str("  hi  ") == "hi"
        assert opt_bool(0) is False
        assert opt_bool(1) is True
        assert opt_list([1, 2]) == [1, 2]


# ── The hop to the wire ───────────────────────────────────────────────
#
# Everything above asserts on a function's return value. What a client
# actually consumes is BYTES, and the two are not the same claim: the
# helpers reach the wire through ``JsonResource.to_array`` →
# ``resolve()`` → ``Response.json`` → ``json_dumps``, and each of those
# hops can re-coerce, drop or refuse a value. ``opt_float`` proved it —
# every direct-call assertion about ``nan`` passed and the response still
# died. So build a real resource, render it through a real ``Response``,
# and parse the bytes it produced. Nothing below writes the payload dict
# by hand.


class _RowResource(JsonResource):
    """A resource shaped like every product resource: nullable columns
    routed through the helpers, nothing else."""

    def to_array(self, request=None) -> dict:
        return {
            "tracking_number": self.opt_str(self.resource.get("tracking")),
            "is_verified": self.opt_bool(self.resource.get("verified")),
            "variants": opt_list(self.resource.get("variants")),
            "margin": self.opt_float(self.resource.get("margin")),
        }


def _wire(row: dict) -> bytes:
    """Render through the real ``Response`` and return what it would send."""
    return _RowResource(row).to_response(Response(MagicMock())).content


class TestTheHopToTheWire:
    def test_unknown_columns_arrive_as_json_null(self) -> None:
        """The regression, as a client sees it. Before the fix these bytes
        read ``"tracking_number": ""`` and ``"is_verified": false`` — two
        manufactured facts — while ``"variants": null`` claimed ignorance of
        a list the server had just counted."""
        raw = _wire({"tracking": None, "verified": None, "variants": [], "margin": None})

        assert json.loads(raw) == {
            "data": {
                "tracking_number": None,
                "is_verified": None,
                "variants": [],
                "margin": None,
            }
        }

    def test_the_key_is_present_and_null_not_omitted(self) -> None:
        """``null`` and "key absent" are different answers to a client that
        distinguishes them; the serializer must not drop the field."""
        payload = json.loads(
            _wire({"tracking": None, "verified": None, "variants": None, "margin": None})
        )["data"]

        assert "tracking_number" in payload
        assert "is_verified" in payload
        assert payload["variants"] is None

    def test_known_values_survive_the_hop(self) -> None:
        raw = _wire(
            {
                "tracking": "  1Z999  ",
                "verified": 1,
                "variants": ["red", "blue"],
                "margin": Decimal("12.5"),
            }
        )

        assert json.loads(raw)["data"] == {
            "tracking_number": "1Z999",
            "is_verified": True,
            "variants": ["red", "blue"],
            "margin": 12.5,
        }

    @pytest.mark.parametrize(
        "margin",
        [float("nan"), float("inf"), Decimal("NaN"), Decimal("Infinity")],
        ids=["nan", "inf", "decimal-nan", "decimal-inf"],
    )
    def test_a_non_finite_number_does_not_destroy_the_response(self, margin) -> None:
        """The reproduction that only exists at this boundary.

        Pre-fix, ``to_response`` raised ``ValueError: Out of range float
        values are not JSON compliant`` — ``json_dumps`` runs
        ``allow_nan=False`` because the ``NaN``/``Infinity`` literals it
        would otherwise emit are rejected by ``JSON.parse``. One
        unpriceable row and the endpoint answered 500 for the whole
        payload, every other field included. PostgreSQL ``numeric`` stores
        ``'NaN'`` literally, so ``Decimal("NaN")`` comes off a column.
        """
        raw = _wire(
            {"tracking": "1Z999", "verified": True, "variants": [], "margin": margin}
        )

        payload = json.loads(raw)["data"]
        assert payload["margin"] is None
        # The rest of the row must survive the unpriceable field.
        assert payload["tracking_number"] == "1Z999"
        assert payload["is_verified"] is True

    def test_the_bytes_are_strict_json(self) -> None:
        """``json.loads`` accepts the ``NaN`` literal; a browser does not.
        Parse strictly so a regression cannot pass here and fail in the
        client."""
        raw = _wire(
            {"tracking": None, "verified": None, "variants": [], "margin": float("nan")}
        )

        assert b"NaN" not in raw
        assert b"Infinity" not in raw
        json.loads(raw.decode(), parse_constant=_reject_constant)


def _reject_constant(literal: str):  # pragma: no cover - only on regression
    raise AssertionError(f"non-JSON literal {literal!r} reached the wire")
