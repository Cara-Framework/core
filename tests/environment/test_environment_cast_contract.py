"""``cara.environment.env`` typed-cast and auto-coercion contract.

``env(name, default, cast)`` has TWO distinct modes and they behave
differently on purpose:

* ``cast`` is a **type object** (``int`` / ``float`` / ``bool``) —
  strict coercion. A value that cannot be cast raises ``ValueError``
  naming the variable, so a misconfigured deploy dies at boot with an
  actionable message instead of handing a raw string to redis-py, a
  port argument or a concurrency limit far from the config site.
* ``cast`` is the historical **boolean flag** (``True`` by default) —
  best-effort heuristic coercion. It only ever produces ``int``,
  ``bool`` or the raw ``str``: it NEVER produces a ``float``, and an
  integer-looking value wins over a boolean-looking one.

Synkronus configuration passes a type at ~150 call sites. If the typed
branch regressed to "truthy flag", every one of those silently reverts
to the heuristic and starts handing back strings for TTLs, ports,
pool sizes and feature toggles — no exception, just wrong types
downstream. These pins hold both modes, including the heuristic's
documented limits.

The framework owns this behaviour, so the pin lives in the framework's
own suite rather than being duplicated per product.
"""

from __future__ import annotations

import pytest

from cara.environment import env

_KEYS = ("CARA_CAST_FLOAT", "CARA_CAST_INT", "CARA_CAST_BOOL", "CARA_CAST_RAW")


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch) -> None:
    """Remove every probe variable from ``os.environ`` before each test."""
    for key in _KEYS:
        monkeypatch.delenv(key, raising=False)


# ── Typed cast: float ─────────────────────────────────────────────────


class TestFloatCast:
    """``env("X", 5.0, float)`` must always return a real ``float``."""

    def test_unset_returns_the_typed_default(self) -> None:
        """An unset variable falls back to the already-typed default."""
        result = env("CARA_CAST_FLOAT", 5.0, float)
        assert result == 5.0
        assert isinstance(result, float)

    def test_decimal_string_becomes_a_float(self, monkeypatch) -> None:
        """The heuristic path cannot do this — only the typed path can."""
        monkeypatch.setenv("CARA_CAST_FLOAT", "5.5")
        result = env("CARA_CAST_FLOAT", 5.0, float)
        assert result == 5.5
        assert isinstance(result, float)

    def test_integer_string_becomes_a_float(self, monkeypatch) -> None:
        """``"10"`` under ``float`` is 10.0, not the int 10."""
        monkeypatch.setenv("CARA_CAST_FLOAT", "10")
        result = env("CARA_CAST_FLOAT", 5.0, float)
        assert result == 10.0
        assert isinstance(result, float)

    def test_scientific_notation_is_accepted(self, monkeypatch) -> None:
        """``float()`` semantics, not a hand-rolled numeric parser."""
        monkeypatch.setenv("CARA_CAST_FLOAT", "5.5e2")
        assert env("CARA_CAST_FLOAT", 5.0, float) == 550.0

    def test_surrounding_whitespace_is_stripped(self, monkeypatch) -> None:
        """A trailing newline from a secrets mount must not fail the cast."""
        monkeypatch.setenv("CARA_CAST_FLOAT", "  2.5\n")
        assert env("CARA_CAST_FLOAT", 5.0, float) == 2.5

    def test_unparseable_value_raises_and_names_the_variable(
        self, monkeypatch
    ) -> None:
        """The error must be diagnosable without a stack archaeology dig."""
        monkeypatch.setenv("CARA_CAST_FLOAT", "not-a-number")
        with pytest.raises(ValueError) as raised:
            env("CARA_CAST_FLOAT", 5.0, float)
        assert "CARA_CAST_FLOAT" in str(raised.value)

    def test_empty_string_falls_back_to_the_default(self, monkeypatch) -> None:
        """``X=`` in a .env file means "unset", not "zero"."""
        monkeypatch.setenv("CARA_CAST_FLOAT", "")
        assert env("CARA_CAST_FLOAT", 5.0, float) == 5.0


# ── Typed cast: int ───────────────────────────────────────────────────


class TestIntCast:
    """``env("X", 1024, int)`` must always return a real ``int``."""

    def test_unset_returns_the_typed_default(self) -> None:
        """Concurrency limits and pool sizes rely on this default path."""
        result = env("CARA_CAST_INT", 1024, int)
        assert result == 1024
        assert isinstance(result, int)

    def test_integer_string_is_converted(self, monkeypatch) -> None:
        """And it is a plain ``int``, never a ``bool``."""
        monkeypatch.setenv("CARA_CAST_INT", "2048")
        result = env("CARA_CAST_INT", 1024, int)
        assert result == 2048
        assert isinstance(result, int) and not isinstance(result, bool)

    def test_negative_integer_string_is_converted(self, monkeypatch) -> None:
        """Negative offsets/backoffs are legitimate configuration."""
        monkeypatch.setenv("CARA_CAST_INT", "-5")
        assert env("CARA_CAST_INT", 0, int) == -5

    def test_typo_raises_rather_than_leaking_a_string(self, monkeypatch) -> None:
        """``"2048b"`` must fail at boot, not at first arithmetic use."""
        monkeypatch.setenv("CARA_CAST_INT", "2048b")
        with pytest.raises(ValueError) as raised:
            env("CARA_CAST_INT", 1024, int)
        assert "CARA_CAST_INT" in str(raised.value)

    def test_float_string_under_int_raises(self, monkeypatch) -> None:
        """``int("1.5")`` is an error — silently truncating would be worse."""
        monkeypatch.setenv("CARA_CAST_INT", "1.5")
        with pytest.raises(ValueError):
            env("CARA_CAST_INT", 1, int)

    def test_empty_string_with_a_mistyped_default_yields_the_zero_value(
        self, monkeypatch
    ) -> None:
        """Documented limit of the typed path.

        On an empty value the default is used ONLY when it already has
        the target type; otherwise the cast falls back to ``int()`` —
        i.e. ``0``. A config site that writes ``env("X", "", int)``
        therefore gets ``0``, not ``""``. Pinned so the fallback stays
        a typed value rather than degrading to the raw default.
        """
        monkeypatch.setenv("CARA_CAST_INT", "")
        assert env("CARA_CAST_INT", "", int) == 0

    def test_none_default_survives_the_typed_path(self) -> None:
        """``env("X", None, int)`` stays ``None`` — an optional setting."""
        assert env("CARA_CAST_INT", None, int) is None


# ── Typed cast: bool ──────────────────────────────────────────────────


class TestBoolCast:
    """``env("X", True, bool)`` must always return a real ``bool``."""

    def test_unset_returns_the_typed_default(self) -> None:
        """Both polarities, and identity — not merely truthiness."""
        assert env("CARA_CAST_BOOL", True, bool) is True
        assert env("CARA_CAST_BOOL", False, bool) is False

    @pytest.mark.parametrize(
        "value",
        ["true", "True", "TRUE", "yes", "on", "1", "  true  "],
    )
    def test_truthy_keywords(self, monkeypatch, value: str) -> None:
        """Every spelling docker-compose / k8s / .env loaders accept."""
        monkeypatch.setenv("CARA_CAST_BOOL", value)
        assert env("CARA_CAST_BOOL", False, bool) is True

    @pytest.mark.parametrize(
        "value",
        ["false", "False", "FALSE", "no", "off", "0", "  false  "],
    )
    def test_falsy_keywords(self, monkeypatch, value: str) -> None:
        """``"0"`` in particular must be ``False``, not the int ``0``."""
        monkeypatch.setenv("CARA_CAST_BOOL", value)
        result = env("CARA_CAST_BOOL", True, bool)
        assert result is False
        assert isinstance(result, bool)

    def test_unknown_word_raises_rather_than_staying_truthy(
        self, monkeypatch
    ) -> None:
        """``"disabled"`` as a raw string is truthy — the worst outcome."""
        monkeypatch.setenv("CARA_CAST_BOOL", "disabled")
        with pytest.raises(ValueError) as raised:
            env("CARA_CAST_BOOL", True, bool)
        assert "CARA_CAST_BOOL" in str(raised.value)

    def test_empty_string_falls_back_to_the_bool_default(
        self, monkeypatch
    ) -> None:
        """``X=`` means "unset" on the bool path too."""
        monkeypatch.setenv("CARA_CAST_BOOL", "")
        assert env("CARA_CAST_BOOL", True, bool) is True

    def test_none_default_survives_the_bool_path(self) -> None:
        """A tri-state toggle stays ``None`` when unset and undefaulted."""
        assert env("CARA_CAST_BOOL", None, bool) is None


# ── Heuristic path (``cast`` as the historical boolean flag) ──────────


class TestAutoCoercionLimits:
    """The untyped path is a HEURISTIC, and its limits are the reason
    the typed form exists. Pinning them keeps the two modes honestly
    distinguishable instead of one silently absorbing the other."""

    def test_integer_looking_values_become_ints(self, monkeypatch) -> None:
        """Including the signed forms the old ``isnumeric()`` test missed."""
        for raw, expected in (("42", 42), ("-5", -5), ("+7", 7)):
            monkeypatch.setenv("CARA_CAST_RAW", raw)
            assert env("CARA_CAST_RAW", 0) == expected

    def test_decimal_values_stay_strings(self, monkeypatch) -> None:
        """The heuristic NEVER produces a float.

        This is exactly the gap the third-argument type closes: a
        ``REDIS_SOCKET_TIMEOUT=5.5`` read without a type comes back as
        the string ``"5.5"``.
        """
        monkeypatch.setenv("CARA_CAST_RAW", "5.5")
        assert env("CARA_CAST_RAW", 1.0) == "5.5"

    def test_non_decimal_numeric_characters_do_not_crash(
        self, monkeypatch
    ) -> None:
        """``str.isnumeric()`` is True for ``"²"``/``"½"`` but ``int()``
        rejects them — the explicit ``[+-]?\\d+`` match keeps boot alive
        by leaving them as the raw string."""
        for raw in ("²", "½"):
            monkeypatch.setenv("CARA_CAST_RAW", raw)
            assert env("CARA_CAST_RAW", 0) == raw

    @pytest.mark.parametrize(
        "raw",
        ["true", "True", "TRUE", "yes", "on", "  true  "],
    )
    def test_boolean_words_become_true(self, monkeypatch, raw: str) -> None:
        """Case and padding insensitive — the asymmetry that made
        ``X=true`` and ``X=TRUE`` behave differently is gone."""
        monkeypatch.setenv("CARA_CAST_RAW", raw)
        assert env("CARA_CAST_RAW", False) is True

    @pytest.mark.parametrize("raw", ["false", "False", "FALSE", "no", "off"])
    def test_boolean_words_become_false(self, monkeypatch, raw: str) -> None:
        """The falsy vocabulary mirrors the truthy one exactly."""
        monkeypatch.setenv("CARA_CAST_RAW", raw)
        assert env("CARA_CAST_RAW", True) is False

    def test_digit_toggles_are_ints_not_bools_without_a_type(
        self, monkeypatch
    ) -> None:
        """The integer branch runs BEFORE the boolean branch.

        So ``FLAG=0`` read without a type is the int ``0`` (falsy, but
        not ``False``) and ``FLAG=1`` is the int ``1``. A caller that
        needs a real bool must pass ``bool`` — the difference matters
        for ``is True`` / ``is False`` comparisons and for anything
        that serializes the value back out.
        """
        monkeypatch.setenv("CARA_CAST_RAW", "0")
        zero = env("CARA_CAST_RAW", False)
        assert zero == 0
        assert zero is not False

        monkeypatch.setenv("CARA_CAST_RAW", "1")
        one = env("CARA_CAST_RAW", True)
        assert one == 1
        assert one is not True

    def test_unrecognised_word_is_returned_verbatim(self, monkeypatch) -> None:
        """No exception on the heuristic path — only the typed path is strict."""
        monkeypatch.setenv("CARA_CAST_RAW", "disabled")
        assert env("CARA_CAST_RAW", True) == "disabled"

    def test_empty_string_falls_back_to_the_default(self, monkeypatch) -> None:
        """Same "``X=`` means unset" rule as the typed path."""
        monkeypatch.setenv("CARA_CAST_RAW", "   ")
        assert env("CARA_CAST_RAW", "fallback") == "fallback"

    def test_non_string_default_is_returned_untouched(self) -> None:
        """An unset variable with a typed default never enters coercion."""
        assert env("CARA_CAST_RAW", 5) == 5


class TestLegacyCastFlag:
    """``cast=True``/``cast=False`` must keep working — a bool is not a
    ``type``, so the typed branch must not swallow it."""

    def test_cast_true_keeps_the_numeric_heuristic(self, monkeypatch) -> None:
        """Explicit ``True`` behaves exactly like the implicit default."""
        monkeypatch.setenv("CARA_CAST_RAW", "42")
        assert env("CARA_CAST_RAW", 0, True) == 42

    def test_cast_false_returns_the_raw_string(self, monkeypatch) -> None:
        """The escape hatch for values that must not be interpreted."""
        monkeypatch.setenv("CARA_CAST_RAW", "42")
        assert env("CARA_CAST_RAW", 0, False) == "42"

    def test_cast_false_leaves_boolean_words_alone(self, monkeypatch) -> None:
        """``cast=False`` is total — no branch of the heuristic runs."""
        monkeypatch.setenv("CARA_CAST_RAW", "true")
        assert env("CARA_CAST_RAW", "", False) == "true"

    def test_the_implicit_default_is_cast_true(self, monkeypatch) -> None:
        """Omitting the third argument must not change behaviour."""
        monkeypatch.setenv("CARA_CAST_RAW", "true")
        assert env("CARA_CAST_RAW", False) is True
