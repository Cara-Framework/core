"""``TimeStampsScope`` must stamp the real instant, whatever APP_TIMEZONE is.

The write path used to double-convert. ``model.get_new_date()`` returns
``pendulum.now("UTC")``, but the scope immediately called
``.to_datetime_string()`` on it, which DROPS the offset. ``DateTimeCast.set``
then saw a naive string and — per its documented contract for
product-supplied naive input — re-read it as APP_TIMEZONE. On a product
configured ``Europe/Madrid`` every ``created_at`` was therefore stored two
hours in the past: an instant that never happened, with nothing raised.

``updated_at`` was worse. It was derived from the ALREADY-CAST ``created_at``
value, so the naive UTC string went through the same cast a SECOND time and
landed four hours out.

Nothing pinned this before: no test referenced ``TimeStampsScope`` at all, and
both shipping products hard-default APP_TIMEZONE to "UTC", which makes the
corruption invisible in their suites.
"""

from __future__ import annotations

import pendulum
import pytest

from cara.configuration import Configuration
from cara.eloquent.concerns.HasAttributes import HasAttributes
from cara.eloquent.scopes.TimeStampsScope import TimeStampsScope

# Two hours east of UTC in August — a naive UTC string re-read here moves
# backwards by exactly that much, which is what makes the drift measurable.
_APP_TIMEZONE = "Europe/Madrid"
_NOW = pendulum.datetime(2026, 8, 8, 20, 29, 13, tz="UTC")
_TRUE_UTC = "2026-08-08 20:29:13"


@pytest.fixture
def madrid_app_timezone():
    """Run the body with a non-UTC APP_TIMEZONE, then put it back."""
    if not Configuration._instance:
        Configuration.empty()

    configuration = Configuration._instance
    had = configuration.has("app.timezone")
    previous = configuration.get("app.timezone")
    configuration.set("app.timezone", _APP_TIMEZONE)
    try:
        yield
    finally:
        if had:
            configuration.set("app.timezone", previous)
        else:
            configuration._config.pop("app.timezone", None)


class _StampedModel:
    """Minimal stand-in carrying the real cast dispatch, not a fake one."""

    __timestamps__ = True
    date_created_at = "created_at"
    date_updated_at = "updated_at"

    def __init__(self, casts):
        self.__casts__ = casts

    def get_new_date(self):
        return _NOW

    _set_cast_attribute = HasAttributes._set_cast_attribute


class _Builder:
    def __init__(self, model):
        self._model = model
        self._creates = {}
        self._updates = ()


def test_created_at_records_the_true_instant_under_a_non_utc_app_timezone(
    madrid_app_timezone,
):
    model = _StampedModel({"created_at": "datetime", "updated_at": "datetime"})

    created, _ = TimeStampsScope()._timestamp_values(model)

    # Pre-fix: "2026-08-08 18:29:13" — UTC re-read as Europe/Madrid.
    assert created == _TRUE_UTC


def test_updated_at_is_not_cast_a_second_time(madrid_app_timezone):
    model = _StampedModel({"created_at": "datetime", "updated_at": "datetime"})

    created, updated = TimeStampsScope()._timestamp_values(model)

    # Pre-fix: "2026-08-08 16:29:13" — the created_at output fed back through
    # DateTimeCast.set, shifted by the app-timezone offset twice.
    assert updated == _TRUE_UTC
    assert updated == created


def test_update_scope_stamps_the_true_instant(madrid_app_timezone):
    model = _StampedModel({"updated_at": "datetime"})
    builder = _Builder(model)

    TimeStampsScope().set_timestamp_update(builder)

    assert len(builder._updates) == 1
    assert builder._updates[0].column == {"updated_at": _TRUE_UTC}


def test_insert_scope_stamps_the_true_instant(madrid_app_timezone):
    model = _StampedModel({"created_at": "datetime", "updated_at": "datetime"})
    builder = _Builder(model)

    TimeStampsScope().set_timestamp_create(builder)

    assert builder._creates == {"created_at": _TRUE_UTC, "updated_at": _TRUE_UTC}


def test_uncast_columns_carry_their_offset_onto_the_wire(madrid_app_timezone):
    """A naive literal into TIMESTAMPTZ is resolved by the session TimeZone GUC.

    Cara never asserts that GUC, so the framework's own stamps must not depend
    on it — the emitted literal has to say UTC out loud.
    """
    model = _StampedModel({})

    created, updated = TimeStampsScope()._timestamp_values(model)

    assert created.endswith("Z") or "+00:00" in created
    assert pendulum.parse(created) == _NOW
    assert updated == created
