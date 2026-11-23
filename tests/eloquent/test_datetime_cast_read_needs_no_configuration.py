"""A date-time cast reads no application configuration — read or write.

``DateTimeCast.get`` shifted every value into ``app.timezone``, read per
attribute from the booted configuration, so a model's datetime could not be
read outside a booted application: a kernel projection folding inside an
append (the profit fold reads ``order.placed_at``) raised
``InvalidConfigurationSetupException`` in any process that had not booted.
A read now presents the instant in the cast's own declared zone (UTC unless
the model declares ``"datetime:<format>,<zone>"``).

``DateTimeCast.set`` read ``app.timezone`` to GUESS the zone of a naive
write, so one write meant a different instant in each product. A write is an
instant or it is refused (DOCTRINE §7): a naive ``datetime``, an offset-less
string or a bare date raises ``NaiveDateTimeException`` — with or without a
configuration, whatever ``app.timezone`` says.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock

import pendulum
import pytest

from cara.configuration import Configuration
from cara.eloquent.casts.CastRegistry import CastRegistry
from cara.eloquent.casts.DateTimeCast import DateTimeCast
from cara.eloquent.casts.TimestampCast import TimestampCast
from cara.eloquent.concerns.HasAttributes import HasAttributes
from cara.eloquent.models import Model
from cara.eloquent.query import QueryBuilder
from cara.exceptions import NaiveDateTimeException

_INSTANT = pendulum.datetime(2026, 11, 23, 7, 30, 15, 120000, tz="UTC")


@pytest.fixture(autouse=True)
def no_configuration(monkeypatch):
    """A process that never registered ``ConfigurationProvider``."""
    monkeypatch.setattr(Configuration, "_instance", None)


@pytest.mark.parametrize(
    "stored",
    [
        "2026-11-23 07:30:15.12+00:00",
        dt.datetime(2026, 11, 23, 7, 30, 15, 120000, tzinfo=dt.UTC),
    ],
)
def test_a_read_is_the_utc_instant_without_configuration(stored):
    read = DateTimeCast().get(stored)

    assert read == _INSTANT
    assert read.timezone_name == "UTC"


def test_a_declared_zone_presents_the_same_instant():
    registry = CastRegistry()
    registry.register("datetime", DateTimeCast)
    cast = registry.get_cast_instance("datetime:Y-m-d H:i:s,Europe/Istanbul")

    read = cast.get("2026-11-23 07:30:15.12+00:00")

    assert read == _INSTANT
    assert read.timezone_name == "Europe/Istanbul"
    assert read.hour == 10


def test_an_aware_write_needs_no_configuration():
    assert DateTimeCast().set(_INSTANT) == "2026-11-23 07:30:15.120000+00:00"
    assert DateTimeCast().set("2026-11-23T07:30:15Z") == "2026-11-23 07:30:15+00:00"
    assert DateTimeCast().set("2026-11-23T10:30:15+03:00") == (
        "2026-11-23 07:30:15+00:00"
    )


@pytest.mark.parametrize(
    "naive",
    [
        dt.datetime(2026, 11, 23, 7, 30, 15),
        "2026-11-23 07:30:15",
        "2026-11-23",
    ],
)
@pytest.mark.parametrize("cast", [DateTimeCast(), TimestampCast()])
def test_a_naive_write_is_refused(cast, naive):
    with pytest.raises(NaiveDateTimeException):
        cast.set(naive)


def test_a_naive_write_is_refused_even_under_a_configured_zone():
    configuration = Configuration.empty()
    configuration.set("app.timezone", "Europe/Istanbul")

    with pytest.raises(NaiveDateTimeException):
        DateTimeCast().set(dt.datetime(2026, 11, 23, 7, 30, 15))


def test_the_refusal_names_the_model_attribute():
    class Order(HasAttributes):
        __casts__ = {"placed_at": "datetime"}

    with pytest.raises(NaiveDateTimeException, match=r"Order\.placed_at"):
        Order()._set_cast_attribute("placed_at", dt.datetime(2026, 11, 23, 7, 30))


class _Parcel(Model):
    """A cast date-time column beside the two uncast framework dates."""

    __table__ = "parcel"
    __casts__ = {"placed_at": "datetime"}


_NAIVE = dt.datetime(2026, 11, 23, 7, 30)


@pytest.fixture
def parcel(monkeypatch):
    """A loaded row, with no connection behind it."""
    monkeypatch.setattr(_Parcel, "get_builder", lambda _self: MagicMock())
    return _Parcel.hydrate({"id": 1})


@pytest.mark.parametrize("column", ["placed_at", "created_at"])
def test_every_model_write_door_refuses_a_naive_value_by_name(parcel, column):
    named = rf"_Parcel\.{column}"

    with pytest.raises(NaiveDateTimeException, match=named):
        setattr(parcel, column, _NAIVE)
    builder = QueryBuilder(
        model=parcel, table="parcel", dry=True, database_manager=MagicMock()
    )
    with pytest.raises(NaiveDateTimeException, match=named):
        builder.update({column: _NAIVE}, ignore_mass_assignment=True)


@pytest.mark.parametrize("column", ["placed_at", "created_at"])
def test_every_row_write_door_refuses_a_naive_value_by_name(parcel, column):
    """create, bulk_create and upsert state whole rows, so an uncast date
    column (which no ``setattr`` ever sees there) is refused as a cast one."""
    named = rf"_Parcel\.{column}"

    def builder() -> QueryBuilder:
        return QueryBuilder(
            model=parcel, table="parcel", dry=True, database_manager=MagicMock()
        )

    with pytest.raises(NaiveDateTimeException, match=named):
        builder().create({column: _NAIVE}, ignore_mass_assignment=True)
    with pytest.raises(NaiveDateTimeException, match=named):
        _Parcel.cast_values({column: _NAIVE})


def test_an_uncast_date_column_keeps_the_microseconds_of_its_instant(parcel):
    parcel.created_at = _INSTANT

    assert parcel.get_dirty_attributes()["created_at"] == (
        "2026-11-23 07:30:15.120000+00:00"
    )
