"""``APSchedulerDriver._build_trigger`` — the whole schedule vocabulary.

This dispatcher is the only bridge between a schedule declaration
(fluent ``ScheduleBuilder`` call or dict entry in ``config/scheduling.py``)
and the engine that actually fires it. Every failure mode here is
SILENT: a dropped branch raises at registration and the scheduler logs
one warning while the job never runs again; a mis-forwarded field
produces a trigger that fires on the wrong cadence and nothing
complains. Synkronus registers its whole schedule through this one
method, so the vocabulary needs a pin per branch.

Why stubbed trigger classes instead of real APScheduler objects:
cara declares NO apscheduler dependency — not in ``install_requires``,
not in any extra, so ``.[all,dev]`` (what CI installs) has no engine.
The scheduling *applications* declare it. That is also why
``tests/scheduling/test_snapshot_meta.py`` stubs the engine seam. What
belongs to cara here is the parse and the forwarding — which spec key
becomes which trigger kwarg, which shapes are refused, and whether the
timezone reaches the trigger at all — and recording stubs pin exactly
that, in every environment, rather than skipping wherever the optional
dependency is missing.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver

# ── Recording stand-ins for the three APScheduler trigger classes ─────


class _RecordedTrigger:
    """Captures the keyword arguments the driver forwarded."""

    kind = "trigger"

    def __init__(self, **kwargs) -> None:
        """Record every forwarded kwarg for inspection."""
        self.kwargs = kwargs


class _CronTrigger(_RecordedTrigger):
    """Stand-in for ``apscheduler.triggers.cron.CronTrigger``."""

    kind = "cron"


class _IntervalTrigger(_RecordedTrigger):
    """Stand-in for ``apscheduler.triggers.interval.IntervalTrigger``."""

    kind = "interval"


class _DateTrigger(_RecordedTrigger):
    """Stand-in for ``apscheduler.triggers.date.DateTrigger``."""

    kind = "date"


def _trigger_module(name: str, attribute: str, cls: type) -> ModuleType:
    """Build a one-symbol module so ``from ... import X`` resolves to ``cls``."""
    module = ModuleType(name)
    setattr(module, attribute, cls)
    return module


@pytest.fixture(autouse=True)
def _stub_apscheduler_triggers(monkeypatch) -> None:
    """Serve the three trigger modules from ``sys.modules``.

    ``_build_trigger`` imports them lazily inside the function body, so
    a ``sys.modules`` entry is enough — and ``monkeypatch.setitem``
    restores the real modules afterwards wherever they exist.
    """
    modules = {
        "apscheduler": ModuleType("apscheduler"),
        "apscheduler.triggers": ModuleType("apscheduler.triggers"),
        "apscheduler.triggers.cron": _trigger_module(
            "apscheduler.triggers.cron", "CronTrigger", _CronTrigger
        ),
        "apscheduler.triggers.date": _trigger_module(
            "apscheduler.triggers.date", "DateTrigger", _DateTrigger
        ),
        "apscheduler.triggers.interval": _trigger_module(
            "apscheduler.triggers.interval", "IntervalTrigger", _IntervalTrigger
        ),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


class _EngineFreeDriver(APSchedulerDriver):
    """The real driver with only its engine constructor stubbed out.

    ``_build_trigger`` — the subject — runs unmodified; only
    ``_create_scheduler``, which would import and construct a real
    ``BackgroundScheduler``, is replaced. Same seam
    ``tests/scheduling/test_snapshot_meta.py`` uses.
    """

    def _create_scheduler(self):
        """Return an inert stand-in for the APScheduler engine."""
        return SimpleNamespace(running=False, remove_job=lambda **_kwargs: None)


@pytest.fixture
def driver() -> _EngineFreeDriver:
    """A driver whose scheduler-level timezone default is UTC."""
    return _EngineFreeDriver(settings={"timezone": "UTC"})


@pytest.fixture
def tzless_driver() -> _EngineFreeDriver:
    """A driver with no scheduler-level timezone configured."""
    return _EngineFreeDriver(settings={})


# ── Cron expressions ──────────────────────────────────────────────────


class TestCronExpressions:
    """Five whitespace-separated fields, positionally mapped."""

    def test_five_fields_map_positionally(self, driver) -> None:
        """minute hour day month day_of_week — in that order."""
        trigger = driver._build_trigger(
            {"type": "cron", "expression": "*/5 1 2 3 mon"}
        )
        assert trigger.kind == "cron"
        assert trigger.kwargs == {
            "minute": "*/5",
            "hour": "1",
            "day": "2",
            "month": "3",
            "day_of_week": "mon",
            "timezone": "UTC",
        }

    def test_irregular_whitespace_is_tolerated(self, driver) -> None:
        """Hand-written config gets padded; ``split()`` must absorb it."""
        trigger = driver._build_trigger(
            {"type": "cron", "expression": "  0   9 * * *  "}
        )
        assert trigger.kwargs["minute"] == "0"
        assert trigger.kwargs["hour"] == "9"

    @pytest.mark.parametrize(
        "expression",
        ["* * * *", "0 */5 * * * *", "*/5", "* * * * * * *"],
    )
    def test_wrong_field_count_is_refused(self, driver, expression: str) -> None:
        """A 6-field expression is APScheduler's seconds-first dialect.

        Accepting it would turn "every 5 minutes" into "every 5
        seconds" without a word of warning, so the count is checked
        before anything is constructed.
        """
        with pytest.raises(ValueError):
            driver._build_trigger({"type": "cron", "expression": expression})

    @pytest.mark.parametrize("expression", ["", "   ", None, 5])
    def test_missing_or_non_string_expression_is_refused(
        self, driver, expression
    ) -> None:
        """An absent expression must raise, not build an every-minute cron."""
        with pytest.raises(ValueError):
            driver._build_trigger({"type": "cron", "expression": expression})

    def test_absent_expression_key_is_refused(self, driver) -> None:
        """The key defaults to ``""`` — which must still be rejected."""
        with pytest.raises(ValueError):
            driver._build_trigger({"type": "cron"})


# ── Interval ──────────────────────────────────────────────────────────


class TestIntervalSpecs:
    """Every unit APScheduler sums must reach it, defaulted to zero."""

    def test_single_unit_forwards_with_zeroed_siblings(self, driver) -> None:
        """An unspecified unit is 0, never absent — the engine sums them."""
        trigger = driver._build_trigger({"type": "interval", "seconds": 30})
        assert trigger.kind == "interval"
        assert trigger.kwargs == {
            "seconds": 30,
            "minutes": 0,
            "hours": 0,
            "days": 0,
            "weeks": 0,
            "timezone": "UTC",
        }

    def test_composite_units_all_forward(self, driver) -> None:
        """A dropped unit here silently shortens or lengthens the period."""
        trigger = driver._build_trigger(
            {
                "type": "interval",
                "weeks": 1,
                "days": 2,
                "hours": 3,
                "minutes": 4,
                "seconds": 5,
            }
        )
        assert trigger.kwargs["weeks"] == 1
        assert trigger.kwargs["days"] == 2
        assert trigger.kwargs["hours"] == 3
        assert trigger.kwargs["minutes"] == 4
        assert trigger.kwargs["seconds"] == 5


# ── Daily / hourly / weekly sugar ─────────────────────────────────────


class TestCalendarSugar:
    """``daily`` / ``hourly`` / ``weekly`` are cron under the hood."""

    def test_daily_pins_hour_and_minute(self, driver) -> None:
        """Only the two fields it names — the rest stay APScheduler defaults."""
        trigger = driver._build_trigger({"type": "daily", "hour": 9, "minute": 30})
        assert trigger.kind == "cron"
        assert trigger.kwargs == {"hour": 9, "minute": 30, "timezone": "UTC"}

    def test_daily_defaults_to_midnight(self, driver) -> None:
        """An hourless daily entry is 00:00, not "now"."""
        trigger = driver._build_trigger({"type": "daily"})
        assert trigger.kwargs["hour"] == 0
        assert trigger.kwargs["minute"] == 0

    def test_hourly_pins_only_the_minute(self, driver) -> None:
        """Pinning the hour too would make it a daily job."""
        trigger = driver._build_trigger({"type": "hourly", "minute": 15})
        assert trigger.kind == "cron"
        assert trigger.kwargs == {"minute": 15, "timezone": "UTC"}

    def test_weekly_wildcards_day_and_month(self, driver) -> None:
        """``day`` / ``month`` must be ``*`` or the entry fires once a year."""
        trigger = driver._build_trigger(
            {"type": "weekly", "day_of_week": "mon", "hour": 14, "minute": 30}
        )
        assert trigger.kind == "cron"
        assert trigger.kwargs == {
            "minute": 30,
            "hour": 14,
            "day": "*",
            "month": "*",
            "day_of_week": "mon",
            "timezone": "UTC",
        }


# ── One-shot date / at ────────────────────────────────────────────────


class TestOneShotSpecs:
    """``at`` is the contract's name, ``date`` is APScheduler's."""

    def test_at_builds_a_date_trigger(self, driver) -> None:
        """``ScheduleBuilder.at()`` emits ``type="at"``; the driver must
        accept it, or every fluent ``at()`` call raises at registration."""
        trigger = driver._build_trigger(
            {"type": "at", "run_date": "2030-01-01T00:00:00"}
        )
        assert trigger.kind == "date"
        assert trigger.kwargs == {
            "run_date": "2030-01-01T00:00:00",
            "timezone": "UTC",
        }

    def test_date_is_still_accepted(self, driver) -> None:
        """The APScheduler-native spelling stays valid."""
        trigger = driver._build_trigger(
            {"type": "date", "run_date": "2030-06-15T12:00:00"}
        )
        assert trigger.kind == "date"

    def test_missing_run_date_is_refused(self, driver) -> None:
        """Without a date there is nothing to fire — raise, never default."""
        with pytest.raises(ValueError):
            driver._build_trigger({"type": "at"})


# ── Timezone resolution ───────────────────────────────────────────────


class TestTimezoneForwarding:
    """``timezone=None`` resolves to the LOCAL OS timezone at trigger
    construction — NOT the scheduler default. So the scheduler-level
    setting has to be substituted here, or a "daily at 03:00" entry
    fires at 03:00 local on any non-UTC host while its SQL window is
    computed in UTC."""

    @pytest.mark.parametrize(
        "spec",
        [
            {"type": "cron", "expression": "0 9 * * *"},
            {"type": "interval", "seconds": 30},
            {"type": "daily", "hour": 3},
            {"type": "hourly"},
            {"type": "weekly", "day_of_week": "mon"},
            {"type": "at", "run_date": "2030-01-01T00:00:00"},
        ],
    )
    def test_scheduler_default_reaches_every_trigger(self, driver, spec) -> None:
        """Every branch forwards a timezone — no branch may omit it."""
        assert driver._build_trigger(spec).kwargs["timezone"] == "UTC"

    def test_per_entry_timezone_overrides_the_scheduler_default(
        self, driver
    ) -> None:
        """A report pinned to a business timezone must win."""
        trigger = driver._build_trigger(
            {
                "type": "cron",
                "expression": "0 9 * * *",
                "timezone": "America/New_York",
            }
        )
        assert trigger.kwargs["timezone"] == "America/New_York"

    def test_without_either_setting_the_timezone_is_none(
        self, tzless_driver
    ) -> None:
        """Documented fall-through: no scheduler default and no per-entry
        value leaves APScheduler on the host timezone. Pinned so the
        substitution is visibly a *fallback*, not an unconditional
        rewrite that would ignore a deliberate ``None``."""
        trigger = tzless_driver._build_trigger({"type": "interval", "seconds": 30})
        assert trigger.kwargs["timezone"] is None


# ── Unknown and missing types ─────────────────────────────────────────


class TestRejectedSpecs:
    """A typo must fail loudly at registration, never register nothing."""

    def test_unknown_type_raises_and_names_the_typo(self, driver) -> None:
        """The message has to carry the offending value to be actionable."""
        with pytest.raises(ValueError) as raised:
            driver._build_trigger({"type": "intervall", "seconds": 30})
        assert "intervall" in str(raised.value)

    def test_missing_type_raises(self, driver) -> None:
        """A spec without ``type`` cannot be guessed from its other keys."""
        with pytest.raises(ValueError):
            driver._build_trigger({"expression": "* * * * *"})
