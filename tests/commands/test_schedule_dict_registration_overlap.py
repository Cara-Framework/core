"""Dict-config schedule entries must be able to reach the overlap mutex.

``config/scheduling.py`` declares entries as dicts, and those never
touch ``ScheduleBuilder`` directly — ``_register_scheduled_dict_job``
translates them. The translation reads ``trigger``, ``id``, ``name``,
``kwargs``, the interval/cron fields and ``snapshot_meta``; if it does
not ALSO read ``without_overlapping``, the flag is silently dropped
between the config file and the driver, and an operator has no way to
enable the protection short of subclassing the command.

The ordering constraint is the part that is easy to break in a
refactor: the flag has to be applied to the builder BEFORE the
terminal ``interval()`` / ``daily()`` / ``cron()`` call, because that
call is what dispatches ``options`` to the driver. Moving the
``without_overlapping`` block below the trigger dispatch would leave
every test that only reads the builder green while the driver still
received the unprotected options dict — so these pins assert on what
the DRIVER received.

Default stays off: enabling it implicitly would start skipping ticks
for every existing entry whose previous tick is still in flight.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

import cara.commands.core._ScheduleRegistration as schedule_registration
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.exceptions import ConfigurationException
from cara.scheduling.ScheduleBuilder import ScheduleBuilder

_PROBE_MODULE = "_overlap_probe_job"


class _RecordingDriver:
    """Captures what the terminal trigger call dispatched."""

    def __init__(self) -> None:
        """Start with an empty call log."""
        self.calls: list[tuple[str, dict, dict]] = []

    def schedule_job(self, identifier, callback, schedule_spec, options) -> None:
        """Record one registration, snapshotting ``options`` by value."""
        self.calls.append((identifier, dict(schedule_spec), dict(options)))


class _CentralProbeJob:
    """A central scheduled job double.

    Scheduled-job registration is fail-closed on tenancy: an ordinary
    job with no ``tenant_id`` in its spec is rejected outright. Marking
    the probe central exercises the overlap plumbing rather than dying
    at that gate.
    """

    central_job = True

    def handle(self) -> None:
        """Do nothing — the callback is never invoked during registration."""
        return None


class _TenantProbeJob:
    """A tenant-owned scheduled job double."""

    central_job = False

    def handle(self) -> None:
        """Do nothing — the callback is never invoked during registration."""
        return None


@pytest.fixture
def register(monkeypatch) -> Any:
    """Return ``register(spec_extra) -> options`` for one dict entry.

    Installs a probe module for the spec's dotted ``job`` path, swaps
    the ``Schedule`` facade the helper reads for a stub bound to a
    recording driver, and returns the options dict the driver saw.
    """
    module = ModuleType(_PROBE_MODULE)
    module._CentralProbeJob = _CentralProbeJob
    module._TenantProbeJob = _TenantProbeJob
    monkeypatch.setitem(sys.modules, _PROBE_MODULE, module)

    driver = _RecordingDriver()
    monkeypatch.setattr(
        schedule_registration,
        "Schedule",
        SimpleNamespace(call=lambda callback: ScheduleBuilder(driver, "", callback, {})),
    )

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())
    command.warning = lambda *_args, **_kwargs: None
    command.info = lambda *_args, **_kwargs: None

    def _register(spec_extra: dict) -> dict:
        """Register one dict entry and return the driver-side options."""
        spec = {
            "job": f"{_PROBE_MODULE}._CentralProbeJob",
            "id": "sweep_inbox",
            "name": "Sweep the inbox",
            "trigger": "interval",
            "seconds": 30,
            **spec_extra,
        }
        command._register_dict_job(spec)
        return driver.calls[-1][2]

    _register.driver = driver
    _register.command = command
    return _register


# ── The flag reaches the driver ───────────────────────────────────────


class TestOverlapFlagPropagates:
    """``options`` is the only channel the driver reads."""

    def test_flag_true_lands_in_driver_options(self, register) -> None:
        """Without this the mutex branch in the driver never runs."""
        options = register({"without_overlapping": True})
        assert options.get("without_overlapping") is True

    def test_default_lock_timeout_is_one_day(self, register) -> None:
        """Mirrors the driver's own fallback, so a crashed holder cannot
        wedge the slot forever."""
        options = register({"without_overlapping": True})
        assert options.get("lock_timeout") == 86400

    def test_operator_lock_timeout_is_honoured(self, register) -> None:
        """A 30-second entry wants a far tighter TTL than a day."""
        options = register({"without_overlapping": True, "lock_timeout": 120})
        assert options.get("lock_timeout") == 120

    def test_string_lock_timeout_is_coerced(self, register) -> None:
        """Config is hand-written; ``"120"`` must not reach the cache TTL
        as a string, where the driver would hand it straight to Redis."""
        options = register({"without_overlapping": True, "lock_timeout": "120"})
        assert options.get("lock_timeout") == 120


class TestOverlapDefaultsOff:
    """Turning this on by default would change every existing entry."""

    def test_absent_flag_leaves_the_option_unset(self, register) -> None:
        """The backwards-compatible default."""
        assert register({}).get("without_overlapping") in (None, False)

    def test_explicit_false_leaves_the_option_unset(self, register) -> None:
        """Opting out must not leave a stray ``True`` behind."""
        options = register({"without_overlapping": False})
        assert options.get("without_overlapping") in (None, False)


class TestEveryTriggerPathHonoursTheFlag:
    """Interval and both cron shapes share one registration body — a
    refactor that splits them must keep all three wired. The nightly
    cron entries are the LONGEST running, so this matters most there."""

    def test_daily_cron_shape(self, register) -> None:
        """``trigger: cron`` with hour/minute becomes ``builder.daily()``."""
        options = register(
            {"trigger": "cron", "hour": 2, "minute": 0, "without_overlapping": True}
        )
        assert options.get("without_overlapping") is True

    def test_day_of_week_cron_shape(self, register) -> None:
        """``day_of_week`` switches the helper to ``builder.cron()``."""
        options = register(
            {
                "trigger": "cron",
                "hour": 2,
                "minute": 0,
                "day_of_week": "mon",
                "without_overlapping": True,
            }
        )
        assert options.get("without_overlapping") is True

    def test_interval_shape(self, register) -> None:
        """The default trigger shape, and the one most prone to overlap."""
        options = register(
            {"trigger": "interval", "seconds": 30, "without_overlapping": True}
        )
        assert options.get("without_overlapping") is True


class TestFlagIsAppliedBeforeDispatch:
    """The ordering constraint, asserted end to end."""

    def test_exactly_one_dispatch_carries_the_flag(self, register) -> None:
        """One registration, and the flag is on it — not on a later one
        and not on the builder only. If the ``without_overlapping``
        block ever moves below the trigger dispatch, the recorded call
        loses the key and this fails."""
        register({"without_overlapping": True, "lock_timeout": 300})

        calls = register.driver.calls
        assert len(calls) == 1
        _identifier, _spec, options = calls[0]
        assert options["without_overlapping"] is True
        assert options["lock_timeout"] == 300


class TestTenancyGateStillApplies:
    """Overlap protection must not become a way around the tenant gate."""

    def test_ordinary_job_without_tenant_id_is_refused(self, register) -> None:
        """Fail-closed: a non-central entry needs an explicit tenant."""
        with pytest.raises(ConfigurationException):
            register(
                {
                    "job": f"{_PROBE_MODULE}._TenantProbeJob",
                    "without_overlapping": True,
                }
            )

    def test_central_job_declaring_a_tenant_id_is_refused(self, register) -> None:
        """The two markers are mutually exclusive, flag or no flag."""
        with pytest.raises(ConfigurationException):
            register({"tenant_id": 7, "without_overlapping": True})
