"""The published snapshot's per-entry metadata passthrough.

The snapshot is the ONLY thing a reader process sees about the live
schedule. Before this passthrough it carried ``{id, name, next_run_at}``
and nothing else, so any application fact a reader needed — the interval
vocabulary behind a sweep, say — had to travel on a second cache key that
the application invented, published from one deployable and read from
another, duplicating the key constant across both trees. These tests pin
the passthrough that removes the need for that second channel: opaque in,
opaque out, and ABSENT when nothing was registered.
"""

from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace

import cara.commands.core.ScheduleWorkCommand as schedule_work_command
from cara.commands.core.ScheduleWorkCommand import ScheduleWorkCommand
from cara.scheduling.drivers.APSchedulerDriver import APSchedulerDriver
from cara.scheduling.ScheduleBuilder import ScheduleBuilder


class _RecordingDriver:
    """Captures what a terminal trigger call dispatches to the driver."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, dict]] = []

    def schedule_job(self, identifier, callback, schedule_spec, options) -> None:
        self.calls.append((identifier, dict(schedule_spec), dict(options)))


def _builder(driver) -> ScheduleBuilder:
    return ScheduleBuilder(driver, "sweep_inbox", lambda: None, {})


# ── ScheduleBuilder: metadata must reach the driver's options ─────────


def test_snapshot_meta_rides_options_into_the_driver() -> None:
    driver = _RecordingDriver()

    _builder(driver).snapshot_meta({"default_minutes": 360}).interval(minutes=15)

    _identifier, _spec, options = driver.calls[0]
    assert options["snapshot_meta"] == {"default_minutes": 360}


def test_snapshot_meta_is_copied_not_aliased() -> None:
    """A caller mutating its own dict afterwards must not rewrite the
    registered metadata — the schedule config is frequently a module-level
    literal shared across entries."""
    driver = _RecordingDriver()
    source = {"default_minutes": 360}

    builder = _builder(driver)
    builder.snapshot_meta(source)
    source["default_minutes"] = 1

    builder.interval(minutes=15)

    _identifier, _spec, options = driver.calls[0]
    assert options["snapshot_meta"] == {"default_minutes": 360}


def test_builder_without_snapshot_meta_sends_no_key() -> None:
    driver = _RecordingDriver()

    _builder(driver).interval(minutes=15)

    _identifier, _spec, options = driver.calls[0]
    assert "snapshot_meta" not in options


# ── Dict-config entries: the shape applications actually declare ──────


def _dict_spec_options(monkeypatch, spec_extra: dict) -> dict:
    """Register one dict-config entry and return what reached the driver.

    Dict entries in ``config/scheduling.py`` are the shape real
    applications use, and they never touch ``ScheduleBuilder`` directly —
    so the spec-key hop needs its own pin, or metadata could reach the
    fluent API and silently never reach config.
    """
    module = ModuleType("_snapshot_meta_probe_job")

    class _ProbeJob:
        central_job = True

        def handle(self) -> None:
            return None

    module._ProbeJob = _ProbeJob
    monkeypatch.setitem(sys.modules, "_snapshot_meta_probe_job", module)

    driver = _RecordingDriver()
    monkeypatch.setattr(
        schedule_work_command,
        "Schedule",
        SimpleNamespace(call=lambda callback: ScheduleBuilder(driver, "", callback, {})),
    )

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command.application = SimpleNamespace(make=lambda cls: cls())
    command._register_dict_job(
        {
            "job": "_snapshot_meta_probe_job._ProbeJob",
            "id": "sweep_inbox",
            "name": "Sweep the inbox",
            "trigger": "interval",
            "minutes": 15,
            **spec_extra,
        }
    )

    _identifier, _schedule_spec, options = driver.calls[0]
    return options


def test_a_dict_config_entry_forwards_its_snapshot_meta(monkeypatch) -> None:
    options = _dict_spec_options(monkeypatch, {"snapshot_meta": {"default_minutes": 360}})

    assert options["snapshot_meta"] == {"default_minutes": 360}


def test_a_dict_entry_with_an_empty_or_wrong_typed_meta_sends_no_key(
    monkeypatch,
) -> None:
    """Config is hand-written. A typo must publish nothing rather than a
    junk value a reader would then have to defend against."""
    assert "snapshot_meta" not in _dict_spec_options(monkeypatch, {})
    assert "snapshot_meta" not in _dict_spec_options(monkeypatch, {"snapshot_meta": {}})
    assert "snapshot_meta" not in _dict_spec_options(
        monkeypatch, {"snapshot_meta": "360"}
    )


# ── Driver: job-id keyed storage, replaced and pruned ─────────────────


class _EngineFreeDriver(APSchedulerDriver):
    """The real driver with only its APScheduler engine stubbed out.

    APScheduler is a dependency of the APPLICATIONS that schedule (services
    declares ``apscheduler==3.11.3``); cara declares none and its own test
    environment has no engine — the same reason
    ``tests/observability/test_job_class_metric_labels.py`` reaches into
    this module for ``_instrument_scheduled`` instead of building a driver.

    The snapshot-metadata map is driver state that never touches the
    engine, so stubbing the two seams that do — trigger construction and
    scheduler creation — runs the REAL ``schedule_job`` / ``remove_job``
    bodies rather than a paraphrase of them.
    """

    def _create_scheduler(self):
        return SimpleNamespace(running=False, remove_job=lambda **_kwargs: None)

    def _build_trigger(self, spec):
        return spec


def _apscheduler_driver():
    return _EngineFreeDriver()


def test_driver_stores_and_returns_metadata_by_job_id() -> None:
    driver = _apscheduler_driver()

    driver.schedule_job(
        "sweep_inbox",
        lambda: None,
        {"type": "interval", "minutes": 15},
        {"silent": True, "snapshot_meta": {"default_minutes": 360}},
    )

    assert driver.snapshot_meta("sweep_inbox") == {"default_minutes": 360}
    assert driver.snapshot_meta("never_registered") is None


def test_reregistering_without_metadata_clears_the_previous_entry() -> None:
    """Re-registration is how a restart re-applies the schedule. A config
    that DROPPED its metadata must not keep publishing the old dict."""
    driver = _apscheduler_driver()
    spec = {"type": "interval", "minutes": 15}

    driver.schedule_job(
        "sweep_inbox", lambda: None, spec, {"silent": True, "snapshot_meta": {"a": 1}}
    )
    driver.schedule_job("sweep_inbox", lambda: None, spec, {"silent": True})

    assert driver.snapshot_meta("sweep_inbox") is None


def test_remove_job_prunes_the_metadata_map() -> None:
    """A long-lived scheduler that churns jobs must not leak the map."""
    driver = _apscheduler_driver()

    driver.schedule_job(
        "sweep_inbox",
        lambda: None,
        {"type": "interval", "minutes": 15},
        {"silent": True, "snapshot_meta": {"a": 1}},
    )
    driver.remove_job("sweep_inbox")

    assert driver.snapshot_meta("sweep_inbox") is None


# ── Publisher: 'meta' present only when registered ────────────────────


class _FakeCache:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def put(self, key, value, ttl=None) -> None:
        self.store[key] = value


class _StubDriver:
    """Only what ``_publish_schedule_snapshot`` actually touches."""

    def __init__(self, jobs, meta=None) -> None:
        self._jobs = jobs
        self._meta = meta or {}

    def list_jobs(self):
        return self._jobs

    def snapshot_meta(self, identifier):
        return self._meta.get(identifier)


class _MetalessDriver:
    """A driver predating the passthrough — must stay valid."""

    def __init__(self, jobs) -> None:
        self._jobs = jobs

    def list_jobs(self):
        return self._jobs


def _published(monkeypatch, driver) -> dict:
    cache = _FakeCache()
    monkeypatch.setattr("cara.facades.Cache", cache, raising=False)

    command = ScheduleWorkCommand.__new__(ScheduleWorkCommand)
    command._snapshot_at = 0.0
    command._publish_schedule_snapshot(driver)

    from cara.scheduling.Snapshot import SCHEDULE_SNAPSHOT_CACHE_KEY

    return json.loads(cache.store[SCHEDULE_SNAPSHOT_CACHE_KEY])


def _job(job_id, next_run_time=None):
    return SimpleNamespace(id=job_id, name=job_id, next_run_time=next_run_time)


def test_published_entry_carries_registered_metadata(monkeypatch) -> None:
    snapshot = _published(
        monkeypatch,
        _StubDriver([_job("sweep_inbox")], {"sweep_inbox": {"default_minutes": 360}}),
    )

    assert snapshot["jobs"][0]["meta"] == {"default_minutes": 360}


def test_entry_without_metadata_omits_the_key(monkeypatch) -> None:
    """Absent, not ``None`` — readers key on ``.get("meta")`` and an
    always-present null would make "no metadata" and "metadata is null"
    indistinguishable."""
    snapshot = _published(monkeypatch, _StubDriver([_job("sweep_inbox")]))

    assert "meta" not in snapshot["jobs"][0]


def test_paused_entry_still_publishes_its_metadata(monkeypatch) -> None:
    """``next_run_at`` is None while a job is paused. Not knowing WHEN it
    runs does not mean not knowing how often it is meant to."""
    snapshot = _published(
        monkeypatch,
        _StubDriver([_job("sweep_inbox")], {"sweep_inbox": {"default_minutes": 360}}),
    )

    entry = snapshot["jobs"][0]
    assert entry["next_run_at"] is None
    assert entry["meta"] == {"default_minutes": 360}


def test_driver_without_the_hook_still_publishes(monkeypatch) -> None:
    """The hook is OPTIONAL in the driver contract — probed with getattr so
    an alternate driver that never implements it keeps working."""
    snapshot = _published(monkeypatch, _MetalessDriver([_job("sweep_inbox")]))

    assert snapshot["jobs"][0]["id"] == "sweep_inbox"
    assert "meta" not in snapshot["jobs"][0]
