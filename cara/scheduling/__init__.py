"""Scheduling — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "APSchedulerDriver": (".drivers", "APSchedulerDriver"),
    "SCHEDULE_SNAPSHOT_CACHE_KEY": (".Snapshot", "SCHEDULE_SNAPSHOT_CACHE_KEY"),
    "SCHEDULE_SNAPSHOT_EVERY_SECONDS": (".Snapshot", "SCHEDULE_SNAPSHOT_EVERY_SECONDS"),
    "SCHEDULE_SNAPSHOT_TTL_SECONDS": (".Snapshot", "SCHEDULE_SNAPSHOT_TTL_SECONDS"),
    "ScheduleBuilder": (".ScheduleBuilder", "ScheduleBuilder"),
    "Scheduling": (".Scheduling", "Scheduling"),
    "SchedulingContract": (".contracts", "SchedulingContract"),
    "SchedulingProvider": (".SchedulingProvider", "SchedulingProvider"),
    "ShouldSchedule": (".contracts", "ShouldSchedule"),
    "read_schedule_snapshot": (".Snapshot", "read_schedule_snapshot"),
}

__all__ = [
    "APSchedulerDriver",
    "SCHEDULE_SNAPSHOT_CACHE_KEY",
    "SCHEDULE_SNAPSHOT_EVERY_SECONDS",
    "SCHEDULE_SNAPSHOT_TTL_SECONDS",
    "ScheduleBuilder",
    "Scheduling",
    "SchedulingContract",
    "SchedulingProvider",
    "ShouldSchedule",
    "read_schedule_snapshot",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
