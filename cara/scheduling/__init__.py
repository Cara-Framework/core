"""Scheduling — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ALERT_FENCE_SECONDS": (".SchedulerHeartbeat", "ALERT_FENCE_SECONDS"),
    "APSchedulerDriver": (".drivers", "APSchedulerDriver"),
    "HEARTBEAT_CACHE_KEY": (".SchedulerHeartbeat", "HEARTBEAT_CACHE_KEY"),
    "HEARTBEAT_TTL_SECONDS": (".SchedulerHeartbeat", "HEARTBEAT_TTL_SECONDS"),
    "MEMO_SECONDS": (".SchedulerHeartbeat", "MEMO_SECONDS"),
    "SCHEDULE_SNAPSHOT_CACHE_KEY": (".Snapshot", "SCHEDULE_SNAPSHOT_CACHE_KEY"),
    "SCHEDULE_SNAPSHOT_EVERY_SECONDS": (".Snapshot", "SCHEDULE_SNAPSHOT_EVERY_SECONDS"),
    "SCHEDULE_SNAPSHOT_TTL_SECONDS": (".Snapshot", "SCHEDULE_SNAPSHOT_TTL_SECONDS"),
    "STALE_AFTER_SECONDS": (".SchedulerHeartbeat", "STALE_AFTER_SECONDS"),
    "ScheduleBuilder": (".ScheduleBuilder", "ScheduleBuilder"),
    "Scheduling": (".Scheduling", "Scheduling"),
    "SchedulingContract": (".contracts", "SchedulingContract"),
    "SchedulingProvider": (".SchedulingProvider", "SchedulingProvider"),
    "ShouldSchedule": (".contracts", "ShouldSchedule"),
    "declared_interval_seconds": (".WallClock", "declared_interval_seconds"),
    "last_scheduler_tick": (".SchedulerHeartbeat", "last_scheduler_tick"),
    "on_wall_clock": (".WallClock", "on_wall_clock"),
    "read_schedule_snapshot": (".Snapshot", "read_schedule_snapshot"),
    "record_scheduler_tick": (".SchedulerHeartbeat", "record_scheduler_tick"),
    "reset_scheduler_status_memo": (".SchedulerHeartbeat", "reset_scheduler_status_memo"),
    "scheduler_status": (".SchedulerHeartbeat", "scheduler_status"),
}

__all__ = [
    "ALERT_FENCE_SECONDS",
    "APSchedulerDriver",
    "HEARTBEAT_CACHE_KEY",
    "HEARTBEAT_TTL_SECONDS",
    "MEMO_SECONDS",
    "SCHEDULE_SNAPSHOT_CACHE_KEY",
    "SCHEDULE_SNAPSHOT_EVERY_SECONDS",
    "SCHEDULE_SNAPSHOT_TTL_SECONDS",
    "STALE_AFTER_SECONDS",
    "ScheduleBuilder",
    "Scheduling",
    "SchedulingContract",
    "SchedulingProvider",
    "ShouldSchedule",
    "declared_interval_seconds",
    "last_scheduler_tick",
    "on_wall_clock",
    "read_schedule_snapshot",
    "record_scheduler_tick",
    "reset_scheduler_status_memo",
    "scheduler_status",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
