"""Metric labels stay unambiguous after Prometheus ingestion."""

import importlib
import time
import uuid

from cara.observability import MetricsBase, init_build_info
from cara.scheduling.drivers.APSchedulerDriver import _instrument_scheduled


def test_queue_metrics_use_job_class_label() -> None:
    metric_names = (
        "queue_dispatches_total",
        "queue_jobs_consumed_total",
        "queue_job_duration_seconds",
        "queue_jobs_in_flight",
        "queue_wait_seconds",
        "queue_jobs_dead_lettered_total",
    )

    for name in metric_names:
        labels = getattr(MetricsBase, name)._labelnames
        assert "job_class" in labels
        assert "job" not in labels


def test_build_info_accepts_runtime_role_override() -> None:
    try:
        init_build_info(MetricsBase, role="scheduler")
        samples = [
            sample
            for family in MetricsBase.build_info.collect()
            for sample in family.samples
        ]
        assert len(samples) == 1
        assert samples[0].labels["role"] == "scheduler"
    finally:
        init_build_info(MetricsBase)


def test_scheduler_wrapper_emits_tick_and_per_task_last_run() -> None:
    task = f"metric_contract_{uuid.uuid4().hex}"
    before = time.time()

    wrapped = _instrument_scheduled(task, lambda: "done")

    assert wrapped() == "done"
    assert MetricsBase.scheduler_last_tick_timestamp_seconds._value.get() >= before
    assert (
        MetricsBase.scheduled_task_last_run_timestamp_seconds.labels(
            task=task
        )._value.get()
        >= before
    )
    assert (
        MetricsBase.scheduled_tasks_total.labels(
            task=task,
            outcome="success",
        )._value.get()
        >= 1
    )


def test_render_restamps_build_info_after_config_boot(monkeypatch) -> None:
    metrics_module = importlib.import_module("cara.observability.Metrics")
    resolved = {"metrics.service": "test-api", "metrics.role": "web"}
    original_config = metrics_module.config
    monkeypatch.setattr(
        metrics_module,
        "config",
        lambda key, default=None: resolved.get(key, default),
    )

    try:
        metrics_module.render()
        samples = [
            sample
            for family in MetricsBase.build_info.collect()
            for sample in family.samples
        ]
        assert len(samples) == 1
        assert samples[0].labels == {"service": "test-api", "role": "web"}
    finally:
        monkeypatch.setattr(metrics_module, "config", original_config)
        init_build_info(MetricsBase)
