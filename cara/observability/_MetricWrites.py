"""Exception-safe Prometheus writes shared by framework and app metrics."""

from __future__ import annotations


def _metric_child(metric, labels: dict):
    """Return an unlabelled collector or its labelled child."""
    return metric if not labels else metric.labels(**labels)


def _safe_inc(metric, labels: dict, amount: float = 1) -> bool:
    try:
        _metric_child(metric, labels).inc(amount)
    except Exception:
        return False
    return True


def _safe_observe(metric, labels: dict, value: float) -> bool:
    try:
        _metric_child(metric, labels).observe(value)
    except Exception:
        return False
    return True


def _safe_set(metric, labels: dict, value: float) -> bool:
    try:
        _metric_child(metric, labels).set(value)
    except Exception:
        return False
    return True
