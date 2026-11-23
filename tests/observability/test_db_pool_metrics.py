"""Tests for the DB connection-pool saturation gauges in MetricsBase.

Covers:

* the three gauges are registered on the shared Prometheus registry with
  the namespaced names,
* :func:`sample_db_pool_metrics` is a safe no-op (does not raise, leaves
  gauges untouched) while no pool exists, and
* the sampler reads ``cara.eloquent.database_pool_stats()`` — ``in_use`` /
  ``idle`` / ``max`` summed over every connection's ``PostgresPool`` — and
  writes the gauges, refusing figures that cannot be true.

The ORM keeps one pool per configured connection; real ``PostgresPool``
objects are registered (``PostgresPool.of``) and driven through their own
API rather than spinning up Postgres.
"""

from __future__ import annotations

import importlib

import pytest

from cara.eloquent import PostgresPool, database_pool_stats
from cara.observability.MetricsBase import (
    REGISTRY,
    MetricsBase,
    metric_name,
    sample_db_pool_metrics,
)

RuntimeMetrics = importlib.import_module("cara.observability._RuntimeMetrics")


@pytest.fixture(autouse=True)
def no_pools():
    """Every test starts and ends with no pool registered."""
    PostgresPool.reset_all(close=False)
    yield
    PostgresPool.reset_all(close=False)


def _pool(name: str, *, max_size: int, in_use: int = 0, idle: int = 0):
    """A registered pool with ``in_use`` permits out and ``idle`` parked
    sessions (stand-ins: only ever counted, never used)."""
    pool = PostgresPool.of((name, ()), name, max_size)
    for _ in range(in_use):
        assert pool.acquire(0)
    pool.warm(lambda: _Session(), idle)
    return pool


class _Session:
    autocommit = True

    def close(self):
        return None


# ── Registration ─────────────────────────────────────────────────────


def test_pool_gauges_registered_with_namespaced_names():
    names = REGISTRY._names_to_collectors
    assert metric_name("db_pool_connections_in_use") in names
    assert metric_name("db_pool_connections_idle") in names
    assert metric_name("db_pool_connections_max") in names


def test_pool_gauges_are_attributes_on_metrics_base():
    assert hasattr(MetricsBase, "db_pool_connections_in_use")
    assert hasattr(MetricsBase, "db_pool_connections_idle")
    assert hasattr(MetricsBase, "db_pool_connections_max")


# ── No pool yet ──────────────────────────────────────────────────────


def test_read_stats_returns_none_while_no_pool_exists():
    assert database_pool_stats() is None
    assert RuntimeMetrics._read_db_pool_stats() is None


def test_sample_is_noop_and_does_not_raise_while_no_pool_exists():
    MetricsBase.db_pool_connections_max.set(-7)

    sample_db_pool_metrics()

    assert MetricsBase.db_pool_connections_max._value.get() == -7


# ── Figures that cannot be true ──────────────────────────────────────


def test_read_stats_rejects_missing_pool_ceiling(monkeypatch):
    monkeypatch.setattr(
        RuntimeMetrics,
        "database_pool_stats",
        lambda: {"in_use": 0, "idle": 0, "max": None},
    )

    with pytest.raises(RuntimeError, match="invalid 'max'"):
        RuntimeMetrics._read_db_pool_stats()


def test_read_stats_rejects_zero_pool_ceiling(monkeypatch):
    monkeypatch.setattr(
        RuntimeMetrics,
        "database_pool_stats",
        lambda: {"in_use": 0, "idle": 0, "max": 0},
    )

    with pytest.raises(RuntimeError, match="publish a positive"):
        RuntimeMetrics._read_db_pool_stats()


def test_read_stats_rejects_more_in_use_than_the_ceiling():
    pool = _pool("app", max_size=20)
    pool._in_use = 21  # a count no acquire/release sequence can reach

    with pytest.raises(RuntimeError, match="exceeds"):
        RuntimeMetrics._read_db_pool_stats()


# ── Live pools ───────────────────────────────────────────────────────


def test_read_stats_derives_in_use_idle_max():
    _pool("app", max_size=20, in_use=12, idle=2)

    assert RuntimeMetrics._read_db_pool_stats() == {"in_use": 12, "idle": 2, "max": 20}


def test_read_stats_sums_every_connections_pool():
    """One pool per configured connection; the gauges stay process totals."""
    _pool("app", max_size=20, in_use=5, idle=1)
    _pool("reporting", max_size=4, in_use=3, idle=1)

    assert RuntimeMetrics._read_db_pool_stats() == {"in_use": 8, "idle": 2, "max": 24}
    assert sorted(entry["connection"] for entry in PostgresPool.all_stats()) == [
        "app",
        "reporting",
    ]


def test_released_permits_leave_the_in_use_figure():
    pool = _pool("app", max_size=4, in_use=3)
    pool.release()

    assert database_pool_stats() == {"in_use": 2, "idle": 0, "max": 4}


def test_sample_writes_gauge_values():
    _pool("app", max_size=20, in_use=5, idle=1)

    sample_db_pool_metrics()

    assert MetricsBase.db_pool_connections_in_use._value.get() == 5
    assert MetricsBase.db_pool_connections_idle._value.get() == 1
    assert MetricsBase.db_pool_connections_max._value.get() == 20


def test_render_invokes_pool_sampler_without_raising():
    """``render()`` calls the sampler before serialising; with no pool yet
    it must still produce a payload."""
    from cara.observability import render

    payload, content_type = render(service="test", role="test")
    assert isinstance(payload, bytes)
    assert "text/plain" in content_type
