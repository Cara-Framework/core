"""Benchmark — quick wall-clock measurement helper.

Laravel's ``Illuminate\\Support\\Benchmark`` parity. Times one or
more callables and returns / prints the elapsed milliseconds::

    elapsed = Benchmark.measure(lambda: heavy_query())
    # → 124.3   (ms)

    results = Benchmark.measure({
        "query": lambda: heavy_query(),
        "render": lambda: render_page(),
    })
    # → {"query": 124.3, "render": 18.7}

    Benchmark.dd(lambda: heavy_query())
    # prints "[BENCHMARK] 124.3 ms" then re-raises SystemExit.

Designed for ad-hoc profiling during development — not a
replacement for proper APM. Each call runs the callback exactly
once and uses :func:`time.perf_counter` for monotonic precision.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Mapping, Union


class Benchmark:
    """Time callables and return elapsed milliseconds."""

    @staticmethod
    def measure(
        target: Union[Callable[[], Any], Mapping[str, Callable[[], Any]]],
        iterations: int = 1,
    ) -> Union[float, Dict[str, float]]:
        """Time ``target`` and return milliseconds.

        * Single callable → returns ``float`` milliseconds.
        * Mapping ``{name: callable}`` → returns ``{name: float}``.
        * ``iterations > 1`` → reports the *average* per call, mirroring
          Laravel's overload that runs the callable N times.
        """
        if callable(target):
            return Benchmark._time_one(target, iterations)
        if isinstance(target, Mapping):
            return {
                name: Benchmark._time_one(callback, iterations)
                for name, callback in target.items()
            }
        raise TypeError(f"Benchmark.measure: unsupported target type {type(target)!r}")

    @staticmethod
    def value(callback: Callable[[], Any]) -> tuple:
        """Run ``callback`` once and return ``(value, elapsed_ms)`` — Laravel parity."""
        start = time.perf_counter()
        result = callback()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return result, elapsed_ms

    @staticmethod
    def dd(target: Union[Callable[[], Any], Mapping[str, Callable[[], Any]]]) -> None:
        """Measure, print formatted result, and exit (Laravel parity).

        ``dd`` = "dump and die". Useful for one-off profiling where
        you don't want the surrounding code to keep running after
        the measurement.
        """
        result = Benchmark.measure(target)
        if isinstance(result, dict):
            for name, ms in result.items():
                print(f"[BENCHMARK] {name}: {ms:.3f} ms")
        else:
            print(f"[BENCHMARK] {result:.3f} ms")
        raise SystemExit(0)

    @staticmethod
    def _time_one(callback: Callable[[], Any], iterations: int) -> float:
        if iterations <= 0:
            raise ValueError("iterations must be >= 1")
        start = time.perf_counter()
        for _ in range(iterations):
            callback()
        elapsed = (time.perf_counter() - start) * 1000.0
        return elapsed / iterations


__all__ = ["Benchmark"]
