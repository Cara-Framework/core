from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_http_middleware_imports_without_database_extras() -> None:
    framework_root = Path(__file__).resolve().parents[2]
    program = """
import builtins

original_import = builtins.__import__

def reject_database_extras(name, *args, **kwargs):
    if name == "faker" or name.startswith("faker."):
        raise ImportError("database extras are intentionally unavailable")
    return original_import(name, *args, **kwargs)

builtins.__import__ = reject_database_extras
from cara.middleware.http import RecordRequestMetrics
assert RecordRequestMetrics.__name__ == "RecordRequestMetrics"
"""
    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=framework_root,
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
