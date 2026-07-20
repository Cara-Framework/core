"""HTTP client imports must not boot server-only body parsing."""

from __future__ import annotations

import subprocess
import sys


def test_http_client_import_does_not_load_request_stack() -> None:
    code = """
import sys
from cara.http.client.HttpClient import HttpFacade
assert HttpFacade is not None
assert 'cara.http.request.Request' not in sys.modules
assert 'cara.http.request.mixins.MakesBodyParsing' not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        check=False,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
