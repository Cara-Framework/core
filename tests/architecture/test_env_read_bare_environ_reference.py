"""EnvReadDiscipline flags a bare ``os.environ`` reference.

The live defect: only ``os.getenv(...)``, ``os.environ.<attr>(...)`` and
``os.environ[...]`` were findings. ``"X" in os.environ``, ``env =
os.environ`` or passing the mapping on each handed the code the whole
environment to read from and sidestepped the per-variable shapes. The copy
exemption still covers its two literal spellings, ``dict(os.environ)`` and
``{**os.environ, ...}``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.architecture.scanners.EnvReadDiscipline import EnvReadDiscipline

from ._fixtures import make_manifest, write


def _messages(root: Path, body: str) -> list[str]:
    write(root / "app" / "services" / "Report.py", f"import os\n\n\n{body}")
    return [str(f) for f in EnvReadDiscipline.scan(make_manifest(root))]


@pytest.mark.parametrize(
    "body",
    [
        "def run():\n    return 'X' in os.environ\n",
        "def run():\n    env = os.environ\n    return env\n",
        "def run(client):\n    return client.start(os.environ)\n",
    ],
)
def test_a_bare_os_environ_reference_is_a_finding(tmp_path: Path, body: str):
    messages = _messages(tmp_path, body)

    assert len(messages) == 1
    assert "os.environ referenced" in messages[0]


@pytest.mark.parametrize(
    "body",
    [
        "def run():\n    return dict(os.environ)\n",
        "def run():\n    return {**os.environ, 'TZ': 'UTC'}\n",
    ],
)
def test_a_whole_environment_copy_is_the_exempt_snapshot(tmp_path: Path, body: str):
    assert _messages(tmp_path, body) == []
