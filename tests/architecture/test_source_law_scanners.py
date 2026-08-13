"""HttpInBusinessLogic, EnvReadDiscipline and SilentExceptSwallow.

Three small source laws that four product guard copies each carried their own
slightly different version of. The cases below pin the union the framework
now enforces — in particular the shapes only ONE of those copies caught.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.scanners.EnvReadDiscipline import EnvReadDiscipline
from cara.architecture.scanners.HttpInBusinessLogic import HttpInBusinessLogic
from cara.architecture.scanners.SilentExceptSwallow import SilentExceptSwallow

from ._fixtures import make_manifest, write


def _messages(scanner, root: Path, **overrides) -> list[str]:
    return [str(finding) for finding in scanner.scan(make_manifest(root, **overrides))]


# ── HTTP types stop at the edge ──────────────────────────────────────


def test_importing_an_http_type_into_business_logic_is_a_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "from cara.http.responses import JsonResponse\n\n\ndef run():\n    return JsonResponse({})\n",
    )

    assert any(
        "imports an HTTP type" in m for m in _messages(HttpInBusinessLogic, tmp_path)
    )


def test_a_plain_import_of_an_http_package_is_a_finding(tmp_path: Path) -> None:
    write(tmp_path / "app" / "services" / "Report.py", "import cara.request\n")

    assert _messages(HttpInBusinessLogic, tmp_path) != []


def test_calling_abort_in_business_logic_is_a_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "def run(ok):\n    if not ok:\n        abort(404)\n",
    )

    assert any("abort()" in m for m in _messages(HttpInBusinessLogic, tmp_path))


def test_a_domain_exception_is_the_sanctioned_shape(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "from app.exceptions import WidgetMissing\n"
        "\n"
        "\n"
        "def run(ok):\n"
        "    if not ok:\n"
        "        raise WidgetMissing()\n",
    )

    assert _messages(HttpInBusinessLogic, tmp_path) == []


# ── the environment is read in config/ only ──────────────────────────


def test_os_environ_and_os_getenv_are_both_caught(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "import os\n"
        "\n"
        "\n"
        "def run():\n"
        "    return os.getenv('A'), os.environ['B'], os.environ.get('C')\n",
    )

    messages = _messages(EnvReadDiscipline, tmp_path)

    assert any("os.getenv(...)" in m for m in messages)
    assert any("os.environ[...]" in m for m in messages)
    assert any("os.environ.get(...)" in m for m in messages)


def test_a_name_imported_from_os_cannot_slip_past(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "from os import getenv\n\n\ndef run():\n    return getenv('A')\n",
    )

    assert any("via `getenv`" in m for m in _messages(EnvReadDiscipline, tmp_path))


def test_importing_the_env_helper_is_itself_the_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "from cara.environment import env\n\n\ndef run():\n    return env('A')\n",
    )

    assert any(
        "imports from cara.environment" in m
        for m in _messages(EnvReadDiscipline, tmp_path)
    )


def test_snapshotting_the_whole_environment_is_composition_not_a_read(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "import os\n\n\ndef run():\n    return os.environ.copy()\n",
    )

    assert _messages(EnvReadDiscipline, tmp_path) == []


def test_the_exempt_attribute_set_is_manifest_data(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "import os\n\n\ndef run():\n    return os.environ.copy()\n",
    )

    assert (
        _messages(EnvReadDiscipline, tmp_path, env_read_exempt_environ_attrs=frozenset())
        != []
    )


# ── no broad except swallows a failure ───────────────────────────────

_BODY = (
    "def run(work):\n    try:\n        work()\n    except {caught}:\n        {handler}\n"
)


def test_a_bare_except_is_always_a_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "def run(work):\n"
        "    try:\n"
        "        work()\n"
        "    except:  # noqa: E722\n"
        "        raise RuntimeError('wrapped')\n",
    )

    # Reported even though it re-raises: a bare handler catches
    # KeyboardInterrupt and SystemExit too, so the shape is wrong regardless
    # of what the body does.
    assert any("bare `except:`" in m for m in _messages(SilentExceptSwallow, tmp_path))


def test_a_broad_except_inside_a_tuple_is_still_broad(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        _BODY.format(caught="(ValueError, Exception)", handler="pass"),
    )

    assert any(
        "swallows the failure" in m for m in _messages(SilentExceptSwallow, tmp_path)
    )


def test_a_narrow_typed_fallback_is_not_a_swallow(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        _BODY.format(caught="(ValueError, TypeError)", handler="return None"),
    )

    assert _messages(SilentExceptSwallow, tmp_path) == []


def test_exiting_the_block_silently_still_counts_as_swallowing(tmp_path: Path) -> None:
    for handler in ("continue", "break"):
        root = tmp_path / handler
        write(
            root / "app" / "services" / "Report.py",
            "def run(items, work):\n"
            "    for item in items:\n"
            "        try:\n"
            "            work(item)\n"
            "        except Exception:\n"
            f"            {handler}\n",
        )

        assert _messages(SilentExceptSwallow, root) != [], handler


def test_logging_or_re_raising_clears_the_handler(tmp_path: Path) -> None:
    for handler in ("logger.exception('failed')", "raise"):
        root = tmp_path / handler[:5]
        write(
            root / "app" / "services" / "Report.py",
            _BODY.format(caught="Exception", handler=handler),
        )

        assert _messages(SilentExceptSwallow, root) == [], handler


def test_a_comment_cannot_bypass_the_reporting_requirement(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "def run(items, work, failures):\n"
        "    for item in items:\n"
        "        try:\n"
        "            work(item)\n"
        "        except Exception:  # allow-silent-except: logged after the loop\n"
        "            continue\n",
    )

    assert any(
        "swallows the failure" in message
        for message in _messages(SilentExceptSwallow, tmp_path)
    )
