"""HttpTimeoutAudit and NestedExceptShadowAudit."""

from __future__ import annotations

from cara.testing.audits import HttpTimeoutAudit, NestedExceptShadowAudit


def _timeouts(source: str) -> list[str]:
    return [f.message for f in HttpTimeoutAudit().scan_sources([(source, "m.py")])]


def test_timeout_audit_sees_aliases_sessions_and_thread_wrappers():
    found = _timeouts(
        "import asyncio as aio\n"
        "import requests as rq\n"
        "from requests import post, Session\n"
        "from httpx import AsyncClient as Client\n"
        "rq.get('https://example.invalid')\n"
        "post('https://example.invalid')\n"
        "Client()\n"
        "aio.to_thread(rq.post, 'https://example.invalid')\n"
        "s = Session()\n"
        "s.get('https://example.invalid')\n"
    )
    assert found == [
        "requests.get lacks timeout=",
        "requests.post lacks timeout=",
        "httpx.Client lacks timeout=",
        "asyncio.to_thread(requests.post) lacks timeout=",
        "s.get lacks timeout=",
    ]


def test_timeout_audit_accepts_bounded_calls_and_ignores_lookalikes():
    assert not _timeouts(
        "import httpx, requests\n"
        "requests.get('u', timeout=5)\n"
        "httpx.AsyncClient(timeout=httpx.Timeout(3.0))\n"
        "cache.get('key')\n"  # not an HTTP library
        "client.get('/x')\n"  # a client's timeout lives on its constructor
    )


def test_timeout_audit_scans_a_tree(tmp_path):
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "Fetch.py").write_text("import requests\nrequests.get('u')\n")
    findings = HttpTimeoutAudit().scan_tree(tmp_path, ["app", "missing"])
    assert [str(f) for f in findings] == ["app/Fetch.py:2: requests.get lacks timeout="]
    assert "timeout=" in HttpTimeoutAudit.report(findings)


def test_nested_handler_rebinding_the_outer_name_is_found():
    source = (
        "try:\n"
        "    work()\n"
        "except Exception as error:\n"
        "    try:\n"
        "        recover()\n"
        "    except RuntimeError as error:\n"
        "        pass\n"
        "    consume(error)\n"
    )
    findings = NestedExceptShadowAudit().scan_sources([(source, "m.py")])
    assert [(f.line, f.message) for f in findings] == [
        (6, "nested except rebinds 'error'")
    ]


def test_distinct_names_and_nested_scopes_are_fine():
    source = (
        "try:\n"
        "    work()\n"
        "except Exception as error:\n"
        "    try:\n"
        "        recover()\n"
        "    except RuntimeError as inner:\n"
        "        pass\n"
        "    def later():\n"
        "        try:\n"
        "            x()\n"
        "        except Exception as error:\n"
        "            pass\n"
        "try:\n"
        "    y()\n"
        "except Exception as error:\n"
        "    pass\n"
    )
    assert NestedExceptShadowAudit().scan_sources([(source, "m.py")]) == []
