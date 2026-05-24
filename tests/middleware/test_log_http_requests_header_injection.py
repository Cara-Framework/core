"""``_redact_headers`` must sanitize control chars in header values.

Sibling regression to ``test_log_http_requests_path_injection``,
same attack surface (newline / ANSI escape injection) but in the
headers debug dump at the bottom of ``LogHttpRequests.handle``.

Bug surface
~~~~~~~~~~~
The path-injection fix (task #24) covered the success-path message
and the exception-path message. But the debug-level headers dump
at line ~191-195 still walked raw header values verbatim:

    raw_headers = request.scope.get("headers", [])
    if raw_headers:
        safe_headers = _redact_headers(raw_headers)
        Log.debug(
            f"  Headers: {safe_headers}",
            category="cara.http.requests",
        )

``_redact_headers`` blocks SENSITIVE headers (Authorization,
Cookie, …) but for everything else just decodes the bytes verbatim.
``User-Agent`` is attacker-controlled — a UA like

    "Mozilla/5.0\nINFO admin_action_succeeded\n\x1b[31mFAILED"

would inject log lines and ANSI re-coloring into the f-string
output the same way the path bug did pre-fix.

Fix
~~~
Reuse ``_sanitize_log_path`` on every non-redacted header value
inside ``_redact_headers``. The helper percent-encodes C0/DEL/C1
control characters — same defense the path now uses. Redacted
header values stay ``[REDACTED]`` (no control chars to worry
about).
"""

from __future__ import annotations

from typing import Any


def _redact(headers: list[tuple[bytes, bytes]]) -> dict[str, Any]:
    import importlib

    mod = importlib.import_module("cara.middleware.http.LogHttpRequests")
    return mod._redact_headers(headers)


# ── Sensitive headers stay redacted (no behavior change) ───────────


class TestSensitiveHeadersStillRedacted:
    def test_authorization_value_is_redacted(self) -> None:
        out = _redact([(b"authorization", b"Bearer s3cr3t")])
        assert out["authorization"] == "[REDACTED]"

    def test_cookie_value_is_redacted(self) -> None:
        out = _redact([(b"cookie", b"session=abc123")])
        assert out["cookie"] == "[REDACTED]"

    def test_x_api_key_value_is_redacted(self) -> None:
        out = _redact([(b"x-api-key", b"key-12345")])
        assert out["x-api-key"] == "[REDACTED]"


# ── Non-sensitive headers have control chars sanitized ─────────────


class TestNonSensitiveHeaderValuesAreSanitized:
    def test_user_agent_newline_gets_percent_encoded(self) -> None:
        """The bug — a User-Agent with a real newline would split the
        Log.debug f-string output across two log lines, enabling
        log forging via UA spoofing."""
        ua = "Mozilla/5.0\nINFO admin_action_succeeded"
        out = _redact([(b"user-agent", ua.encode("latin-1"))])
        value = out["user-agent"]
        assert "\n" not in value, (
            f"User-Agent value still contains a literal newline: "
            f"{value!r}. The headers debug dump would render this as "
            f"a separate log line, allowing log forging via UA spoofing."
        )
        assert "%0A" in value

    def test_user_agent_ansi_escape_gets_percent_encoded(self) -> None:
        """ANSI escape sequences in UA → terminal log viewers honor
        them, allowing color-injection / cursor-manipulation attacks."""
        ua = "Mozilla/5.0\x1b[31mRED"
        out = _redact([(b"user-agent", ua.encode("latin-1"))])
        value = out["user-agent"]
        assert "\x1b" not in value, (
            f"User-Agent value still contains a raw ESC (0x1b): "
            f"{value!r}. Terminal log viewers (less -R, journalctl) "
            f"would honor this as a real ANSI sequence."
        )
        assert "%1B" in value

    def test_referer_with_carriage_return_sanitized(self) -> None:
        out = _redact([(b"referer", b"https://example.com\r\nFAKE")])
        value = out["referer"]
        assert "\r" not in value
        assert "\n" not in value
        assert "%0D" in value or "%0A" in value

    def test_normal_user_agent_passes_through_unchanged(self) -> None:
        """Regression marker — a normal UA must round-trip verbatim."""
        ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        out = _redact([(b"user-agent", ua.encode("latin-1"))])
        assert out["user-agent"] == ua

    def test_unicode_in_value_preserved(self) -> None:
        """Non-control Unicode (rare but possible in custom headers)
        must survive — only control chars get encoded."""
        # Unicode-bearing UA: still safe to log verbatim.
        ua = "MobileApp/1.0 (Şarjlı)"
        out = _redact([(b"x-app-id", ua.encode("latin-1", errors="replace"))])
        value = out["x-app-id"]
        # The latin-1 round-trip might mangle non-latin chars, but
        # whatever survives must not contain control bytes.
        assert not any(
            ord(c) < 0x20 or 0x7f <= ord(c) <= 0x9f for c in value
        )


# ── Combined: redacted sensitive + sanitized non-sensitive ─────────


class TestMixedHeaders:
    def test_authorization_redacted_alongside_sanitized_user_agent(self) -> None:
        ua = "Mozilla/5.0\nFAKE-LINE"
        out = _redact([
            (b"authorization", b"Bearer s3cr3t"),
            (b"user-agent", ua.encode("latin-1")),
            (b"accept", b"application/json"),
        ])
        # Sensitive: still REDACTED.
        assert out["authorization"] == "[REDACTED]"
        # Non-sensitive + injection-bearing: sanitized.
        assert "\n" not in out["user-agent"]
        assert "%0A" in out["user-agent"]
        # Non-sensitive + clean: untouched.
        assert out["accept"] == "application/json"
