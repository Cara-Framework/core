"""``LogHttpRequests`` must sanitize control characters from request path.

Bug surface
~~~~~~~~~~~
``request.path`` comes from ``scope["path"]``, which the ASGI runner
URL-DECODES per the spec. So a malicious URL like
``/api/products/foo%0A%5B31mFAKE``  reaches the middleware as
``/api/products/foo\n[31mFAKE`` — literal newline + ANSI escape
already substituted in.

The middleware then writes ``f"... {method} {path} ..."`` straight
into ``Log.info``. Two attacks ride on that:

1. **Line injection / log forging.** A ``\n`` in the path splits
   one log entry across two lines. A log aggregator (Datadog,
   Loki, journald, plain grep) parses the synthetic second line
   as an independent entry; an attacker can plant fake "successful
   admin action" log lines underneath their own request.
2. **ANSI escape injection.** ``\x1b[31m`` and similar sequences
   are honored by terminal log viewers and by colored log file
   pagers (``less -R``). The attacker can mask their own log line,
   re-colour adjacent entries, or write ``\x1b[2K`` to erase the
   prior line entirely.

Both attacks land in **two** code paths in ``LogHttpRequests.handle``:
the success-path message at line 140 and the exception-path message
at line 117. The headers debug dump at line 158-162 has a similar
exposure via ``User-Agent`` (attacker-controlled).

Fix: %-encode every C0 / C1 control character (``\x00..\x1f``,
``\x7f..\x9f``) before writing the path to logs. Preserves the
URL-style "looks like an encoded escape" rendering so the entry
stays human-readable but no character can break line structure or
inject terminal sequences.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


# ── Helpers ──────────────────────────────────────────────────────


def _sanitize_via_module(value: str) -> str:
    """Reach into the LogHttpRequests module's path-sanitizer helper.

    The helper is added alongside the fix. Importing through the
    module path keeps the test reach-stable across future helper
    renames within the same file."""
    import importlib

    mod = importlib.import_module("cara.middleware.http.LogHttpRequests")
    sanitize = getattr(mod, "_sanitize_log_path", None)
    if sanitize is None:
        # Pre-fix: helper doesn't exist yet → test runs against the
        # identity function so the assertions below fail loudly.
        return value
    return sanitize(value)


# ── Control-character sanitization ──────────────────────────────


class TestPathControlCharsSanitized:
    @pytest.mark.parametrize("raw,expected_marker", [
        # Newline → %0A so the log entry stays single-line.
        ("/api/products/foo\nbar", "%0A"),
        ("/api/products/foo\r\nbar", "%0D"),
        # ANSI escape → %1B so terminal viewers don't honour the
        # sequence as a real control code.
        ("/api/products/\x1b[31mFAKE", "%1B"),
        # Tab → %09 (less critical but still breaks structured
        # tab-separated log formats).
        ("/api/products/foo\tbar", "%09"),
        # NUL → %00 (psycopg2 rejects NUL in any string literal, but
        # the log middleware shouldn't be the failing layer).
        ("/api/products/foo\x00bar", "%00"),
        # C1 controls (0x80-0x9F) — rarely real but should still be
        # escaped (some terminals interpret them).
        ("/api/products/foo\x9bbar", "%9B"),
    ])
    def test_control_chars_get_percent_encoded(
        self, raw: str, expected_marker: str,
    ) -> None:
        sanitized = _sanitize_via_module(raw)
        # The marker must appear at the position of the control char.
        assert expected_marker in sanitized, (
            f"sanitizer left {raw!r} as-is — expected to find "
            f"{expected_marker!r} in the result. The raw control char "
            f"in {raw!r} can split log lines (newlines) or inject "
            f"ANSI sequences (\\x1b) into terminal log viewers."
        )
        # And no raw control characters survive.
        assert not any(
            ord(c) < 0x20 or 0x7f <= ord(c) <= 0x9f
            for c in sanitized
        ), (
            f"sanitized output {sanitized!r} still contains a control "
            f"character; the percent-encoding step is incomplete."
        )

    def test_safe_ascii_path_passes_through_unchanged(self) -> None:
        """Regression marker: a normal path must round-trip verbatim.
        The sanitizer must not over-fire on legitimate URL slashes,
        hyphens, dots, query separators, etc."""
        path = "/api/products/macbook-air-2024?sort=price_asc&limit=20"
        assert _sanitize_via_module(path) == path

    def test_unicode_chars_preserved(self) -> None:
        """Real Turkish / non-Latin URLs (rare in practice but
        possible via path-encoded slugs) must survive — they're not
        control characters and not log-injection vectors."""
        path = "/c/şarjlı-aletler"
        assert _sanitize_via_module(path) == path

    def test_already_percent_encoded_passes_through(self) -> None:
        """A URL where the client did the percent-encoding themselves
        (no decoded control chars in scope[path]) must round-trip
        unchanged — the sanitizer is a post-decode safety net, not
        a re-encoder."""
        path = "/api/products/foo%20bar"
        assert _sanitize_via_module(path) == path


# ── Empty / None inputs ─────────────────────────────────────────


class TestEdgeCases:
    def test_empty_string_returns_empty(self) -> None:
        assert _sanitize_via_module("") == ""

    def test_only_control_chars(self) -> None:
        """Adversarial: path consisting entirely of control chars
        becomes a string of percent-escapes. Length grows by 3x but
        no characters break the log line."""
        sanitized = _sanitize_via_module("\n\r\t\x1b")
        assert "\n" not in sanitized
        assert "\r" not in sanitized
        assert "\t" not in sanitized
        assert "\x1b" not in sanitized
        assert sanitized == "%0A%0D%09%1B"
