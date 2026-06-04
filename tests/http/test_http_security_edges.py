"""HTTP response + upload security edge cases.

Three independent guards landed together — each tackles a class of
attack the framework left open at the boundary:

1. **Location header CR/LF (response splitting)**
   ``HeaderManager.location("https://evil.com/\\r\\nSet-Cookie: hi")``
   would write the literal bytes through to the wire, and the next
   header parser (browser, intermediary cache) would treat the
   bytes after ``\\n`` as additional headers — classic
   response-splitting / header-injection. The guard rejects
   CR/LF in the URL with a clear ValueError so the bug surfaces
   at the call site (where it can be fixed) instead of as a
   poisoned cookie at the client.

2. **UploadedFile null-byte filename**
   Python's ``open(path, ...)`` silently truncates the filename
   at the first NUL byte. An attacker who controls part of the
   uploaded filename smuggles ``innocent.txt\\x00.php`` — the
   extension-allowlist sees ``.php`` and rejects (or accepts on
   the wrong allowlist), but the file written to disk lands as
   ``innocent.txt``. The two layers disagree about what just
   landed; classic null-byte bypass. Same defence applied to the
   ``directory`` argument.

3. **SSE multi-line data field (RFC 8030 §8.3)**
   The formatter emitted a SINGLE ``data: `` prefix even when
   the payload contained embedded ``\\n`` (very common with JSON
   pretty-printing or multi-line markdown). The browser's SSE
   parser joins consecutive ``data:`` lines with ``\\n`` to
   reconstruct the value — a line without the prefix is treated
   as a malformed field and ignored, so the event was either
   truncated to its first line or dropped entirely. The fix
   splits on ``\\n`` and prefixes every line.

The four older Py2-style ``except E1, E2:`` clauses that the broader
regex sweep missed (different file paths, dotted exception names
that escaped the prior ``[A-Z]``-anchored grep) are not unit-tested
here — they're mechanical paren-adds, covered by the existing
import-time + module-load tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cara.exceptions import BadRequestException


# ── Location CR/LF (response splitting) ─────────────────────────────


class TestLocationCRLF:
    def setup_method(self):
        from cara.http.response.HeaderManager import HeaderManager

        # HeaderManager wraps a list-of-tuples header store; pass a
        # MagicMock with the same surface so we don't drag in the
        # full Response constructor here.
        self.mgr = HeaderManager(MagicMock())

    def test_clean_url_passes(self):
        self.mgr.location("https://example.com/next")
        # No raise; sanity that the normal path works.

    @pytest.mark.parametrize(
        "evil",
        [
            "https://example.com/\r\nSet-Cookie: poisoned=1",
            "https://example.com/\nX-Injected: yes",
            "https://example.com/\rX-Injected: yes",
            "https://example.com/foo\r\n",
            "/path\nGET /admin HTTP/1.1",  # request-line smuggle into intermediary
        ],
    )
    def test_crlf_in_url_raises_value_error(self, evil):
        # Response-splitting protection: the bytes after CR/LF would
        # be parsed by the next header consumer as additional headers,
        # poisoning cookies / Content-Type / cached responses. Pin
        # that the boundary refuses the URL outright (with a clear
        # ValueError the caller can catch + 400 the request).
        with pytest.raises(ValueError, match="CR or LF"):
            self.mgr.location(evil)

    def test_non_string_url_coerced_then_checked(self):
        # If a caller passes a stringifiable object (URL helper, etc.)
        # the str() conversion happens before the CR/LF check so the
        # check still runs.
        class _StrPayload:
            def __str__(self):
                return "https://x/\r\ninjected"

        with pytest.raises(ValueError):
            self.mgr.location(_StrPayload())  # type: ignore[arg-type]


# ── UploadedFile null-byte filename ─────────────────────────────────


class TestUploadedFileNullByte:
    def _file(self):
        # Minimal UploadedFile clone — UploadedFile.__init__ pulls in
        # framework facades we don't need for the null-byte guard.
        from cara.http.request.UploadedFile import UploadedFile

        f = UploadedFile.__new__(UploadedFile)
        f.content = b"payload bytes"
        f.filename = "ignored-by-test"
        return f

    @pytest.mark.parametrize(
        "bad_filename",
        [
            "innocent.txt\x00.php",  # extension smuggle
            "\x00malicious",  # leading null
            "trailing\x00",  # trailing null
            "mid\x00dle.txt",  # mid-string null
        ],
    )
    def test_null_byte_in_filename_raises(self, bad_filename, tmp_path, monkeypatch):
        # ``cara.support`` re-exports ``paths`` as a function name,
        # shadowing the submodule, so we go through ``sys.modules``
        # to grab the actual submodule for the monkeypatch.
        import sys

        paths_module = sys.modules["cara.support.paths"]
        monkeypatch.setattr(paths_module, "paths", lambda _="": str(tmp_path))

        with pytest.raises(BadRequestException, match="null byte"):
            self._file()._store_file("uploads", bad_filename)

    @pytest.mark.parametrize(
        "bad_dir",
        [
            "uploads\x00",
            "\x00escape",
            "up\x00loads",
        ],
    )
    def test_null_byte_in_directory_raises(self, bad_dir, tmp_path, monkeypatch):
        # ``cara.support`` re-exports ``paths`` as a function name,
        # shadowing the submodule, so we go through ``sys.modules``
        # to grab the actual submodule for the monkeypatch.
        import sys

        paths_module = sys.modules["cara.support.paths"]
        monkeypatch.setattr(paths_module, "paths", lambda _="": str(tmp_path))

        with pytest.raises(BadRequestException, match="null byte"):
            self._file()._store_file(bad_dir, "ok.txt")

    def test_clean_filename_writes_normally(self, tmp_path, monkeypatch):
        # ``cara.support`` re-exports ``paths`` as a function name,
        # shadowing the submodule, so we go through ``sys.modules``
        # to grab the actual submodule for the monkeypatch.
        import sys

        paths_module = sys.modules["cara.support.paths"]
        monkeypatch.setattr(paths_module, "paths", lambda _="": str(tmp_path))

        result = self._file()._store_file("uploads", "receipt.pdf")
        assert result == "uploads/receipt.pdf"
        written = tmp_path / "uploads" / "receipt.pdf"
        assert written.exists()
        assert written.read_bytes() == b"payload bytes"


# ── SSE multi-line data formatting (RFC 8030 §8.3) ──────────────────


class TestSSEMultilineData:
    def _format(self, event):
        # The formatter is a private helper on StreamingResponse.
        # Bypass __init__ — we only need the static-ish formatter
        # (it reads no instance state past ``self``).
        from cara.http.response.StreamingResponse import StreamingResponse

        sr = StreamingResponse.__new__(StreamingResponse)
        return sr._format_sse_event(event)

    def test_single_line_data_unchanged(self):
        out = self._format({"data": "hello"})
        assert "data: hello" in out
        # Sanity — single-line case wasn't broken by the multi-line fix.
        assert out.count("data:") == 1

    def test_multi_line_data_string_gets_per_line_prefix(self):
        # Pre-fix: emitted ``data: line1\nline2`` — browser SSE parser
        # reads ``line2`` as a malformed field with no leading ``data:``
        # and drops it, truncating the event.
        # Post-fix: each line carries its own ``data:`` prefix so the
        # parser rejoins them with ``\n`` into the original value.
        out = self._format({"data": "line1\nline2\nline3"})
        assert "data: line1" in out
        assert "data: line2" in out
        assert "data: line3" in out
        # Exactly three data lines (no bare-text leftovers).
        assert out.count("data:") == 3

    def test_pretty_printed_dict_data_each_line_prefixed(self):
        # The realistic multi-line shape is a string the CALLER built
        # with embedded newlines (markdown body, log excerpt, etc).
        # ``json.dumps`` of a flat dict ESCAPES ``\n`` to ``\\n``, so
        # a plain dict payload stays single-line — but a string
        # payload with real ``\n``s, or any dict the caller passed
        # to a multi-line serializer (``indent=2``), surfaces real
        # newlines that must each get a ``data:`` prefix.
        markdown_body = "# title\n\n- bullet 1\n- bullet 2"
        out = self._format({"data": markdown_body})
        data_lines = [ln for ln in out.split("\n") if ln.startswith("data:")]
        assert len(data_lines) == 4, (
            f"Multi-line string data payload must emit ONE ``data:`` "
            f"line per source line (RFC 8030 §8.3); got: {out!r}"
        )
        # Empty middle line — markdown often has blank lines — must
        # also carry the prefix or the parser truncates the event.
        assert any(ln == "data: " for ln in data_lines), (
            f"Blank source line lost its ``data:`` prefix; got: {data_lines!r}"
        )

    def test_id_and_event_fields_still_emit_once(self):
        out = self._format({"id": "42", "event": "ping", "data": "hi"})
        # Sanity that the multi-line fix didn't accidentally duplicate
        # the single-occurrence fields.
        assert out.count("id:") == 1
        assert out.count("event:") == 1
