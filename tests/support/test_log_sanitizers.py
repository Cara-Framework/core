"""Tests for log-sanitization helpers in ``cara.support.Str``.

These functions mask PII / secrets before they reach log aggregation.
Every test verifies that the original sensitive value does NOT appear
in the output while useful debug context is preserved.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

# Import Str.py directly to sidestep the cara.support package init
# which pulls in heavy dependencies (dotty_dict, etc.) that are only
# available in the full venv.  Mirrors the pattern used by the
# services repo's ``test_str.py`` (``load_module`` helper).
_STR_PATH = Path(__file__).resolve().parents[2] / "cara" / "support" / "Str.py"
_spec = importlib.util.spec_from_file_location("cara.support.Str", _STR_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("cara.support.Str", _mod)
_spec.loader.exec_module(_mod)

email_mask = _mod.email_mask
mask_ip = _mod.mask_ip
mask_proxy_url = _mod.mask_proxy_url
mask_token = _mod.mask_token
redact_log_secrets = _mod.redact_log_secrets


# ── email_mask ──────────────────────────────────────────────────────


class TestEmailMask:
    def test_standard_address(self):
        result = email_mask("john@example.com")
        assert result == "j******@example.com"
        assert "john" not in result

    def test_short_local_part(self):
        result = email_mask("ab@example.com")
        assert result == "**@example.com"
        assert "ab" not in result.split("@")[0]

    def test_single_char_local(self):
        result = email_mask("a@x.com")
        assert result == "*@x.com"

    def test_empty_string(self):
        assert email_mask("") == ""

    def test_no_at_sign(self):
        assert email_mask("not-an-email") == ""

    def test_none_input(self):
        # The function signature says str, but callers may pass None.
        assert email_mask(None) == ""  # type: ignore[arg-type]

    def test_domain_preserved(self):
        result = email_mask("alice@company.co.uk")
        assert result.endswith("@company.co.uk")


# ── mask_token ──────────────────────────────────────────────────────


class TestMaskToken:
    def test_long_token(self):
        token = "sk_live_abc123xyz789"
        result = mask_token(token)
        assert result == "sk_l***z789"
        assert "abc123" not in result

    def test_short_token(self):
        result = mask_token("short")
        assert result == "*****"
        assert "short" not in result

    def test_exactly_eight_chars(self):
        result = mask_token("12345678")
        assert result == "********"

    def test_nine_chars_shows_edges(self):
        result = mask_token("123456789")
        assert result == "1234***6789"

    def test_empty(self):
        assert mask_token("") == "***"

    def test_none(self):
        assert mask_token(None) == "***"  # type: ignore[arg-type]


# ── mask_ip ─────────────────────────────────────────────────────────


class TestMaskIp:
    def test_ipv4_standard(self):
        result = mask_ip("192.168.1.42")
        assert result == "192.168.x.x"
        assert "1.42" not in result

    def test_ipv4_preserves_network(self):
        result = mask_ip("10.0.55.123")
        assert result.startswith("10.0.")
        assert result.endswith(".x")

    def test_ipv6(self):
        result = mask_ip("2001:0db8:85a3::8a2e:0370:7334")
        assert result == "2001:****"
        assert "0db8" not in result
        assert "7334" not in result

    def test_empty(self):
        assert mask_ip("") == "*.*.*.*"

    def test_none(self):
        assert mask_ip(None) == "*.*.*.*"  # type: ignore[arg-type]

    def test_whitespace_stripped(self):
        result = mask_ip("  192.168.1.42  ")
        assert result == "192.168.x.x"

    def test_invalid_format(self):
        assert mask_ip("not-an-ip") == "*.*.*.*"


# ── mask_proxy_url ──────────────────────────────────────────────────


class TestMaskProxyUrl:
    def test_url_with_credentials(self):
        result = mask_proxy_url("http://user:pass@proxy.example.com:3128")
        assert result == "http://***:***@proxy.example.com:3128"
        assert "user" not in result
        assert "pass" not in result

    def test_url_without_credentials(self):
        url = "http://proxy.example.com:3128"
        assert mask_proxy_url(url) == url

    def test_complex_password(self):
        result = mask_proxy_url("http://admin:s3cr3t!@p%40ssw0rd@host:8080")
        assert "s3cr3t" not in result
        assert "***:***@" in result

    def test_empty(self):
        assert mask_proxy_url("") == "***"

    def test_none(self):
        assert mask_proxy_url(None) == "***"  # type: ignore[arg-type]

    def test_host_and_port_preserved(self):
        result = mask_proxy_url("http://u:p@10.0.0.1:3128")
        assert "10.0.0.1:3128" in result

    def test_https_scheme_preserved(self):
        result = mask_proxy_url("https://u:p@secure.proxy.io:443")
        assert result.startswith("https://")
        assert "secure.proxy.io:443" in result


# ── Cross-cutting: no original value leaks ──────────────────────────


class TestNoLeaks:
    """Verify that the sensitive portion is never present in output."""

    def test_email_local_part_not_in_output(self):
        original = "verysecretname@example.com"
        masked = email_mask(original)
        # The full local part must not appear.
        assert "verysecretname" not in masked

    def test_token_middle_not_in_output(self):
        original = "ghp_A1B2C3D4E5F6G7H8I9"
        masked = mask_token(original)
        assert "C3D4E5" not in masked

    def test_full_ip_not_in_output(self):
        original = "203.0.113.195"
        masked = mask_ip(original)
        assert original not in masked

    def test_proxy_creds_not_in_output(self):
        original = "http://myuser:mypass@proxy.lan:3128"
        masked = mask_proxy_url(original)
        assert "myuser" not in masked
        assert "mypass" not in masked


class TestLogSecretRedaction:
    def test_httpx_request_url_query_token_is_fully_redacted(self):
        secret = "scrape-provider-secret-value"
        message = (
            "HTTP Request: GET https://api.example.test/?token="
            f"{secret}&url=https%3A%2F%2Fshop.example%2Fitem \"HTTP/2 200 OK\""
        )

        redacted = redact_log_secrets(message)

        assert secret not in redacted
        assert "token=[REDACTED]" in redacted
        assert "url=https%3A%2F%2Fshop.example%2Fitem" in redacted

    def test_json_and_header_credentials_are_redacted(self):
        message = (
            "payload={'api_key': 'key-secret'} "
            "Authorization: Bearer bearer-secret"
        )

        redacted = redact_log_secrets(message)

        assert "key-secret" not in redacted
        assert "bearer-secret" not in redacted
        assert redacted.count("[REDACTED]") == 2

    def test_url_userinfo_is_redacted_but_host_is_preserved(self):
        redacted = redact_log_secrets(
            "proxy=https://username:password@proxy.example.test:8443/path"
        )

        assert "username" not in redacted
        assert "password" not in redacted
        assert "https://[REDACTED]@proxy.example.test:8443/path" in redacted
