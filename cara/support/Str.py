"""String generators and helpers."""

from __future__ import annotations

import html
import re
import unicodedata
from typing import Any

# --- Sanitization ---------------------------------------------------------
# These patterns target the "user-supplied free text → JSON → HTML context"
# pipeline. Reviews, comments, profile bios etc. should never carry markup
# through to a browser. We strip tags at input time as defense in depth —
# frontend escaping remains the primary protection.
_TAG_RE = re.compile(r"<[^>]+>")
# Script/style bodies — strip tags AND contents since the content itself
# is dangerous (event handlers, inline JS).
_SCRIPT_BLOCK_RE = re.compile(
    r"<(script|style|iframe|object|embed)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)
# Control characters except \t \n \r — these are never legitimate in text
# input and often used to evade filters.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Whitespace normalization — collapse runs of whitespace into single space
# but preserve paragraph breaks (double newlines → newline).
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")

_LOG_SECRET_NAME = (
    r"(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|"
    r"api[_-]?key|apikey|client[_-]?secret|secret|password|passwd|"
    r"signature|x-amz-signature|x-amz-security-token)"
)
_LOG_SECRET_ASSIGNMENT_RE = re.compile(
    rf"(?i)([\"']?{_LOG_SECRET_NAME}[\"']?\s*[:=]\s*)([\"']?)"
    r"([^\"'\s,;&#}]+)(\2)"
)
_LOG_AUTHORIZATION_RE = re.compile(
    r"(?i)(\b(?:authorization|proxy-authorization)\s*[:=]\s*"
    r"(?:bearer|basic)\s+)([^\s,;]+)"
)
_LOG_URL_CREDENTIALS_RE = re.compile(r"(?i)(https?://)([^/@\s]+)@")


def modularize(file_path, suffix=".py"):
    """
    Transforms a file path to a dotted path. On UNIX paths contains / and on Windows \\.

    Keyword Arguments:
        file_path {str} -- A file path such app/controllers

    Returns:
        value {str} -- a dotted path such as app.controllers
    """
    # if the file had the .py extension remove it as it's not needed for a module
    return removesuffix(
        file_path.replace("/", ".").replace("\\", "."),
        suffix,
    )


def as_filepath(dotted_path):
    """
    Inverse of modularize, transforms a dotted path to a file path (with /).

    Keyword Arguments:
        dotted_path {str} -- A dotted path such app.controllers

    Returns:
        value {str} -- a file path such as app/controllers
    """
    return dotted_path.replace(".", "/")


def removesuffix(string, suffix):
    """Implementation of str.removesuffix() function available for Python versions lower than
    3.9."""
    if suffix and string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        return string


def slugify(
    text: str,
    separator: str = "-",
    *,
    max_length: int | None = None,
) -> str:
    """Convert a string to a URL-friendly slug.

    Handles common Unicode transliterations (Turkish chars, accented letters).
    Non-alphanumeric characters become the separator. Leading/trailing
    separators and consecutive separators are removed.

    Returns empty string for empty/whitespace-only input.

    Args:
        text: input string.
        separator: character placed between word boundaries (default ``"-"``).
        max_length: optional hard cap on the returned slug length. When the
            slug would otherwise be longer, it is truncated at the last
            separator before ``max_length`` (so the cut lands on a word
            boundary rather than mid-word). Set to ``None`` to opt out.
            Callers persisting to a slug column (product/brand/category)
            SHOULD pass this matching the column width / read-side cap.
            Pre-fix the consolidator wrote 187-char slugs into a
            ``varchar(500)`` column while the API's ``SlugParser`` capped
            reads at 255 — a slug in (255, 500] was unreachable.
    """
    if not text or text.isspace():
        return ""

    # Common character transliterations
    char_map = {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "ş": "s",
        "ö": "o",
        "ü": "u",
        "Ç": "C",
        "Ğ": "G",
        "İ": "I",
        "Ş": "S",
        "Ö": "O",
        "Ü": "U",
        "à": "a",
        "á": "a",
        "â": "a",
        "ã": "a",
        "ä": "a",
        "è": "e",
        "é": "e",
        "ê": "e",
        "ë": "e",
        "ì": "i",
        "í": "i",
        "î": "i",
        "ï": "i",
        "ò": "o",
        "ó": "o",
        "ô": "o",
        "õ": "o",
        "ù": "u",
        "ú": "u",
        "û": "u",
        "ñ": "n",
        "ß": "ss",
    }
    for char, replacement in char_map.items():
        text = text.replace(char, replacement)

    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", separator, text)
    text = re.sub(rf"{re.escape(separator)}+", separator, text)
    text = text.strip(separator)

    if max_length is not None and max_length > 0 and len(text) > max_length:
        # Truncate at the last separator before ``max_length`` so the cut
        # lands on a word boundary. If no separator exists in the head
        # (rare — single very long token), fall back to a hard slice.
        head = text[:max_length]
        cut = head.rfind(separator)
        text = (head[:cut] if cut > 0 else head).rstrip(separator)

    return text


def email_mask(email: str) -> str:
    """Mask the local part of an email address for privacy.

    Used for PII redaction in logs / public surfaces (notification
    digests, exposed audit trails) where the user's identity should
    be partially obscured but the domain stays visible. Generic — the
    framework owns it because every app eventually needs the same
    "show ``j****@example.com`` not ``john@example.com``" rendering.

    Args:
        email: Raw email address. Empty / ``None`` / strings without
            ``@`` return ``""`` so callers can chain without guards.

    Returns:
        ``"<local-mask>@<domain>"``. Local parts of length ≤ 2 are
        fully masked (no leak of first letter); longer locals show
        only the first character followed by six asterisks.

    Examples:
        >>> email_mask("john@example.com")
        'j******@example.com'
        >>> email_mask("ab@example.com")
        '**@example.com'
        >>> email_mask("")
        ''
    """
    if not email or "@" not in email:
        return ""
    local_part, domain = email.split("@", 1)
    if len(local_part) <= 2:
        masked_local = "*" * len(local_part)
    else:
        masked_local = f"{local_part[0]}******"
    return f"{masked_local}@{domain}"


def mask_token(token: str) -> str:
    """Mask a token/key/secret for safe log output.

    Shows only the first 4 and last 4 characters of long tokens;
    shorter values are fully masked.  Empty / ``None`` returns
    ``"***"``.

    Examples:
        >>> mask_token("sk_live_abc123xyz789")
        'sk_l***z789'
        >>> mask_token("short")
        '*****'
        >>> mask_token("")
        '***'
    """
    if not token:
        return "***"
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}***{token[-4:]}"


def redact_log_secrets(message: Any) -> str:
    """Remove credentials from arbitrary log messages.

    Third-party clients commonly log complete request URLs at INFO level.
    Query-string API keys therefore need redaction at the logging boundary,
    independent of each client's configuration. The replacement is total
    rather than partial so log aggregation can never become a credential
    store. URL hosts, paths and non-sensitive parameters remain available for
    debugging.
    """
    text = str(message)
    text = _LOG_URL_CREDENTIALS_RE.sub(r"\1[REDACTED]@", text)
    text = _LOG_AUTHORIZATION_RE.sub(r"\1[REDACTED]", text)
    return _LOG_SECRET_ASSIGNMENT_RE.sub(r"\1\2[REDACTED]\4", text)


def mask_ip(ip: str) -> str:
    """Partially mask an IP address for safe log output.

    IPv4: shows the first two octets, masks the rest
    (``192.168.x.x``).  IPv6: shows the first group, masks the
    rest.  Empty / invalid input returns ``"*.*.*.*"``.

    Examples:
        >>> mask_ip("192.168.1.42")
        '192.168.x.x'
        >>> mask_ip("2001:0db8:85a3::8a2e:0370:7334")
        '2001:****'
        >>> mask_ip("")
        '*.*.*.*'
    """
    if not ip:
        return "*.*.*.*"
    ip = ip.strip()
    if ":" in ip:
        # IPv6 — show only the first group.
        first = ip.split(":")[0]
        return f"{first}:****"
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.x.x"
    return "*.*.*.*"


def mask_proxy_url(url: str) -> str:
    """Strip credentials from a proxy URL for safe log output.

    Proxy URLs often contain ``user:pass@host:port``.  This keeps
    the scheme and host:port but replaces any embedded credentials
    with ``***:***``.

    Examples:
        >>> mask_proxy_url("http://user:pass@proxy.example.com:3128")
        'http://***:***@proxy.example.com:3128'
        >>> mask_proxy_url("http://proxy.example.com:3128")
        'http://proxy.example.com:3128'
        >>> mask_proxy_url("")
        '***'
    """
    if not url:
        return "***"
    try:
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(url)
        if parsed.username or parsed.password:
            # Replace netloc with masked credentials.
            host_port = parsed.hostname or ""
            if parsed.port:
                host_port = f"{host_port}:{parsed.port}"
            masked_netloc = f"***:***@{host_port}"
            return urlunparse(
                (parsed.scheme, masked_netloc, parsed.path,
                 parsed.params, parsed.query, parsed.fragment)
            )
        return url
    except Exception:
        return "***"


def strip_tags(text: str) -> str:
    """Strip HTML/XML tags and dangerous block contents from ``text``.

    Removes <script>/<style>/<iframe>/<object>/<embed> blocks entirely
    (tags + contents), then strips remaining tags. Safe for user-entered
    free text before it's stored or echoed back.
    """
    if not text:
        return ""
    out = _SCRIPT_BLOCK_RE.sub("", text)
    out = _TAG_RE.sub("", out)
    # Decode any HTML entities that were smuggled in, so the storage is
    # canonicalized and downstream escaping only happens once.
    out = html.unescape(out)
    return out


def sanitize_text(text: Any, max_length: int = 0) -> str:
    """Sanitize user-supplied free text for safe storage.

    Guarantees:
      - No HTML tags (content of script/style blocks dropped too).
      - No HTML entities (already unescaped).
      - No control chars other than tab/newline/CR.
      - Unicode NFKC-normalized (defeats look-alike/zero-width evasion).
      - Whitespace normalized: tabs → spaces, runs collapsed, 3+ blank
        lines clamped to 2, outer whitespace stripped.
      - Truncated to max_length (>0) if given.

    Returns empty string for empty/None input. Never returns None so
    callers can chain without guards.
    """
    if text is None:
        return ""
    s = str(text)
    if not s:
        return ""
    s = strip_tags(s)
    s = unicodedata.normalize("NFKC", s)
    s = _CONTROL_CHARS_RE.sub("", s)
    s = _MULTI_SPACE_RE.sub(" ", s)
    s = _MULTI_NEWLINE_RE.sub("\n\n", s)
    s = s.strip()
    if max_length and len(s) > max_length:
        s = s[:max_length].rstrip()
    return s
