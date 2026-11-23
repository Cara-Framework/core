"""String generators and helpers."""

from __future__ import annotations

import html
import re
import unicodedata
from typing import Any
from urllib.parse import urlparse, urlunparse

# --- Sanitization ---------------------------------------------------------
# These patterns target the "user-supplied free text → JSON → HTML context"
# pipeline. Reviews, comments, profile bios etc. should never carry markup
# through to a browser. We strip tags at input time as defense in depth —
# frontend escaping remains the primary protection.
_TAG_RE = re.compile(r"<[^>]+>")
# What a browser tokenizes as a tag: ``<`` followed by a letter, ``/``, ``!``
# or ``?``. Anything else after ``<`` is text ("1 < 2 and 3 > 2"). Used on
# text that was entity-DECODED by this module, where the caller never typed a
# bracket pair and must not lose the prose between two decoded comparisons.
_REAL_TAG_RE = re.compile(r"<[A-Za-z/!?][^>]*>")
# The start of such a tag, closed or not: an unterminated one at the end of
# the text is closed by the page's next ``>`` once embedded (``_strip_tag_spans``).
_TAG_OPENER_RE = re.compile(r"<[A-Za-z/!?]")
# Script/style bodies — strip tags AND contents since the content itself
# is dangerous (event handlers, inline JS). Matched by ``_strip_blocks``,
# which walks these two instead of one ``<name...>.*?</name>`` regex: that
# regex rescanned to the end of the text from every unclosed opener.
_BLOCK_NAMES = ("script", "style", "iframe", "object", "embed")
_BLOCK_OPEN_RE = re.compile(rf"<({'|'.join(_BLOCK_NAMES)})\b", re.IGNORECASE)
_BLOCK_CLOSE_RE = {
    name: re.compile(rf"</{name}\s*>", re.IGNORECASE) for name in _BLOCK_NAMES
}
# Entity layers decoded before the remainder is left encoded (inert as text).
# Real input is single- or double-encoded; an unbounded loop would let
# ``&amp;amp;amp;…lt;`` buy one full pass per four bytes.
_MAX_MARKUP_PASSES = 5
# Control characters except \t \n \r — these are never legitimate in text
# input and often used to evade filters.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Whitespace normalization — collapse runs of whitespace into single space
# but preserve paragraph breaks (double newlines → newline).
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
# C1 controls (U+0080-U+009F) are as illegitimate in text as C0 ones.
_C1_CONTROL_CHARS_RE = re.compile(r"[\x80-\x9f]")
# Characters outside category Cf that still render as nothing: the Hangul
# fillers and the braille blank, the usual padding of an "empty" name.
_BLANK_LOOKALIKES = frozenset("\u115f\u1160\u3164\uffa0\u2800")
# ZWNJ / ZWJ: invisible on their own, but they shape Persian and Indic text
# and glue emoji sequences (woman + ZWJ + laptop), so they survive between two visible
# non-ASCII characters.
_JOINERS = frozenset("\u200c\u200d")
# A subdivision flag (🏴 + tag letters + cancel tag) is the one legitimate
# use of the Cf tag block.
_FLAG_BASE = "\U0001f3f4"

_LOG_SECRET_NAME = (
    r"(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|"
    r"api[_-]?key|apikey|client[_-]?secret|secret|password|passwd|"
    r"signature|x-amz-signature|x-amz-security-token)"
)
# A QUOTED value runs to its closing quote (or to the end of a truncated
# line): the bare-token form stopped at the first space or ``,;&#}``, and a
# quoted secret holding one of those then failed the closing-quote check and
# was not redacted AT ALL (``{"password": "correct horse"}``, ``"p@ss,word"``).
_LOG_SECRET_ASSIGNMENT_RE = re.compile(
    rf"(?i)([\"']?{_LOG_SECRET_NAME}[\"']?\s*[:=]\s*)"
    r"(\"(?:[^\"\\]|\\.)*(?:\"|\Z)|'(?:[^'\\]|\\.)*(?:'|\Z)|[^\"'\s,;&#}]+)"
)
# The header name and the scheme may each sit inside quotes: a logged header
# dict is ``{'Authorization': 'Bearer …'}``.
_LOG_AUTHORIZATION_RE = re.compile(
    r"(?i)(\b(?:authorization|proxy-authorization)[\"']?\s*[:=]\s*[\"']?"
    r"(?:bearer|basic)\s+)([^\s,;\"']+)"
)
# Any scheme, not only http(s): a DSN in a connection error
# (``postgresql://app:pw@db/app``, ``redis://:pw@cache:6379/0``,
# ``amqp://user:pw@broker/``) carries the same userinfo. The lookbehind makes
# a scheme start only at the head of its run, so a long token is scanned once.
_LOG_URL_CREDENTIALS_RE = re.compile(
    r"(?i)(?<![a-z0-9+.\-])([a-z][a-z0-9+.\-]*://)([^/@\s]+)@"
)


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

    Handles common Unicode transliterations (Turkish chars, accented
    letters); anything the explicit map misses is NFKD-folded to its ASCII
    base, so an accent is never simply deleted ("Škoda" → ``skoda``, not
    ``koda``). Non-alphanumeric characters become the separator.
    Leading/trailing separators and consecutive separators are removed.

    Returns empty string for empty/whitespace-only input.

    Args:
        text: input string.
        separator: character placed between word boundaries (default ``"-"``).
        max_length: optional hard cap on the returned slug length. When the
            slug would otherwise be longer, it is truncated at the last
            separator before ``max_length`` (so the cut lands on a word
            boundary rather than mid-word). Set to ``None`` to opt out.
            Callers persisting a slug SHOULD pass this, matching BOTH the
            column width and whatever cap the READ side applies. A writer
            allowed to mint slugs longer than the reader will accept
            produces rows that can never be looked up again — the column
            takes them, and every lookup misses.
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

    # Anything the explicit map doesn't cover is NFKD-decomposed and its
    # combining marks dropped, so an accented letter FOLDS to its ASCII
    # base instead of being deleted by the regex below. Deletion silently
    # corrupts tokens ("Nestlé" → "nestl", "Müller" → "mller"), which both
    # splits one entity across two slugs and lets distinct accented stems
    # collide after the accent is thrown away ("Máller"/"Müller" → "mller").
    # It also makes the same string in NFC and NFD hash to different slugs.
    # Characters with no ASCII base (CJK, emoji) still fall through to the
    # regex and become separators — a URL slug is ASCII by definition.
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )

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
        >>> mask_token("sample-token-value")
        'samp***alue'
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
    return _LOG_SECRET_ASSIGNMENT_RE.sub(_redact_assignment, text)


def _redact_assignment(match: re.Match[str]) -> str:
    """``name=<value>`` with the value replaced and its quotes kept."""
    value = match.group(2)
    quote = value[0] if value[0] in "\"'" else ""
    closing = quote if quote and len(value) > 1 and value.endswith(quote) else ""
    return f"{match.group(1)}{quote}[REDACTED]{closing}"


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
        parsed = urlparse(url)
        if parsed.username or parsed.password:
            # Replace netloc with masked credentials.
            host_port = parsed.hostname or ""
            if parsed.port:
                host_port = f"{host_port}:{parsed.port}"
            masked_netloc = f"***:***@{host_port}"
            return urlunparse(
                (
                    parsed.scheme,
                    masked_netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
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
    return _strip_markup(text, fold=False)


def _strip_tag_spans(text: str, pattern: re.Pattern[str]) -> str:
    """``pattern.sub("", text)`` for a ``<…>`` pattern, in linear time, and
    an unterminated tag at the end dropped.

    Past the last ``>`` nothing can match, yet the regex still walked to the
    end of the text from every ``<`` there: 80k unclosed ``<`` cost 2 s.

    That tail is not safe to keep as-is: ``hello <img src=x onerror=alert(1)``
    has no ``>``, but once embedded in a page the page's own next ``>`` closes
    it and the handler runs. So the tail is cut at its first real tag opener
    (``<`` + letter, ``/``, ``!`` or ``?``: what a browser tokenizes as a
    tag); a ``<`` that opens nothing (``a < b``, ``<3``) stays text. One
    forward search of the tail, so still linear.
    """
    last = text.rfind(">")
    head = pattern.sub("", text[: last + 1]) if last >= 0 else ""
    tail = text[last + 1 :]
    opener = _TAG_OPENER_RE.search(tail)
    if opener is not None:
        tail = tail[: opener.start()]
    return head + tail


def _strip_blocks(text: str) -> str:
    """Drop ``<script|style|iframe|object|embed …>…</same>`` with contents.

    The leftmost opener that has a closer wins, and the block ends at the
    FIRST closer after it. A name whose closer search failed once is never
    searched again — nothing later can find one either — so unclosed openers
    cost one scan in total, not one each.
    """
    if text.rfind(">") < 0:
        return text
    out: list[str] = []
    kept_from = 0
    search_from = 0
    tag_end = -1
    unclosed: set[str] = set()
    while True:
        opener = _BLOCK_OPEN_RE.search(text, search_from)
        if opener is None:
            break
        # Openers nested in one unclosed tag share its ``>``: look once.
        if tag_end < opener.end():
            tag_end = text.find(">", opener.end())
            if tag_end < 0:
                break
        name = opener.group(1).lower()
        closer = (
            None if name in unclosed else _BLOCK_CLOSE_RE[name].search(text, tag_end + 1)
        )
        if closer is None:
            unclosed.add(name)
            search_from = opener.start() + 1
            continue
        out.append(text[kept_from : opener.start()])
        kept_from = search_from = closer.end()
    out.append(text[kept_from:])
    return "".join(out)


def _fold_text(text: str) -> str:
    """NFKC, invisible and control characters dropped — what a reader sees."""
    text = unicodedata.normalize("NFKC", text)
    text = strip_invisible(text)
    text = _CONTROL_CHARS_RE.sub("", text)
    return _C1_CONTROL_CHARS_RE.sub("", text)


def _strip_markup(text: str, *, fold: bool) -> str:
    """Strip markup, decode entities, and strip what the decoding exposed.

    Decoding used to be the LAST step, so ``&lt;script&gt;`` went in as text
    and came out as a live ``<script>``; with ``fold`` (``sanitize_text``) the
    NFKC fold ran after the strip too, so fullwidth ``＜script＞`` did the
    same. Each pass now folds, strips, then decodes, and repeats while the
    decode changed anything. The first pass strips every ``<…>`` (what the
    caller typed, unchanged behaviour); later passes strip only what a browser
    reads as a tag, so decoded prose such as ``1 &lt; 2 and 3 &gt; 2`` keeps
    its middle.
    """
    out = text
    for depth in range(_MAX_MARKUP_PASSES):
        if fold:
            out = _fold_text(out)
        out = _strip_tag_spans(_strip_blocks(out), _REAL_TAG_RE if depth else _TAG_RE)
        # Decode any HTML entities that were smuggled in, so the storage is
        # canonicalized and downstream escaping only happens once.
        decoded = html.unescape(out)
        if decoded == out:
            return out
        out = decoded
    # Encoded deeper than any real text: strip what the last decode exposed
    # and leave the rest encoded, where it is inert.
    if fold:
        out = _fold_text(out)
    return _strip_tag_spans(_strip_blocks(out), _REAL_TAG_RE)


def _is_tag(ch: str) -> bool:
    return 0xE0020 <= ord(ch) <= 0xE007F


def _joinable(ch: str) -> bool:
    return (
        ord(ch) > 0x7F
        and not ch.isspace()
        and ch not in _BLANK_LOOKALIKES
        and unicodedata.category(ch) not in ("Cf", "Cc")
    )


def strip_invisible(text: str) -> str:
    """Drop the characters that render as nothing or reorder what is shown.

    Every format character (Unicode ``Cf``: zero-width spaces, the BOM, soft
    hyphens, and the bidi embeddings, overrides and isolates that make
    ``"evil\u202etxt.exe"`` display backwards) and the blank look-alikes
    (Hangul fillers, braille blank). Kept: ZWNJ / ZWJ between two visible
    non-ASCII characters (script shaping, emoji sequences) and the tag
    characters of a subdivision flag. Without this a name made of nothing
    visible passes every length rule and spreads to every roster.
    """
    if not text:
        return ""
    out: list[str] = []
    last = len(text) - 1
    for index, ch in enumerate(text):
        if ch in _BLANK_LOOKALIKES:
            continue
        if unicodedata.category(ch) != "Cf":
            out.append(ch)
        elif ch in _JOINERS:
            if out and index < last and _joinable(out[-1]) and _joinable(text[index + 1]):
                out.append(ch)
        elif _is_tag(ch) and out and (out[-1] == _FLAG_BASE or _is_tag(out[-1])):
            out.append(ch)
    return "".join(out)


def clean_line(text: Any, max_length: int = 0) -> str:
    """A SINGLE-LINE field (a name, a label, an address line) as stored.

    NFKC-normalized, invisible and control characters dropped
    (``strip_invisible``), every line break (CR, LF, NEL, U+2028/2029) and
    other whitespace run folded to one space, ends trimmed, truncated to
    ``max_length`` (>0). A length rule run AFTER this judges what a reader
    sees. Markup is not touched — escaping on output stays the guard.
    """
    if text is None:
        return ""
    s = unicodedata.normalize("NFKC", str(text))
    s = strip_invisible(s)
    s = _CONTROL_CHARS_RE.sub(" ", s)
    s = _C1_CONTROL_CHARS_RE.sub(lambda match: " " if match.group() == "\x85" else "", s)
    s = " ".join(s.split())
    if max_length and len(s) > max_length:
        s = s[:max_length].rstrip()
    return s


def sanitize_text(text: Any, max_length: int = 0) -> str:
    """Sanitize user-supplied free text for safe storage.

    Guarantees:
      - No HTML tags (content of script/style blocks dropped too).
      - No HTML entities (already unescaped).
      - No control chars other than tab/newline/CR.
      - Unicode NFKC-normalized, invisible format characters dropped
        (``strip_invisible``: zero-width, bidi controls, blank fillers).
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
    s = _fold_text(_strip_markup(s, fold=True))
    s = _MULTI_SPACE_RE.sub(" ", s)
    s = _MULTI_NEWLINE_RE.sub("\n\n", s)
    s = s.strip()
    if max_length and len(s) > max_length:
        s = s[:max_length].rstrip()
    return s
