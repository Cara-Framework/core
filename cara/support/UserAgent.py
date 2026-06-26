"""User-Agent classification helpers for the API layer.

Used to filter bot traffic out of click/visit analytics and to decide
whether to deeplink mobile users straight into a native client app.

Empty UAs are treated as bots — every real browser sends a UA, and the
small minority of legitimate clients without one (privacy extensions,
custom apps) are not the target audience.
"""

from __future__ import annotations

# Lowercase substrings to match against the lower-cased UA. Update as
# new automation surfaces appear; keep entries short to maximise hit
# rate (e.g. "bot" catches Googlebot, MJ12bot, BingBot, …).
BOT_UA_NEEDLES: tuple = (
    # Generic crawlers
    "bot",
    "crawler",
    "spider",
    # Link-preview unfurlers
    "facebookexternalhit",
    "slackbot",
    "twitterbot",
    "linkedinbot",
    "discordbot",
    "whatsapp",
    "telegrambot",
    # Search engines (extra explicit on top of the "bot" catch-all)
    "googlebot",
    "bingbot",
    "duckduckbot",
    "yandex",
    "baiduspider",
    # SEO tooling
    "ahrefsbot",
    "semrushbot",
    "mj12bot",
    "dotbot",
    "petalbot",
    # Headless browsers / automation
    "headlesschrome",
    "phantomjs",
    "selenium",
    "puppeteer",
    "playwright",
    # HTTP clients
    "curl/",
    "wget/",
    "python-requests",
    "go-http-client",
    "httpclient",
    "okhttp",
    "axios/",
    "node-fetch",
)


def is_bot(user_agent: str | None) -> bool:
    """Return True when ``user_agent`` looks like an automated client.

    Empty / whitespace-only / missing UA is treated as a bot — every
    real browser sends a non-empty UA. Pre-fix the function only
    short-circuited on the falsy-empty case, so an attacker sending
    ``User-Agent: "   "`` (one or more spaces — easy to construct
    via ``curl -A " "``) slipped through the substring scan and the
    request was treated as a real user. Click analytics, request-log
    sampling, and history rollups would then capture the automated
    traffic as if it were human.
    """
    if not user_agent or not user_agent.strip():
        return True
    ua = user_agent.lower()
    return any(needle in ua for needle in BOT_UA_NEEDLES)


def looks_mobile(user_agent: str | None) -> bool:
    """Best-effort mobile detection used for deeplink routing.

    Returns False on empty UA — we don't deeplink for unknown clients.
    """
    if not user_agent:
        return False
    ua = user_agent.lower()
    return "iphone" in ua or "ipad" in ua or "android" in ua or "mobile safari" in ua


__all__ = ["BOT_UA_NEEDLES", "is_bot", "looks_mobile"]
