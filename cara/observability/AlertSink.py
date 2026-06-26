"""Alert sink — fan out critical operational alerts to PagerDuty + Slack.

Wraps both providers behind one ``AlertSink.fire(...)`` call so callers
(jobs, schedulers, health probes) don't have to know which channel
they're hitting. Both endpoints are best-effort; a failure on one
provider never blocks the other or the calling job.

Configuration keys (all optional — empty = skip provider):
    notifications.alerts_pagerduty_routing_key
    notifications.alerts_pagerduty_dedup_prefix
    notifications.alerts_slack_webhook_url
    notifications.alerts_source_hostname

Usage::

    from cara.observability import AlertSink

    AlertSink.fire(
        severity="critical",
        title="Error rate >40%",
        body="Last 5 min: 412/700 failed.",
        dedup_key="error_rate",
        context={"failure_rate": 0.41},
    )
"""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from cara.configuration import config
from cara.facades import Log


class AlertSink:
    """Critical-alert fan-out across PagerDuty + Slack."""

    HTTP_TIMEOUT_SECONDS = 5

    @classmethod
    def fire(
        cls,
        *,
        severity: str,
        title: str,
        body: str = "",
        dedup_key: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> bool:
        """Fan an alert out to every configured provider.

        Returns True when at least one provider acknowledged.
        """
        if severity not in ("critical", "error", "warning", "info", "resolved"):
            severity = "warning"

        delivered = False
        try:
            if cls._post_pagerduty(severity, title, body, dedup_key, context):
                delivered = True
        except Exception as exc:
            Log.warning("AlertSink: PagerDuty post failed: %s", exc, category='alert.sink')

        try:
            if cls._post_slack(severity, title, body, context):
                delivered = True
        except Exception as exc:
            Log.warning("AlertSink: Slack post failed: %s", exc, category='alert.sink')

        if not delivered:
            _sev = str(severity or "").lower()
            _level = (
                "info"
                if _sev in ("resolved", "ok", "info")
                else "warning"
                if _sev in ("warning", "warn")
                else "error"
            )
            getattr(Log, _level)(
                f"[ALERT-FALLBACK] severity={severity} title={title!r} "
                f"dedup={dedup_key} body={body!r}",
                category="alert.fallback",
            )
        return delivered

    @classmethod
    def _post_pagerduty(
        cls,
        severity: str,
        title: str,
        body: str,
        dedup_key: str | None,
        context: dict[str, Any] | None,
    ) -> bool:
        routing_key = config("notifications.alerts_pagerduty_routing_key", "")
        if not routing_key:
            return False
        prefix = config("notifications.alerts_pagerduty_dedup_prefix", "app")
        full_key = f"{prefix}.{dedup_key}" if dedup_key else None

        if severity == "resolved":
            event_action = "resolve"
            pd_severity = "info"
        elif severity == "critical":
            event_action = "trigger"
            pd_severity = "critical"
        elif severity == "error":
            event_action = "trigger"
            pd_severity = "error"
        else:
            event_action = "trigger"
            pd_severity = "warning"

        payload = {
            "routing_key": routing_key,
            "event_action": event_action,
            "payload": {
                "summary": title[:1024],
                "source": config(
                    "notifications.alerts_source_hostname", "app"
                ),
                "severity": pd_severity,
                "custom_details": {
                    "body": body[:4096] if body else "",
                    **(context or {}),
                },
            },
        }
        if full_key:
            payload["dedup_key"] = full_key

        try:
            req = Request(
                "https://events.pagerduty.com/v2/enqueue",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=cls.HTTP_TIMEOUT_SECONDS) as resp:
                return 200 <= resp.status < 300
        except (HTTPError, URLError, TimeoutError) as exc:
            Log.warning("PagerDuty enqueue failed (severity=%s): %s", severity, exc, category='alert.sink')
            return False

    @classmethod
    def _post_slack(
        cls,
        severity: str,
        title: str,
        body: str,
        context: dict[str, Any] | None,
    ) -> bool:
        url = config("notifications.alerts_slack_webhook_url", "")
        if not url:
            return False
        emoji = {
            "critical": ":rotating_light:",
            "error": ":x:",
            "warning": ":warning:",
            "info": ":information_source:",
            "resolved": ":white_check_mark:",
        }.get(severity, ":warning:")
        ctx_str = ""
        if context:
            try:
                ctx_str = f"\n```\n{json.dumps(context, indent=2, sort_keys=True, default=str)[:1800]}\n```"
            except (TypeError, ValueError):
                ctx_str = ""
        text = f"{emoji} *{title}*\n{body}{ctx_str}"
        try:
            req = Request(
                url,
                data=json.dumps({"text": text[:35_000]}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=cls.HTTP_TIMEOUT_SECONDS) as resp:
                return 200 <= resp.status < 300
        except (HTTPError, URLError, TimeoutError) as exc:
            Log.warning("Slack webhook failed: %s", exc, category='alert.sink')
            return False


__all__ = ["AlertSink"]
