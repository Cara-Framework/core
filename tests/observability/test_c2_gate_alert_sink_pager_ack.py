"""A configured PagerDuty's answer is what ``AlertSink.fire`` reports.

The outbox watchdog closes its own record of an incident when ``fire``
acknowledges a resolve, and throttles a stall page for 15 minutes when
``fire`` acknowledges it. With PagerDuty and Slack both configured, a Slack
ack alone used to read as delivered: the watchdog forgot an incident that
stayed open in PagerDuty forever, or went quiet on a stall PagerDuty never
received.
"""

from __future__ import annotations

import importlib

import pytest

sink_module = importlib.import_module("cara.observability.AlertSink")
AlertSink = sink_module.AlertSink


def _wire(monkeypatch, *, routing_key, pagerduty_ok, slack_ok):
    settings = {"notifications.alerts_pagerduty_routing_key": routing_key}
    monkeypatch.setattr(
        sink_module, "config", lambda key, default=None: settings.get(key, default)
    )
    monkeypatch.setattr(
        AlertSink, "_post_pagerduty", classmethod(lambda cls, *a: pagerduty_ok)
    )
    monkeypatch.setattr(AlertSink, "_post_slack", classmethod(lambda cls, *a: slack_ok))


@pytest.mark.parametrize("severity", ["critical", "resolved"])
def test_a_slack_ack_does_not_stand_in_for_a_failed_pager(monkeypatch, severity):
    _wire(monkeypatch, routing_key="rk", pagerduty_ok=False, slack_ok=True)

    assert AlertSink.fire(severity=severity, title="t", dedup_key="k") is False


def test_the_pager_ack_is_delivered_even_when_slack_fails(monkeypatch):
    _wire(monkeypatch, routing_key="rk", pagerduty_ok=True, slack_ok=False)

    assert AlertSink.fire(severity="critical", title="t", dedup_key="k") is True


def test_without_a_pager_slack_is_the_sink(monkeypatch):
    _wire(monkeypatch, routing_key="", pagerduty_ok=False, slack_ok=True)

    assert AlertSink.fire(severity="critical", title="t", dedup_key="k") is True
