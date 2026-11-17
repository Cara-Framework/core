"""Gate: every alert's runbook_url lands on a section that names it."""

from __future__ import annotations

from cara.docs.Runbooks import heading_sections, heading_slug, runbook_link_problems

PREFIX = "https://example.test/docs/blob/main/internal/alert-runbook.md"

RUNBOOK = """# Alert runbook

## Services down

`ServiceDown`

```bash
# Not a heading
```

## Paid spend (scrape.do)

`ScrapeCreditsLow`

## Paid spend (scrape.do)

`ScrapeCreditsCritical`
"""


def _rules(tmp_path, body: str):
    path = tmp_path / "alert_rules.yml"
    path.write_text(body, encoding="utf-8")
    return path


def _rule(name: str, url: str | None) -> str:
    notes = f'\n          runbook_url: "{url}"' if url else ""
    return (
        f"      - alert: {name}\n        expr: up == 0\n"
        f"        annotations:\n          summary: x{notes}\n"
    )


def _page(tmp_path):
    page = tmp_path / "alert-runbook.md"
    page.write_text(RUNBOOK, encoding="utf-8")
    return page


def test_slugs_follow_github():
    assert heading_slug("Paid spend (scrape.do)") == "paid-spend-scrapedo"
    assert heading_slug("Queue relay / `outbox`") == "queue-relay--outbox"
    sections = heading_sections(RUNBOOK)
    assert "not-a-heading" not in sections
    assert "paid-spend-scrapedo-1" in sections


def test_bound_alerts_pass(tmp_path):
    rules = _rules(
        tmp_path,
        "groups:\n  - name: g\n    rules:\n"
        + _rule("ServiceDown", f"{PREFIX}#services-down")
        + _rule("ScrapeCreditsCritical", f"{PREFIX}#paid-spend-scrapedo-1")
        + "      - record: job:x\n        expr: up\n",
    )
    assert runbook_link_problems([rules], _page(tmp_path), PREFIX) == []


def test_each_break_is_reported(tmp_path):
    rules = _rules(
        tmp_path,
        "groups:\n  - name: g\n    rules:\n"
        + _rule("NoUrl", None)
        + _rule("WrongPage", "https://example.test/other.md#services-down")
        + _rule("MissingAnchor", f"{PREFIX}#renamed-section")
        + _rule("Unlisted", f"{PREFIX}#services-down"),
    )
    problems = runbook_link_problems([rules], _page(tmp_path), PREFIX)
    assert len(problems) == 4
    assert "NoUrl has no runbook_url" in problems[0]
    assert "WrongPage runbook_url" in problems[1]
    assert "#renamed-section is no heading" in problems[2]
    assert "Unlisted is not listed" in problems[3]
    unlisted_ok = runbook_link_problems(
        [rules], _page(tmp_path), PREFIX, require_listed=False
    )
    assert len(unlisted_ok) == 3
