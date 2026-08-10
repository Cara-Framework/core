"""The deploy-topology audit, against synthetic stacks in both idioms.

The audit's whole value is that it fires on a real omission and stays silent
on a correct stack expressed either way a product may express it. Both halves
are pinned here: a product that adopted a guard which cannot read its compose
file would see "everything missing" and rightly turn the guard off.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from cara.testing.audits import (
    REQUIRED_ROLES,
    DeployTopologyAudit,
    DeployTopologyManifest,
    scheduled_job_ids,
)

pytest.importorskip("yaml")


COMMAND_COMPOSE = """
services:
  worker:
    command: ["python", "craft", "queue:work", "--pool=all"]
    environment:
      METRICS_PORT: "9101"
  queue-relay:
    command: ["python", "craft", "queue:relay"]
    environment:
      METRICS_PORT: "9103"
  queue-hooks:
    command: ["python", "craft", "queue:hooks"]
    environment:
      METRICS_PORT: "9104"
  scheduler:
    command: ["python", "craft", "schedule:work"]
    environment:
      SCHEDULER_METRICS_PORT: "9102"
  database:
    image: postgres:17
"""

PROCESS_TYPE_COMPOSE = """
services:
  worker-sync:
    environment:
      PROCESS_TYPE: worker
      METRICS_PORT: "9400"
  worker-bulk:
    environment:
      PROCESS_TYPE: worker
      METRICS_PORT: "9400"
  queue-relay:
    environment:
      PROCESS_TYPE: relay
      METRICS_PORT: "9402"
  queue-hooks:
    environment:
      PROCESS_TYPE: hooks
      METRICS_PORT: "9403"
  scheduler:
    environment:
      PROCESS_TYPE: scheduler
      METRICS_PORT: "9401"
      SCHEDULER_METRICS_PORT: "9401"
  database:
    image: postgres:17
"""

ALERTS = """
groups:
  - name: queue
    rules:
      - alert: QueueOutboxStalled
        expr: max(acme_queue_outbox_stalled{job="acme-scheduler"}) > 0
      - alert: QueueOutboxBacklogAging
        expr: max(acme_queue_outbox_oldest_due_age_seconds{job="acme-scheduler"}) > 60
      - alert: QueueOutboxProbeStale
        expr: >
          (time() - max(acme_queue_outbox_sample_timestamp_seconds{job="acme-scheduler"})) > 300
          or absent(acme_queue_outbox_sample_timestamp_seconds{job="acme-scheduler"})
      - alert: QueueRelayNotReady
        expr: (max(acme_queue_relay_ready{job="acme-queue-relay"}) or vector(0)) < 1
      - alert: QueueHooksNotReady
        expr: (max(acme_queue_hooks_ready{job="acme-queue-hooks"}) or vector(0)) < 1
"""

SCRAPE = """
scrape_configs:
  - job_name: acme-queue-relay
    static_configs:
      - targets: ["queue-relay:9103"]
  - job_name: acme-queue-hooks
    static_configs:
      - targets: ["queue-hooks:9104"]
  - job_name: acme-scheduler
    static_configs:
      - targets: ["scheduler:9102"]
"""


def _stack(
    tmp_path: Path,
    *,
    compose: str = COMMAND_COMPOSE,
    alerts: str = ALERTS,
    scrape: str | None = SCRAPE,
    **overrides: object,
) -> DeployTopologyAudit:
    compose_path = tmp_path / "compose.yml"
    compose_path.write_text(textwrap.dedent(compose))
    alerts_path = tmp_path / "alert_rules.yml"
    alerts_path.write_text(textwrap.dedent(alerts))
    scrape_path: Path | None = None
    if scrape is not None:
        scrape_path = tmp_path / "prometheus.yml"
        scrape_path.write_text(textwrap.dedent(scrape))
    manifest = DeployTopologyManifest(
        compose=compose_path,
        alert_rules=alerts_path,
        scheduler_job="acme-scheduler",
        metric_prefix="acme_",
        scrape_config=scrape_path,
        **overrides,  # type: ignore[arg-type]
    )
    return DeployTopologyAudit(manifest)


class TestRoleExtraction:
    def test_a_craft_command_stack_is_read(self, tmp_path):
        audit = _stack(tmp_path)
        assert set(audit.services_by_role()) >= set(REQUIRED_ROLES)
        assert audit.missing_roles() == []

    def test_a_process_type_stack_is_read(self, tmp_path):
        """The second idiom must not read as an empty stack.

        An audit that only understood craft commands would report all four
        roles missing here — on a topology that is entirely correct.
        """
        audit = _stack(
            tmp_path,
            compose=PROCESS_TYPE_COMPOSE,
            scrape=SCRAPE.replace("9103", "9402")
            .replace("9104", "9403")
            .replace("9102", "9401"),
        )
        assert audit.missing_roles() == []
        assert audit.services_by_role()["queue:work"] == {"worker-sync", "worker-bulk"}

    def test_a_missing_relay_is_named_with_its_consequence(self, tmp_path):
        compose = COMMAND_COMPOSE.replace(
            '  queue-relay:\n    command: ["python", "craft", "queue:relay"]\n'
            '    environment:\n      METRICS_PORT: "9103"\n',
            "",
        )
        audit = _stack(tmp_path, compose=compose, scrape=SCRAPE)
        missing = audit.missing_roles()
        assert len(missing) == 1
        assert "queue:relay" in missing[0]
        assert "outbox" in missing[0]

    def test_a_worker_only_stack_reports_every_companion(self, tmp_path):
        """The original outage shape: six workers and nothing else."""
        compose = """
        services:
          worker-a:
            command: ["python", "craft", "queue:work"]
          worker-b:
            command: ["python", "craft", "queue:work"]
        """
        audit = _stack(tmp_path, compose=compose, scrape=None)
        assert len(audit.missing_roles()) == 3

    def test_a_command_that_contradicts_process_type_is_a_finding(self, tmp_path):
        compose = """
        services:
          queue-relay:
            command: ["python", "craft", "queue:work"]
            environment:
              PROCESS_TYPE: relay
        """
        audit = _stack(tmp_path, compose=compose, scrape=None)
        conflicts = audit.role_declaration_conflicts()
        assert len(conflicts) == 1
        assert "queue-relay" in conflicts[0]


class TestMetricsPorts:
    def test_shared_ports_are_legal_unless_the_product_opts_in(self, tmp_path):
        """Nine workers on one port number is a correct topology, not a bug.

        Each worker has its own network namespace; forcing distinct numbers on
        every product would make the guard wrong for one of them.
        """
        audit = _stack(
            tmp_path,
            compose=PROCESS_TYPE_COMPOSE,
            scrape=SCRAPE.replace("9103", "9402")
            .replace("9104", "9403")
            .replace("9102", "9401"),
        )
        assert audit.metrics_port_collisions() == []

    def test_opting_in_catches_two_roles_on_one_port(self, tmp_path):
        compose = COMMAND_COMPOSE.replace('METRICS_PORT: "9104"', 'METRICS_PORT: "9103"')
        audit = _stack(
            tmp_path,
            compose=compose,
            scrape=SCRAPE.replace("queue-hooks:9104", "queue-hooks:9103"),
            distinct_metrics_ports=True,
        )
        collisions = audit.metrics_port_collisions()
        assert len(collisions) == 1
        assert ":9103" in collisions[0]

    def test_a_zero_port_means_no_exporter_and_never_collides(self, tmp_path):
        compose = COMMAND_COMPOSE.replace('METRICS_PORT: "9101"', 'METRICS_PORT: "0"')
        compose = compose.replace(
            '  queue-hooks:\n    command: ["python", "craft", "queue:hooks"]\n'
            '    environment:\n      METRICS_PORT: "9104"\n',
            '  queue-hooks:\n    command: ["python", "craft", "queue:hooks"]\n'
            '    environment:\n      METRICS_PORT: "0"\n',
        )
        audit = _stack(
            tmp_path,
            compose=compose,
            scrape=SCRAPE,
            distinct_metrics_ports=True,
        )
        assert audit.metrics_port_collisions() == []


class TestAlerting:
    def test_a_conforming_alert_file_is_silent(self, tmp_path):
        audit = _stack(tmp_path)
        assert audit.outbox_alert_problems() == []
        assert audit.readiness_alert_problems() == []

    def test_a_relay_scoped_outbox_alert_is_rejected(self, tmp_path):
        """The observer must not be the observed."""
        alerts = ALERTS.replace(
            'max(acme_queue_outbox_stalled{job="acme-scheduler"})',
            'max(acme_queue_outbox_stalled{job="acme-queue-relay"})',
        )
        audit = _stack(tmp_path, alerts=alerts)
        problems = audit.outbox_alert_problems()
        assert len(problems) == 1
        assert "QueueOutboxStalled" in problems[0]

    def test_a_probe_stale_alert_without_absent_is_rejected(self, tmp_path):
        alerts = ALERTS.replace(
            "\n          or absent(acme_queue_outbox_sample_timestamp_seconds"
            '{job="acme-scheduler"})',
            "",
        )
        audit = _stack(tmp_path, alerts=alerts)
        problems = audit.outbox_alert_problems()
        assert len(problems) == 1
        assert "absent()" in problems[0]

    def test_a_readiness_alert_without_vector_zero_is_rejected(self, tmp_path):
        alerts = ALERTS.replace(
            '(max(acme_queue_relay_ready{job="acme-queue-relay"}) or vector(0)) < 1',
            'max(acme_queue_relay_ready{job="acme-queue-relay"}) < 1',
        )
        audit = _stack(tmp_path, alerts=alerts)
        problems = audit.readiness_alert_problems()
        assert len(problems) == 1
        assert "vector(0)" in problems[0]

    def test_a_missing_outbox_alert_is_reported(self, tmp_path):
        alerts = ALERTS.replace("QueueOutboxBacklogAging", "QueueSomethingElse")
        audit = _stack(tmp_path, alerts=alerts)
        assert any("QueueOutboxBacklogAging" in p for p in audit.outbox_alert_problems())

    def test_the_metric_prefix_is_the_products_own(self, tmp_path):
        """A product whose metrics carry another namespace must fail loudly."""
        compose_path = tmp_path / "compose.yml"
        compose_path.write_text(textwrap.dedent(COMMAND_COMPOSE))
        alerts_path = tmp_path / "alert_rules.yml"
        alerts_path.write_text(textwrap.dedent(ALERTS))
        audit = DeployTopologyAudit(
            DeployTopologyManifest(
                compose=compose_path,
                alert_rules=alerts_path,
                scheduler_job="acme-scheduler",
                metric_prefix="other_",
            )
        )
        assert any(
            "other_queue_relay_ready" in p for p in audit.readiness_alert_problems()
        )


class TestScrapeCoverage:
    def test_a_fully_scraped_stack_is_silent(self, tmp_path):
        assert _stack(tmp_path).unscraped_roles() == []

    def test_an_unscraped_relay_is_reported(self, tmp_path):
        scrape = SCRAPE.replace('- targets: ["queue-relay:9103"]', "- targets: []")
        audit = _stack(tmp_path, scrape=scrape)
        problems = audit.unscraped_roles()
        assert len(problems) == 1
        assert "queue-relay:9103" in problems[0]

    def test_a_port_that_drifted_from_compose_is_reported(self, tmp_path):
        """The scrape target and the compose port must be the SAME number.

        A target left pointing at the old port scrapes nothing, and the
        readiness alert it feeds can never fire.
        """
        scrape = SCRAPE.replace("queue-hooks:9104", "queue-hooks:9204")
        audit = _stack(tmp_path, scrape=scrape)
        problems = audit.unscraped_roles()
        assert len(problems) == 1
        assert "queue-hooks:9104" in problems[0]

    def test_no_scrape_config_declared_means_no_scrape_findings(self, tmp_path):
        assert _stack(tmp_path, scrape=None).unscraped_roles() == []


class TestProblemsAggregate:
    def test_a_correct_stack_has_no_problems_at_all(self, tmp_path):
        assert _stack(tmp_path).problems() == []

    def test_every_check_contributes_to_the_aggregate(self, tmp_path):
        compose = COMMAND_COMPOSE.replace(
            '  queue-hooks:\n    command: ["python", "craft", "queue:hooks"]\n'
            '    environment:\n      METRICS_PORT: "9104"\n',
            "",
        )
        alerts = ALERTS.replace("QueueRelayNotReady", "QueueRelayMaybeNotReady")
        audit = _stack(tmp_path, compose=compose, alerts=alerts)
        problems = audit.problems()
        assert any("queue:hooks" in p for p in problems)
        assert any("QueueRelayNotReady" in p for p in problems)


class TestScheduledJobIds:
    def test_ids_are_collected_and_blank_entries_ignored(self):
        entries = [{"id": "sweep_queue_outbox_health"}, {"id": ""}, {"name": "x"}]
        assert scheduled_job_ids(entries) == {"sweep_queue_outbox_health"}
