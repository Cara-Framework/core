"""DeployTopologyManifest."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DeployTopologyManifest:
    """Where one product's deployment describes itself.

    ``scrape_config`` is any file whose text lists Prometheus targets — a
    rendered YAML config or the shell script that renders one. It is searched
    for the literal ``service:port`` pair, so a compose port and a scrape
    target that drift apart fail, not just a missing target.
    """

    compose: Path
    alert_rules: Path
    #: Prometheus ``job`` label of the scheduler, e.g. ``"<product>-scheduler"``.
    scheduler_job: str
    #: Metric namespace prefix including its separator, e.g. ``"<product>_"``.
    metric_prefix: str
    scrape_config: Path | None = None
    #: Roles whose exporter must appear in ``scrape_config``. Worker roles are
    #: excluded by default: a product may run many worker containers and scrape
    #: them through a job-level target list rather than one entry per role.
    scraped_roles: tuple[str, ...] = ("queue:relay", "queue:hooks", "schedule:work")
    #: Opt-in. True only where roles share a network namespace, so two
    #: exporters on one port would mean the loser dies or runs blind.
    distinct_metrics_ports: bool = False
    #: Every env key that can drive an exporter binding. Role-specific keys
    #: (``RELAY_METRICS_PORT``/``HOOKS_METRICS_PORT``/``SCHEDULER_METRICS_PORT``)
    #: exist because dev runs all roles in one network namespace; a service
    #: that sets more than one of these must set them to ONE value —
    #: :meth:`DeployTopologyAudit.split_metrics_ports` enforces that, because
    #: probes and Prometheus follow ``METRICS_PORT`` while the process binds
    #: its role key, and the two agreeing only by default-value coincidence
    #: is exactly the drift that pages an operator against a healthy relay.
    metrics_port_keys: tuple[str, ...] = (
        "METRICS_PORT",
        "SCHEDULER_METRICS_PORT",
        "RELAY_METRICS_PORT",
        "HOOKS_METRICS_PORT",
    )
    #: Compose services that are not application processes (databases, proxies)
    #: and are exempt from role extraction noise. Purely cosmetic: a service
    #: with neither idiom contributes no role either way.
    ignored_services: frozenset[str] = field(default_factory=frozenset)
