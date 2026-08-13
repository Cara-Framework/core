"""DeployTopologyAudit: prove a deployment runs the processes the queue needs.

WHY THIS IS FRAMEWORK KNOWLEDGE
-------------------------------
``Bus.dispatch`` does not publish to the broker. It commits a row to the
``queue_job_delivery`` outbox, and only ``craft queue:relay`` turns that row
into a broker message; ``craft queue:hooks`` owns the terminal-hook outbox the
same way. That makes the relay and the hooks process MANDATORY companions of
the amqp driver — a fact about cara, not about any product.

When the framework made that change, neither product's compose file learned
about it. One ran six ``queue:work`` services and no relay for five days; the
other piled up 1250 ``pending`` jobs for hours while every trigger still
reported success. Nothing failed loudly because nothing could: the failure
mode is a queue that LOOKS drained.

The rule therefore belongs here, and a product supplies only the coordinates
of its own deployment: where its compose file and alert rules live, what its
Prometheus job label and metric prefix are, and whether its roles are expected
to hold distinct exporter ports.

WHAT IT REFUSES TO ASSUME
-------------------------
Products declare a container's role in two different, equally valid ways: a
compose ``command`` naming the craft subcommand, or a ``PROCESS_TYPE``
environment value the image's entrypoint switches on. An audit that understood
only one idiom would report every role missing on a perfectly correct stack —
a false alarm in a guard is worse than no guard, because it trains people to
ignore it. Both idioms are read, and their results are unioned.

Distinct metrics ports are likewise NOT a universal law: a product that gives
each worker container its own network namespace legitimately runs nine workers
on one port number, while a product co-locating roles must keep them apart.
That is why ``distinct_metrics_ports`` is opt-in rather than assumed.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .DeployTopologyManifest import DeployTopologyManifest

#: Companion processes the amqp delivery rail cannot work without, mapped to
#: what breaks — silently — when one is absent. ``queue:work`` is deliberately
#: listed: assuming it is the whole story is the bug this audit exists for.
REQUIRED_ROLES: dict[str, str] = {
    "queue:relay": (
        "publishes queue_job_delivery outbox rows to the broker; without it "
        "every dispatch is a no-op that reports success"
    ),
    "queue:hooks": (
        "drains the terminal-hook outbox; without it completion callbacks "
        "accumulate unsent"
    ),
    "queue:work": "consumes and executes jobs",
    "schedule:work": (
        "runs scheduled ticks, including the outbox stall probe that is the "
        "only thing able to report a dead relay"
    ),
}

#: ``PROCESS_TYPE`` values an image entrypoint switches on, mapped to the craft
#: role each one execs. Pinned here so a change to the entrypoint contract has
#: to change the framework too, loudly, instead of quietly blinding the audit.
PROCESS_TYPE_ROLES: dict[str, str] = {
    "worker": "queue:work",
    "scheduler": "schedule:work",
    "relay": "queue:relay",
    "hooks": "queue:hooks",
}

#: Outbox alerts that must read a SCHEDULER-scoped gauge. Every relay-published
#: series vanishes the instant the relay dies, so a stall rule scoped to the
#: relay evaluates to "no data" precisely when it should fire.
OUTBOX_ALERTS: tuple[str, ...] = (
    "QueueOutboxStalled",
    "QueueOutboxBacklogAging",
    "QueueOutboxProbeStale",
)

#: Alert whose expression must carry an ``absent()`` arm: a probe that NEVER
#: ran leaves no series at all, and staleness arithmetic over a missing series
#: can never fire on its own.
PROBE_STALE_ALERT = "QueueOutboxProbeStale"

#: Readiness alert -> metric stem (prefixed per product). ``0`` is the alarming
#: value, so a missing series must read as ``0`` via ``or vector(0)`` — a
#: process that never started otherwise produces no data and no alert.
READINESS_ALERTS: dict[str, str] = {
    "QueueRelayNotReady": "queue_relay_ready",
    "QueueHooksNotReady": "queue_hooks_ready",
}

#: A metrics port of ``0`` means "no exporter on this process" and is shareable.
NO_EXPORTER = "0"


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml  # local: heavy optional dep

    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _environment(service: dict[str, Any]) -> dict[str, str]:
    """Compose ``environment`` as a mapping, in either legal spelling."""
    environment = service.get("environment") or {}
    if isinstance(environment, dict):
        return {str(k): str(v) for k, v in environment.items()}
    pairs: dict[str, str] = {}
    for entry in environment:
        key, _, value = str(entry).partition("=")
        pairs[key] = value
    return pairs


def _role_from_command(service: dict[str, Any]) -> str | None:
    """The craft subcommand a compose ``command`` execs, if it names one."""
    command = service.get("command")
    tokens: list[str]
    if isinstance(command, list):
        tokens = [str(token) for token in command]
    elif isinstance(command, str):
        tokens = command.split()
    else:
        return None
    if "craft" not in tokens:
        return None
    for token in tokens[tokens.index("craft") + 1 :]:
        if not token.startswith("-"):
            return token
    return None


def _role_from_process_type(environment: dict[str, str]) -> str | None:
    return PROCESS_TYPE_ROLES.get(environment.get("PROCESS_TYPE", "").strip())


class DeployTopologyAudit:
    """Read a deployment and report every way it under-serves the queue rail.

    Every method returns a list of human-readable problems, empty when the
    deployment is correct. Callers assert on the lists rather than on booleans
    so a failure names what is missing and why it matters.
    """

    def __init__(self, manifest: DeployTopologyManifest) -> None:
        self._manifest = manifest
        self._compose_cache: dict[str, Any] | None = None
        self._alerts_cache: dict[str, str] | None = None

    # --- deployment facts -------------------------------------------------

    def _compose(self) -> dict[str, Any]:
        if self._compose_cache is None:
            self._compose_cache = _load_yaml(self._manifest.compose)
        return self._compose_cache

    def services_by_role(self) -> dict[str, set[str]]:
        """Role -> the compose services running it, read through BOTH idioms.

        A service may declare its role by craft ``command`` or by
        ``PROCESS_TYPE``; whichever it uses, it lands under the same role, and
        a service using both must agree with itself (see
        :meth:`role_declaration_conflicts`).
        """
        found: dict[str, set[str]] = {}
        for name, service in (self._compose().get("services") or {}).items():
            if name in self._manifest.ignored_services or not isinstance(service, dict):
                continue
            for role in {
                _role_from_command(service),
                _role_from_process_type(_environment(service)),
            } - {None}:
                found.setdefault(str(role), set()).add(str(name))
        return found

    def role_declaration_conflicts(self) -> list[str]:
        """Services whose two role declarations disagree.

        A compose ``command`` overrides the image entrypoint, so a container
        whose ``PROCESS_TYPE`` says ``relay`` while its command execs
        ``queue:work`` runs a worker and is monitored as a relay — an outage
        that looks like a healthy stack from both ends.
        """
        problems: list[str] = []
        for name, service in (self._compose().get("services") or {}).items():
            if name in self._manifest.ignored_services or not isinstance(service, dict):
                continue
            commanded = _role_from_command(service)
            declared = _role_from_process_type(_environment(service))
            if commanded and declared and commanded != declared:
                problems.append(
                    f"{name} declares PROCESS_TYPE for {declared} but its command "
                    f"execs {commanded}; the command wins and the monitoring does not"
                )
        return problems

    # --- the rules --------------------------------------------------------

    def missing_roles(self) -> list[str]:
        """Required companion processes the stack never starts."""
        deployed = self.services_by_role()
        return [
            f"{role} is not deployed — it {REQUIRED_ROLES[role]}"
            for role in sorted(set(REQUIRED_ROLES) - set(deployed))
        ]

    def metrics_port_collisions(self) -> list[str]:
        """Two exporters bound to one port, when the product forbids that."""
        if not self._manifest.distinct_metrics_ports:
            return []
        owners: dict[str, str] = {}
        collisions: list[str] = []
        for name, service in (self._compose().get("services") or {}).items():
            if not isinstance(service, dict):
                continue
            environment = _environment(service)
            for key in self._manifest.metrics_port_keys:
                port = environment.get(key, "").strip()
                if not port or port == NO_EXPORTER:
                    continue
                if port in owners and owners[port] != name:
                    collisions.append(
                        f"{name} and {owners[port]} both export on :{port} — one of "
                        f"them will fail to bind and the other runs unmonitored"
                    )
                owners[port] = name
        return collisions

    def _alerts(self) -> dict[str, str]:
        if self._alerts_cache is None:
            rules = _load_yaml(self._manifest.alert_rules)
            self._alerts_cache = {
                rule["alert"]: str(rule.get("expr", ""))
                for group in rules.get("groups") or []
                for rule in group.get("rules") or []
                if "alert" in rule
            }
        return self._alerts_cache

    def outbox_alert_problems(self) -> list[str]:
        """The observer must not be the observed.

        Outbox alerts read gauges the SCHEDULER publishes, because the process
        they are watching for is the one that stops publishing when it dies.
        """
        alerts = self._alerts()
        scope = f'job="{self._manifest.scheduler_job}"'
        problems: list[str] = []
        for name in OUTBOX_ALERTS:
            if name not in alerts:
                problems.append(
                    f"{name} is missing from {self._manifest.alert_rules.name}"
                )
                continue
            if scope not in alerts[name]:
                problems.append(
                    f"{name} must read a scheduler-scoped gauge ({scope}); scoping it "
                    f"to the relay makes it go silent exactly when the relay dies"
                )
        if PROBE_STALE_ALERT in alerts and "absent(" not in alerts[PROBE_STALE_ALERT]:
            problems.append(
                f"{PROBE_STALE_ALERT} needs its absent() arm: a probe that never "
                f"started leaves no series for the staleness arm to age"
            )
        return problems

    def readiness_alert_problems(self) -> list[str]:
        """A dead relay or hooks process must page on its own, not by inference."""
        alerts = self._alerts()
        problems: list[str] = []
        for name, stem in READINESS_ALERTS.items():
            metric = f"{self._manifest.metric_prefix}{stem}"
            if name not in alerts:
                problems.append(
                    f"{name} is missing from {self._manifest.alert_rules.name}"
                )
                continue
            expression = alerts[name]
            if metric not in expression:
                problems.append(f"{name} does not read {metric}")
            if "or vector(0)" not in expression:
                problems.append(
                    f"{name} must substitute 0 for a missing series (`or vector(0)`) — "
                    f"a process that never started otherwise produces no data and no alert"
                )
        return problems

    def unscraped_roles(self) -> list[str]:
        """An exporter nobody scrapes is an alert that can never fire.

        The compose port is matched literally against the scrape config, so a
        port changed on one side and not the other fails here rather than in
        the silence after an incident.
        """
        if self._manifest.scrape_config is None:
            return []
        scrape = self._manifest.scrape_config.read_text(encoding="utf-8")
        services = self._compose().get("services") or {}
        deployed = self.services_by_role()
        problems: list[str] = []
        for role in self._manifest.scraped_roles:
            for name in sorted(deployed.get(role, set())):
                port = self._exporter_port(services.get(name) or {})
                if port is None:
                    problems.append(
                        f"{name} runs {role} but declares no metrics port, so its "
                        f"readiness gauge is unreachable"
                    )
                    continue
                if not re.search(rf"\b{re.escape(name)}:{re.escape(port)}\b", scrape):
                    problems.append(
                        f"{name}:{port} ({role}) is not a Prometheus target in "
                        f"{self._manifest.scrape_config.name}; its readiness gauge is "
                        f"never collected and its alert can never fire"
                    )
        return problems

    def _exporter_port(self, service: dict[str, Any]) -> str | None:
        environment = _environment(service)
        for key in self._manifest.metrics_port_keys:
            port = environment.get(key, "").strip()
            if port and port != NO_EXPORTER:
                return port
        return None

    # --- everything at once ----------------------------------------------

    def problems(self) -> list[str]:
        """Every problem the audit can see, in reporting order."""
        return [
            *self.missing_roles(),
            *self.role_declaration_conflicts(),
            *self.metrics_port_collisions(),
            *self.outbox_alert_problems(),
            *self.readiness_alert_problems(),
            *self.unscraped_roles(),
        ]


def scheduled_job_ids(entries: Iterable[dict[str, Any]]) -> set[str]:
    """Ids of a product's scheduled entries, for the stall-probe assertion.

    The probe has to stay a scheduled INLINE tick: dispatching it onto the
    queue would put the detector inside the outage it exists to report. The
    entry's NAME is product-owned, so the product asserts membership itself —
    this only spares it from re-deriving the id set.
    """
    return {str(entry.get("id")) for entry in entries if entry.get("id")}
