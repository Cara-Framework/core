"""PreflightCommand: production-readiness gate, run before deploy.

``make:migration`` + ``migrate`` + ``schema:check`` cover the *schema* side of a
safe deploy. Nothing covered the *configuration* side: an empty / dev-default
signing key, a forgotten ``REDIS_HOST``, or ``APP_DEBUG=true`` leaking stack
traces in a prod-like environment would all sail past CI and only blow up (or,
worse, silently weaken security) once the box is live.

``check:deploy`` (alias ``preflight``) runs a registry of opinionated,
production-readiness checks and FAILS LOUDLY — non-zero exit + a per-check
report — when any of them fail. Each check is a small named callable returning a
``CheckResult`` (ok / warn / fail + message), so adding a new gate is a
one-function change: write the callable and append it to ``_default_checks``.

Seeded gates (tuned to THIS deploy — api + services, Postgres + Redis +
RabbitMQ + Meilisearch):

  * **app_key_set** — the app signing key / JWT secret is present and is NOT an
    empty / dev / placeholder default (``app.key`` + the active auth guard's
    ``secret``). A forged-token / weak-key footgun.
  * **required_config_present** — every key in a configurable REQUIRED list is
    present and non-empty (DB host + name, Redis host, RabbitMQ host,
    Meilisearch URL). Reads the REAL config keys (``database.drivers``,
    ``cache.drivers``, ``queue.drivers``, ``meilisearch.url``).
  * **debug_off_in_prod** — DEBUG / trace-leaking error rendering is OFF when the
    environment looks production-like (``app.debug`` must be falsy when
    ``app.env`` ∈ {production, prod, staging}).

``--warn-only`` downgrades every FAIL to a WARN so CI can *inspect* readiness
without the gate blocking the pipeline (exit 0). Without it, any FAIL exits 1.
``--only=app_key_set,debug_off_in_prod`` runs a subset by name.

This command is READ-ONLY — it inspects config, never mutates anything. It needs
no optional dependency (no DB / queue import), so it always registers, even on a
DB-less service.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from cara.commands import CommandBase
from cara.configuration import config
from cara.decorators import command

# --- Check result ----------------------------------------------------------

# A check's three outcomes. ``fail`` is the only one that (absent
# ``--warn-only``) drives a non-zero exit; ``warn`` is advisory; ``ok`` passes.
OK = "ok"
WARN = "warn"
FAIL = "fail"


@dataclass(frozen=True)
class CheckResult:
    """The outcome of a single preflight check."""

    status: str  # OK | WARN | FAIL
    message: str

    @property
    def failed(self) -> bool:
        return self.status == FAIL

    @property
    def warned(self) -> bool:
        return self.status == WARN


def ok(message: str) -> CheckResult:
    return CheckResult(OK, message)


def warn(message: str) -> CheckResult:
    return CheckResult(WARN, message)


def fail(message: str) -> CheckResult:
    return CheckResult(FAIL, message)


# A check is a no-arg callable returning a CheckResult. They read config via the
# module-level ``config()`` helper, so they're trivially unit-testable by
# stubbing config.
Check = Callable[[], CheckResult]


# --- Shared helpers --------------------------------------------------------

# Values that mean "the key was never really configured" — empty, whitespace, or
# a well-known dev/scaffold placeholder. Kept in lock-step with the secrets the
# app configs already refuse to boot with (see api/config/auth.py
# ``_INSECURE_DEFAULTS`` and the meilisearch/queue prod guards).
_PLACEHOLDER_SECRETS = {
    "",
    "your-secret-key",
    "your-secret-key-here",
    "secret",
    "changeme",
    "change-me",
    "default",
    "password",
    "test",
    "dev",
    "development",
    "local",
    "todo",
    "tbd",
    "xxx",
    "none",
    "null",
    # base64: / hex: prefixed *empty* payloads (key:generate writes a real one).
    "base64:",
    "hex:",
}

# Environments where a leaking-debug / weak-secret posture is unacceptable.
_PRODLIKE_ENVS = {"production", "prod", "staging", "stage"}


def _is_blank(value) -> bool:
    """True when a config value is unset or effectively empty."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _is_placeholder_secret(value) -> bool:
    """True when a secret is blank or a recognised dev/scaffold placeholder."""
    if _is_blank(value):
        return True
    return str(value).strip().lower() in _PLACEHOLDER_SECRETS


def _is_truthy(value) -> bool:
    """Interpret a config value as a boolean.

    ``app.debug`` may arrive as a real bool (config files cast it) or, defensively,
    as a string ("true"/"1"/"yes"). Treat the usual truthy strings as True.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _active_guard_secret():
    """Resolve the signing secret of the active auth guard, if any.

    ``auth.default`` names the guard (e.g. ``jwt``); ``auth.guards.<name>.secret``
    holds its signing key. Returns ``None`` when no guard / secret is configured
    (some services don't run an auth guard at all).
    """
    guard_name = config("auth.default")
    guards = config("auth.guards") or {}
    if not guard_name or not isinstance(guards, dict):
        return None
    guard = guards.get(guard_name)
    if not isinstance(guard, dict):
        return None
    return guard.get("secret")


def _active_driver(group: str):
    """Return the active driver config dict for a ``<group>`` config (or None).

    ``<group>.default`` names the driver, ``<group>.drivers.<name>`` is its
    config dict. Used to read the live host/database out of database / cache /
    queue configs without hard-coding which driver is selected.
    """
    default = config(f"{group}.default")
    drivers = config(f"{group}.drivers") or {}
    if not isinstance(drivers, dict):
        return None
    if default and default in drivers:
        return drivers.get(default)
    # Fall back to the first driver so the check still has something to inspect.
    return next(iter(drivers.values()), None) if drivers else None


# --- Seeded checks ---------------------------------------------------------


def check_app_key_set() -> CheckResult:
    """The app signing key / JWT secret must be set and not a dev placeholder."""
    app_key = config("app.key")
    guard_secret = _active_guard_secret()

    # The app key is the cross-cutting signing key. If a guard secret exists it
    # must also be real (JWT forging risk). Either being a placeholder fails.
    problems = []
    if _is_placeholder_secret(app_key):
        problems.append("app.key (APP_KEY) is empty or a dev/placeholder default")
    if guard_secret is not None and _is_placeholder_secret(guard_secret):
        problems.append(
            "the active auth guard's signing secret is empty or a placeholder"
        )
    elif guard_secret is not None and len(str(guard_secret)) < 32:
        problems.append("the active auth guard's signing secret is shorter than 32 chars")

    if problems:
        return fail(
            "Signing key not production-ready: "
            + "; ".join(problems)
            + ". Generate one with 'python craft key:generate' (and set a strong JWT_SECRET)."
        )
    return ok("Signing key / auth secret is set and not a placeholder.")


# REQUIRED config keys for this deploy. Each entry is
# ``(human_label, resolver_callable)``; the resolver returns the live value so
# the check can assert it's present + non-empty. Extend by appending a tuple.
def _required_config_specs() -> list[tuple[str, Callable[[], object]]]:
    return [
        ("database host", lambda: (_active_driver("database") or {}).get("host")),
        ("database name", lambda: (_active_driver("database") or {}).get("database")),
        ("redis cache host", lambda: (_active_driver("cache") or {}).get("host")),
        ("rabbitmq/queue host", lambda: (_active_driver("queue") or {}).get("host")),
        ("meilisearch url", lambda: config("meilisearch.url")),
    ]


def check_required_config_present() -> CheckResult:
    """Every required production infra config key must be present and non-empty."""
    missing: list[str] = []
    for label, resolver in _required_config_specs():
        try:
            value = resolver()
        except Exception:  # noqa: BLE001 — a resolver should never abort the gate
            value = None
        if _is_blank(value):
            missing.append(label)

    if missing:
        return fail(
            "Required config keys are missing or empty: "
            + ", ".join(missing)
            + ". Check your .env (DB_HOST/DB_DATABASE, REDIS_HOST, RABBIT_HOST, MEILISEARCH_URL)."
        )
    return ok("All required infra config keys are present.")


def check_debug_off_in_prod() -> CheckResult:
    """DEBUG / trace-leaking error rendering must be OFF in a prod-like env."""
    env = str(config("app.env", "") or "").strip().lower()
    debug = config("app.debug", False)

    if env not in _PRODLIKE_ENVS:
        # Not prod-like — debug being on is fine (and expected) locally.
        if _is_truthy(debug):
            return ok(f"app.debug is on, but env '{env or 'unset'}' is not prod-like.")
        return ok(f"app.debug is off (env '{env or 'unset'}').")

    if _is_truthy(debug):
        return fail(
            f"app.debug is ON in a production-like env ('{env}') — this leaks stack "
            "traces / verbose errors to clients. Set APP_DEBUG=false before deploying."
        )
    return ok(f"app.debug is off in prod-like env '{env}'.")


# The default registry, keyed by name. Adding a check = write the callable above
# and add one line here. Order is the report order.
_DEFAULT_CHECKS: dict[str, Check] = {
    "app_key_set": check_app_key_set,
    "required_config_present": check_required_config_present,
    "debug_off_in_prod": check_debug_off_in_prod,
}


@command(
    name="check:deploy",
    help="Run production-readiness preflight checks; fails loudly on any problem.",
    options={
        "--warn-only": "Downgrade failures to warnings (exit 0) for CI inspection",
        "--only=?": "Comma-separated check names to run (default: all)",
    },
)
class PreflightCommand(CommandBase):
    """Run a registry of production-readiness checks before deploy."""

    def __init__(self, application=None):
        super().__init__(application)
        # Instance copy so tests / callers can register extra checks without
        # mutating the module-level default registry.
        self.checks: dict[str, Check] = dict(_DEFAULT_CHECKS)

    def register_check(self, name: str, check: Check) -> None:
        """Add (or override) a check by name. Makes extension trivial."""
        self.checks[name] = check

    def handle(self):
        self.info("Running deploy preflight checks...")

        warn_only = bool(self.option("warn_only"))
        selected = self._selected_check_names()
        if selected is None:
            return 1  # unknown --only name; _selected_check_names already reported

        failures = 0
        warnings = 0
        passed = 0

        for name in selected:
            check = self.checks[name]
            try:
                result = check()
            except Exception as exc:  # noqa: BLE001 — a broken check must not abort the gate
                # A check that throws is itself a readiness problem — treat it as
                # a failure rather than letting it crash the whole preflight.
                result = fail(f"check raised an unexpected error: {exc}")

            if result.failed:
                if warn_only:
                    warnings += 1
                    self.warning(
                        f"⚠ {name}: {result.message} (downgraded by --warn-only)"
                    )
                else:
                    failures += 1
                    self.error(f"× {name}: {result.message}")
            elif result.warned:
                warnings += 1
                self.warning(f"⚠ {name}: {result.message}")
            else:
                passed += 1
                self.success(f"{name}: {result.message}")

        self._summary(passed, warnings, failures, warn_only)

        if failures:
            # Non-zero exit so CI fails loudly. CommandRunner maps the int return
            # into ``typer.Exit(code=...)``.
            return 1

    # --- helpers -----------------------------------------------------------

    def _selected_check_names(self) -> list[str] | None:
        """Resolve the ordered list of check names to run.

        Honours ``--only=a,b``; reports + returns ``None`` on an unknown name so
        the caller exits non-zero (a typo'd gate name silently skipping its
        check would defeat the purpose).
        """
        only = self.option("only")
        if not only:
            return list(self.checks.keys())

        requested = [n.strip() for n in str(only).split(",") if n.strip()]
        unknown = [n for n in requested if n not in self.checks]
        if unknown:
            self.error(
                "Unknown check name(s): "
                + ", ".join(unknown)
                + ". Available: "
                + ", ".join(self.checks.keys())
            )
            return None
        # Preserve registry order for the subset.
        return [n for n in self.checks if n in requested]

    def _summary(self, passed: int, warnings: int, failures: int, warn_only: bool):
        self.info("\n" + "=" * 60)
        self.info(
            f"Preflight: {passed} passed, {warnings} warning(s), {failures} failure(s)."
        )
        if failures:
            self.error(
                f"✗ Deploy preflight FAILED ({failures} failing check(s)). "
                "Fix the above before deploying."
            )
        elif warnings and warn_only:
            self.warning(
                "Preflight passed with warnings (--warn-only downgraded failures). "
                "Review the warnings before deploying."
            )
        elif warnings:
            self.warning("Preflight passed, but with warnings — review them.")
        else:
            self.success("All preflight checks passed — clear to deploy!")
