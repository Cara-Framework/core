"""The deployment-environment vocabulary every process reads identically.

``APP_ENV`` decides whether a security gate is strict. Left to each caller,
the answer drifts: one module accepts ``{"production", "prod"}``, another
compares the RAW value to the single literal ``"production"`` — so
``APP_ENV=prod`` (or ``Production``, or ``" prod"``) reads as "not production"
there and a relaxed development default leaks into a production process. The
framework restated the pair itself in its mail provider and half a dozen
commands, lower-casing but never stripping.

Configuration still owns the ``env()`` READ (DOCTRINE §5); what the value
MEANS is decided here, once, for the framework and every product on it.
It lives in ``cara.configuration`` rather than ``cara.environment`` because
it reads nothing: application code may ask what an ``APP_ENV`` value means
without being allowed to read the process environment itself.

Normalisation is part of the contract, not a caller's courtesy: ``APP_ENV`` is
typed by humans into shells, Compose files and CI settings, so ``" Local"`` and
``"PRODUCTION"`` are ordinary inputs and every predicate strips and lowercases
before it decides.

"Not production" and "a developer's machine" are different questions —
staging is the environment that proves it — so both predicates exist and a
caller picks the one it means.
"""

from __future__ import annotations

#: Environments that mean "an automated test run" — both spellings in use.
TEST_ENVIRONMENTS = frozenset({"test", "testing"})

#: Environments that mean "a developer's machine or a test run" — the only
#: places a relaxed default may apply.
LOCAL_ENVIRONMENTS = frozenset({"local", "dev", "development"}) | TEST_ENVIRONMENTS

#: Environments that mean "real users are on the other end". Both spellings
#: are accepted because both are in use across Compose, CI and shells.
PRODUCTION_ENVIRONMENTS = frozenset({"prod", "production"})

#: Secrets that are not secrets. A key equal to any of these is treated as
#: absent, so a placeholder copied out of an example file cannot boot a process.
INSECURE_SECRET_DEFAULTS = frozenset(
    {"", "your-secret-key", "secret", "changeme", "default"}
)


def normalize_environment(raw: object) -> str:
    """The canonical form of an ``APP_ENV`` value: stripped and lowercased."""
    return str(raw or "").strip().lower()


def is_local_environment(raw: object) -> bool:
    """Whether *raw* names a developer or test environment."""
    return normalize_environment(raw) in LOCAL_ENVIRONMENTS


def is_production_environment(raw: object) -> bool:
    """Whether *raw* names a production environment."""
    return normalize_environment(raw) in PRODUCTION_ENVIRONMENTS


def is_test_environment(raw: object) -> bool:
    """Whether *raw* names an automated test run."""
    return normalize_environment(raw) in TEST_ENVIRONMENTS


def is_insecure_secret(value: object) -> bool:
    """Whether *value* is missing, a known placeholder, or under 32 characters.

    The length floor lives with the placeholder list because they answer one
    question — "may this key be used to sign anything?" — and splitting them
    let one process check both while another checked only the list.
    """
    if not isinstance(value, str):
        return True
    return value in INSECURE_SECRET_DEFAULTS or len(value) < 32


__all__ = [
    "INSECURE_SECRET_DEFAULTS",
    "LOCAL_ENVIRONMENTS",
    "PRODUCTION_ENVIRONMENTS",
    "TEST_ENVIRONMENTS",
    "is_insecure_secret",
    "is_local_environment",
    "is_production_environment",
    "is_test_environment",
    "normalize_environment",
]
