"""Broker configuration guards for a deployable's ``config/queue.py``.

Config-layer support, like the config modules it serves: it reads ``env()`` at
import time on purpose (the app-code "use ``config()``, never ``env()``" rule
does not apply — ``config()`` is not loaded yet when a config module executes).

Every guard here fails CLOSED and fails at BOOT. A queue misconfiguration is
not a degraded feature: an unsigned envelope is arbitrary code execution at the
consumer, a shared vhost lets another product's worker eat these messages, and
a plaintext connection to an external broker hands both away. None of those
announce themselves at runtime, so the only safe place to notice them is the
process that refuses to start.

Product identity is a PARAMETER, never a constant: ``require_isolated_vhost``
takes the vhost name the deployable must own.
"""

from __future__ import annotations

from cara.environment import env
from cara.security.SigningKeys import require_signing_keyring

__all__ = [
    "AMQP_MAX_PRIORITY",
    "AMQP_PRIORITY_LEVELS",
    "PRIVATE_BROKER_HOSTS",
    "queue_signing_keyring",
    "rabbit_broker_access",
    "rabbit_credentials",
    "rabbit_scheme",
    "require_isolated_vhost",
]

# The priority vocabulary the AMQP driver validates a job's tier against, and
# the exact set the delivery ledger's priority CHECK constraint accepts. The
# driver refuses any tier outside ``priority_levels`` before persisting, so an
# unset vocabulary makes EVERY dispatch raise — including the default tier,
# which cannot validate itself. ``max_priority`` bounds the integer mapping and
# must cover the highest level.
AMQP_MAX_PRIORITY: int = 4
AMQP_PRIORITY_LEVELS: dict[str, int] = {
    "critical": 4,
    "high": 3,
    "default": 2,
    "low": 1,
}

# Hosts a plaintext AMQP connection cannot leave the machine or the compose
# network to reach.
PRIVATE_BROKER_HOSTS: frozenset[str] = frozenset(
    {"127.0.0.1", "localhost", "::1", "rabbitmq"}
)

_BROKER_ACCESS_LEVELS: frozenset[str] = frozenset(
    {"none", "consume", "publish", "topology", "full"}
)
_PRODUCTION_ENVIRONMENTS: frozenset[str] = frozenset({"production", "prod"})

# A process with no broker identity gets an impossible credential pair rather
# than a blank one, so a regression that opens a connection anyway fails
# authentication instead of silently succeeding as an anonymous user.
_DISABLED_BROKER_CREDENTIAL = "__broker_access_disabled__"


def _app_env() -> str:
    return str(env("APP_ENV", "") or "").strip().lower()


def _is_production() -> bool:
    return _app_env() in _PRODUCTION_ENVIRONMENTS


def rabbit_broker_access() -> str:
    """Return the validated broker capability assigned to this process.

    Production forbids the catch-all ``full`` identity: a relay only needs to
    publish, a worker only needs to consume, and a scheduler needs no broker at
    all. One shared superuser turns any one compromised process into every
    other one's authority.
    """
    access = str(env("QUEUE_BROKER_ACCESS", "full") or "").strip().lower()
    if access not in _BROKER_ACCESS_LEVELS:
        raise RuntimeError(
            "QUEUE_BROKER_ACCESS must be none, consume, publish, topology or full."
        )
    if _is_production() and access == "full":
        raise RuntimeError(
            "QUEUE_BROKER_ACCESS=full is forbidden in production; use a "
            "capability-specific RabbitMQ identity."
        )
    return access


def rabbit_credentials(access: str | None = None) -> tuple[str, str]:
    """Read broker credentials; refuse the ``guest`` default in production.

    ``guest:guest`` is the well-known RabbitMQ default. Images usually bind it
    to loopback, but a deployment that exposes 5672 across a cluster network
    has no authentication at all, and nothing about that state is visible from
    the application side.

    Pass ``access`` (from :func:`rabbit_broker_access`) to additionally honour
    the capability gate: a process assigned ``none`` gets a sentinel pair that
    cannot authenticate anywhere. Omit it and only the guest guard applies.

    Returns ``(username, password)`` for the ``DRIVERS["amqp"]`` block.
    """
    if access is not None and str(access).strip().lower() == "none":
        return _DISABLED_BROKER_CREDENTIAL, _DISABLED_BROKER_CREDENTIAL

    username = env("RABBIT_USERNAME", "guest")
    password = env("RABBIT_PASSWORD", "guest")
    if _is_production() and (username == "guest" or password == "guest"):
        raise RuntimeError(
            "RABBIT_USERNAME / RABBIT_PASSWORD must not be left at the "
            "'guest' default in production. Configure dedicated credentials "
            "before booting."
        )
    return username, password


def require_isolated_vhost(product: str, default: str | None = None) -> str:
    """Return the product-isolated vhost, refusing a shared root in production.

    Broker infrastructure is shared between products in some environments.
    Falling back to RabbitMQ's root vhost lets a worker with an overlapping
    queue name consume another product's messages — a failure with no error and
    no log line anywhere, because from the broker's side nothing went wrong.

    ``product`` is the vhost this deployable must own; production pins it
    exactly.
    """
    product = str(product).strip()
    if not product:
        raise RuntimeError("require_isolated_vhost needs a non-empty product vhost.")
    vhost = str(env("RABBIT_VHOST", default if default is not None else product) or "")
    vhost = vhost.strip()
    if not vhost:
        raise RuntimeError("RABBIT_VHOST must not be empty.")
    if _is_production() and vhost != product:
        raise RuntimeError(
            f"RABBIT_VHOST must be the isolated {product!r} vhost in production."
        )
    return vhost


def rabbit_scheme(private_hosts: frozenset[str] = PRIVATE_BROKER_HOSTS) -> str:
    """Return a validated AMQP transport scheme.

    Plain AMQP is allowed only for loopback or a private single-host broker
    service. Any production broker reached over a network must authenticate and
    encrypt the transport, because the credentials guarded above travel on it.
    """
    scheme = str(env("RABBIT_SCHEME", "amqp") or "").strip().lower()
    if scheme not in {"amqp", "amqps"}:
        raise RuntimeError("RABBIT_SCHEME must be either 'amqp' or 'amqps'.")

    host = str(env("RABBIT_HOST", "127.0.0.1") or "").strip().lower()
    if _is_production() and host not in private_hosts and scheme != "amqps":
        raise RuntimeError(
            "RABBIT_SCHEME=amqps is required for an external production broker."
        )
    return scheme


def queue_signing_keyring() -> tuple:
    """Validate the job-envelope signing keyring; refuse to boot without it.

    Every AMQP envelope is HMAC-signed and the consumer IMPORTS THE CLASS the
    envelope names, so the signature is the only thing between the broker and
    arbitrary code execution. There is no unsigned path: the serializer rejects
    an empty key id outright, which is why an unset keyring means no dispatch
    at all rather than degraded security.

    Fail-closed in EVERY environment, not just production — a development
    default would be the one value most likely to reach a real broker.

    ``QUEUE_SIGNING_PREVIOUS_KEYS`` must be explicit (``{}`` when not rotating)
    so an operator mid-rotation cannot silently drop the old key and strand
    in-flight envelopes. The disallowed set pins this key as independent from
    the application and cache keys: reusing one would let a leak in either
    subsystem forge jobs.

    Returns ``(key_id, keyring)`` for the ``DRIVERS["amqp"]`` block. Publisher
    and consumer must share it — a keyring that differs between the two fails
    every signature check.
    """
    return require_signing_keyring(
        active_key_id=env("QUEUE_SIGNING_KEY_ID", ""),
        active_key=env("QUEUE_SIGNING_KEY", ""),
        previous_keys=env("QUEUE_SIGNING_PREVIOUS_KEYS", None),
        disallowed={
            "APP_KEY": env("APP_KEY", ""),
            "CACHE_SIGNING_KEY": env("CACHE_SIGNING_KEY", ""),
        },
    )
