"""Per-account brute-force protection for login flows.

Per-IP request throttling (e.g. a ``throttle:login`` middleware) slows the
common single-IP brute-force case but leaves a distributed-attack blind
spot: N addresses × the per-IP ceiling against one account stays under
every per-IP limit while still affording a credential-guessing budget.

This helper closes that gap by counting failures **per identity (email)**
in the cache. After ``MAX_FAILURES`` mis-authentications inside the rolling
``FAILURE_WINDOW_SECONDS`` window the account is locked for
``LOCK_DURATION_SECONDS`` regardless of source IP. A successful login
clears the counter so a legitimate typo-then-recover doesn't escalate.

Multi-IP gate (DoS hardening)
-----------------------------
A naive per-email lockout is itself a DoS vector: a handful of wrong
passwords from one throwaway IP would lock the legitimate owner out. The
fix layers an IP-distinctness gate on top of the email counter:

  * Every ``record_failure`` is keyed by (email, ip); the per-email
    counter still bumps so a real brute-force is detected.
  * We track the SET of distinct IPs that failed against this email in the
    window (capped to bound cache footprint).
  * Lockout triggers only when EITHER (a) failures come from >= 2 distinct
    IPs past the per-email threshold (the real distributed shape), OR
    (b) a SINGLE IP issued >= ``SINGLE_IP_LOCK_THRESHOLD`` failures
    (extreme single-source brute force). One IP at 5-19 failures is left to
    the per-IP throttle so a throwaway-IP attack can't lock the owner out.

Storage is the ``Cache`` facade so the state survives restarts and is shared
across workers without sticky sessions. The cache is the authority for this
security gate: when it cannot answer, authentication stops with a retryable
service-unavailable response instead of silently removing account protection.

All thresholds are env-overridable via ``config("security.*")``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging

from cara.configuration import config
from cara.exceptions import (
    AuthenticationConfigurationException,
    ServiceUnavailableException,
)
from cara.facades import Cache, Log
from cara.support import email_mask, mask_ip

from .LoginLocked import LoginLocked

# Maximum entries to keep in the per-email distinct-IP set. Bounds cache
# footprint under a wide-fanout distributed attack — once we observe this
# many distinct IPs against one email we're well past the multi-IP
# threshold and adding more doesn't change the lock decision.
_IP_SET_CAP = 10
_logger = logging.getLogger("cara.authentication.login_attempts")


class LoginAttemptTracker:
    """Stateless Cache-backed brute-force counter.

    Class-method API rather than an instance + DI because the helper has no
    construction-time state — it only reads/writes the cache.
    """

    @staticmethod
    def identifier_digest(value: str) -> str:
        """Opaque, deployment-keyed digest for identifiers in shared storage."""
        return LoginAttemptTracker._digest(value)

    @staticmethod
    def _max_failures() -> int:
        return LoginAttemptTracker._positive_setting(
            "security.login_max_failures",
            5,
        )

    @staticmethod
    def _failure_window_seconds() -> int:
        return LoginAttemptTracker._positive_setting(
            "security.login_failure_window_seconds",
            600,
        )

    @staticmethod
    def _lock_duration_seconds() -> int:
        return LoginAttemptTracker._positive_setting(
            "security.login_lock_duration_seconds",
            3600,
        )

    @staticmethod
    def _single_ip_lock_threshold() -> int:
        """Per-IP failure count that engages the account-wide lock even
        without a second IP in the picture.

        Default 20 is well above a typical per-IP throttle ceiling — a
        single IP still landing 20+ failures against one email past that
        throttle is either misconfigured automation or a determined
        attacker worth stopping.
        """
        return LoginAttemptTracker._positive_setting(
            "security.login_single_ip_lock_threshold",
            20,
        )

    @staticmethod
    def _positive_setting(key: str, default: int) -> int:
        value = config(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise AuthenticationConfigurationException(
                f"{key} must be a positive integer"
            )
        return value

    @staticmethod
    def _failure_key(email: str) -> str:
        return f"login_fails:{LoginAttemptTracker._digest(email)}"

    @staticmethod
    def _lock_key(email: str) -> str:
        return f"login_locked:{LoginAttemptTracker._digest(email)}"

    @staticmethod
    def _ip_set_key(email: str) -> str:
        return f"login_fail_ips:{LoginAttemptTracker._digest(email)}"

    @staticmethod
    def _per_ip_key(email: str, ip: str) -> str:
        return LoginAttemptTracker._per_ip_digest_key(
            email, LoginAttemptTracker._digest(ip)
        )

    @staticmethod
    def _per_ip_digest_key(email: str, ip_digest: str) -> str:
        return f"login_fails_ip:{LoginAttemptTracker._digest(email)}:{ip_digest}"

    @staticmethod
    def _cache_unavailable(
        operation: str,
        email: str,
        exc: Exception,
    ) -> ServiceUnavailableException:
        _logger.error(
            "login security cache %s failed for %s",
            operation,
            email_mask(email),
            exc_info=exc,
        )
        return ServiceUnavailableException(
            "Login security state is temporarily unavailable",
            retry_after=5,
        )

    @classmethod
    def assert_unlocked(cls, email: str | None) -> None:
        """Raise ``LoginLocked`` if the account is currently locked.

        Empty / missing email is a no-op — let validation surface the
        missing-field error from the request layer instead of masking it
        with a 429 here.
        """
        if not email:
            return
        try:
            locked_until = Cache.get(cls._lock_key(email), strict=True)
        except Exception as exc:
            raise cls._cache_unavailable("lock read", email, exc) from exc
        if locked_until:
            # The cache TTL gives us the remaining window; we don't store it
            # explicitly so a recovered cache instance with a fresh key still
            # emits the right retry hint.
            raise LoginLocked(cls._lock_duration_seconds())

    @classmethod
    def _read_ip_set(cls, email: str) -> list[str]:
        """Return the JSON-encoded distinct-IP list for ``email``.

        Stored as a JSON list under one cache key rather than a native set
        (the ``Cache`` facade doesn't expose set ops); a list capped at
        ``_IP_SET_CAP`` gives the same multi-IP-detection contract with a
        much smaller surface to test/fake.
        """
        try:
            raw = Cache.get(cls._ip_set_key(email), strict=True)
        except Exception as exc:
            raise cls._cache_unavailable("IP-set read", email, exc) from exc
        if not raw:
            return []
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise cls._cache_unavailable("IP-set decode", email, exc) from exc
        if not isinstance(decoded, list) or any(
            not isinstance(item, str)
            or len(item) != 64
            or any(char not in "0123456789abcdef" for char in item)
            for item in decoded
        ):
            exc = ValueError("login IP-set state is malformed")
            raise cls._cache_unavailable("IP-set validation", email, exc) from exc
        return decoded

    @classmethod
    def _write_ip_set(cls, email: str, ips: list[str]) -> None:
        """Persist the distinct-IP list under the per-email key with the
        failure-window TTL so it ages out with the counter."""
        try:
            Cache.put(
                cls._ip_set_key(email),
                json.dumps(ips),
                cls._failure_window_seconds(),
                strict=True,
            )
        except Exception as exc:
            raise cls._cache_unavailable("IP-set write", email, exc) from exc

    @classmethod
    def record_failure(cls, email: str | None, ip: str) -> int:
        """Bump the per-email counter; lock the account if the multi-IP
        threshold is crossed.

        Returns the post-increment per-email count (0 only when email is
        missing). A cache failure raises a retryable service-unavailable error.
        """
        if not email:
            return 0
        if not isinstance(ip, str) or not ip.strip():
            raise ValueError("Login failure tracking requires a source IP")
        ip = ip.strip()
        try:
            count = Cache.increment(
                cls._failure_key(email),
                1,
                cls._failure_window_seconds(),
            )
        except Exception as exc:
            raise cls._cache_unavailable("failure increment", email, exc) from exc
        count = int(v) if (v := count) is not None else 0

        # Track the per-IP failure count + add this IP to the per-email
        # distinct-IP set so the multi-IP gate below can decide whether to
        # engage the account-wide lockout.
        per_ip_count = 0
        try:
            per_ip_count = (
                int(v)
                if (
                    v := Cache.increment(
                        cls._per_ip_key(email, ip),
                        1,
                        cls._failure_window_seconds(),
                    )
                )
                is not None
                else 0
            )
        except Exception as exc:
            raise cls._cache_unavailable("per-IP increment", email, exc) from exc

        ip_digest = cls._digest(ip)
        ips = cls._read_ip_set(email)
        if ip_digest not in ips and len(ips) < _IP_SET_CAP:
            ips.append(ip_digest)
            cls._write_ip_set(email, ips)

        distinct_ips = len(cls._read_ip_set(email))

        # Lockout decision:
        #   * Multi-IP path: per-email failures past threshold AND >= 2
        #     distinct IPs in the window → real distributed brute force.
        #   * Single-IP path: one source past the high threshold → a serious
        #     nuisance even past the per-IP throttle.
        #   * Otherwise (5+ failures, one IP under the high threshold): the
        #     per-IP throttle is already slowing them; do NOT engage the
        #     account-wide lockout so a throwaway-IP DoS can't lock the
        #     legitimate owner out.
        should_lock_multi_ip = count >= cls._max_failures() and distinct_ips >= 2
        should_lock_single_ip = per_ip_count >= cls._single_ip_lock_threshold()
        if should_lock_multi_ip or should_lock_single_ip:
            try:
                # Sentinel just needs to be truthy; keep it short so the cache
                # footprint stays tiny under a large-scale stuffing attempt.
                Cache.put(
                    cls._lock_key(email),
                    "1",
                    cls._lock_duration_seconds(),
                    strict=True,
                )
                if should_lock_multi_ip:
                    reason = f"multi_ip(distinct={distinct_ips},count={count})"
                elif should_lock_single_ip:
                    reason = (
                        f"single_ip(ip={mask_ip(ip or '')},per_ip_count={per_ip_count})"
                    )
                Log.warning(
                    f"LoginAttemptTracker: locking account {email_mask(email)} — "
                    f"reason={reason}, window={cls._failure_window_seconds()}s",
                    category="security.login",
                )
            except Exception as exc:
                raise cls._cache_unavailable("lock write", email, exc) from exc
        return count

    @classmethod
    def record_success(cls, email: str | None) -> None:
        """Clear the failure counter after a successful login.

        Lock keys are NOT cleared on success — a locked account would have
        hit the 429 path and never reached the success branch. Belt-and-
        suspenders: even if a race let a request through, leaving the lock
        key in place forces the next request through ``assert_unlocked``.

        The per-email distinct-IP set + every per-IP counter touched in the
        current window are also cleared so the next window starts clean.
        """
        if not email:
            return
        try:
            Cache.forget(cls._failure_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("failure clear", email, exc) from exc
        # Wipe the per-IP counters and the IP set so a fresh login session
        # doesn't carry stale per-IP buckets into the next window's multi-IP
        # threshold calculation.
        for ip_digest in cls._read_ip_set(email):
            try:
                Cache.forget(cls._per_ip_digest_key(email, ip_digest))
            except Exception as exc:
                raise cls._cache_unavailable("per-IP clear", email, exc) from exc
        try:
            Cache.forget(cls._ip_set_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("IP-set clear", email, exc) from exc

    @classmethod
    def clear_lockout(cls, email: str | None) -> None:
        """Clear BOTH the failure counter and the lock sentinel.

        Called from a password-reset success path: proving inbox ownership
        AND knowledge of the freshly-set password is a strictly stronger
        signal than the typed-password budget the lockout guards. Without
        this clear, an attacker who knows a victim's email could trip the
        lockout from a throwaway IP and the owner couldn't recover until the
        TTL elapses — turning the brute-force defense into a DoS vector.

        Cache failures are surfaced. A partial clear remains safe and a retry
        completes the idempotent cleanup.

        Also drops the per-email distinct-IP set + every per-IP counter so
        the multi-IP gate starts cold after a legitimate reset.
        """
        if not email:
            return
        try:
            Cache.forget(cls._failure_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("failure clear", email, exc) from exc
        try:
            Cache.forget(cls._lock_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("lock clear", email, exc) from exc
        for ip_digest in cls._read_ip_set(email):
            try:
                Cache.forget(cls._per_ip_digest_key(email, ip_digest))
            except Exception as exc:
                raise cls._cache_unavailable("per-IP clear", email, exc) from exc
        try:
            Cache.forget(cls._ip_set_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("IP-set clear", email, exc) from exc

    @staticmethod
    def _digest(value: str) -> str:
        """HMAC identifiers before they enter shared cache keys/values."""
        secret_value = config("app.key")
        if not secret_value:
            raise AuthenticationConfigurationException(
                "app.key is required to protect security identifiers"
            )
        if not isinstance(secret_value, str):
            raise AuthenticationConfigurationException("app.key must be text")
        secret = secret_value.encode()
        if len(secret) < 32:
            raise AuthenticationConfigurationException(
                "app.key must contain at least 32 bytes"
            )
        # Derive a purpose-specific key before hashing user identifiers. This
        # keeps identifier hashes separated from every other app-key protocol.
        digest_key = hmac.new(
            secret, b"cara:security-identifier:v1", hashlib.sha256
        ).digest()
        return hmac.new(
            digest_key, value.strip().lower().encode(), hashlib.sha256
        ).hexdigest()
