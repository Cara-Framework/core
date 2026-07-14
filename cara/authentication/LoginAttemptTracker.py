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

Storage is the ``Cache`` facade so the state survives restarts, is shared
across workers without sticky sessions, and degrades to "no lock" when the
cache is down (the per-IP throttle is still enforced; we prefer that to
hard-failing every login).

All thresholds are env-overridable via ``config("security.*")``.
"""

from __future__ import annotations

import hashlib
import hmac
import json

from cara.configuration import config
from cara.exceptions import AccountLockedException, AuthenticationConfigurationException
from cara.facades import Cache, Log
from cara.support import email_mask, mask_ip


class LoginLocked(AccountLockedException):
    """Raised when the requested account is currently locked-out.

    Maps to HTTP 429 (via :class:`AccountLockedException`) — Too-Many-
    Requests rather than 403/401 because the credentials might be correct;
    the lockout is policy, not authorisation. The retry-after window is
    included so well-behaved clients know when to come back.
    """

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__(
            "Too many failed login attempts. Try again in "
            f"{max(1, retry_after_seconds // 60)} minute(s).",
            retry_after_seconds=retry_after_seconds,
        )


# Maximum entries to keep in the per-email distinct-IP set. Bounds cache
# footprint under a wide-fanout distributed attack — once we observe this
# many distinct IPs against one email we're well past the multi-IP
# threshold and adding more doesn't change the lock decision.
_IP_SET_CAP = 10


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
        return int(config("security.login_max_failures", 5))

    @staticmethod
    def _failure_window_seconds() -> int:
        return int(config("security.login_failure_window_seconds", 600))

    @staticmethod
    def _lock_duration_seconds() -> int:
        return int(config("security.login_lock_duration_seconds", 3600))

    @staticmethod
    def _single_ip_lock_threshold() -> int:
        """Per-IP failure count that engages the account-wide lock even
        without a second IP in the picture.

        Default 20 is well above a typical per-IP throttle ceiling — a
        single IP still landing 20+ failures against one email past that
        throttle is either misconfigured automation or a determined
        attacker worth stopping.
        """
        return int(config("security.login_single_ip_lock_threshold", 20))

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
            locked_until = Cache.get(cls._lock_key(email))
        except Exception as e:
            # Cache-down → no lock. The per-IP throttle is still in force;
            # we don't want every login to 500 because the cache blipped.
            Log.debug(
                f"LoginAttemptTracker.assert_unlocked: cache read failed for {email_mask(email)}: {e}",
                category="security.login",
            )
            return
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
            raw = Cache.get(cls._ip_set_key(email))
        except Exception as e:
            Log.warning(
                f"[LoginAttemptTracker] failed to read IP set for {email_mask(email)}: {e}",
            )
            return []
        if not raw:
            return []
        if isinstance(raw, list):
            return [str(x) for x in raw]
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, list):
                return [str(x) for x in decoded]
        except (TypeError, ValueError) as exc:
            Log.debug("[LoginAttemptTracker] failed to decode IP set JSON: %s", exc)
        return []

    @classmethod
    def _write_ip_set(cls, email: str, ips: list[str]) -> None:
        """Persist the distinct-IP list under the per-email key with the
        failure-window TTL so it ages out with the counter."""
        try:
            Cache.put(
                cls._ip_set_key(email),
                json.dumps(ips),
                cls._failure_window_seconds(),
            )
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker: ip-set write failed for {email_mask(email)}: {e}",
                category="security.login",
            )

    @classmethod
    def record_failure(cls, email: str | None, ip: str | None = None) -> int:
        """Bump the per-email counter; lock the account if the multi-IP
        threshold is crossed.

        ``ip`` is the request's source IP. Passing ``None`` is supported for
        backwards compatibility — the helper still bumps the email counter
        but the multi-IP gate falls back to "single unknown source".

        Returns the post-increment per-email count (0 if email is falsy /
        cache write failed). Callers don't need to act on it — the next
        ``assert_unlocked`` surfaces the lock.
        """
        if not email:
            return 0
        try:
            count = Cache.increment(
                cls._failure_key(email),
                1,
                cls._failure_window_seconds(),
            )
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker.record_failure: cache increment failed for {email_mask(email)}: {e}",
                category="security.login",
            )
            return 0
        count = int(v) if (v := count) is not None else 0

        # Track the per-IP failure count + add this IP to the per-email
        # distinct-IP set so the multi-IP gate below can decide whether to
        # engage the account-wide lockout.
        per_ip_count = 0
        if ip:
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
            except Exception as e:
                Log.debug(
                    f"LoginAttemptTracker.record_failure: per-IP increment failed "
                    f"for {email_mask(email)}/{mask_ip(ip or '')}: {e}",
                    category="security.login",
                )

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
        #   * No-IP fallback: ``ip=None`` → fall back to per-email-only.
        #   * Otherwise (5+ failures, one IP under the high threshold): the
        #     per-IP throttle is already slowing them; do NOT engage the
        #     account-wide lockout so a throwaway-IP DoS can't lock the
        #     legitimate owner out.
        should_lock_multi_ip = count >= cls._max_failures() and distinct_ips >= 2
        should_lock_single_ip = per_ip_count >= cls._single_ip_lock_threshold()
        should_lock_legacy_no_ip = ip is None and count >= cls._max_failures()

        if should_lock_multi_ip or should_lock_single_ip or should_lock_legacy_no_ip:
            try:
                # Sentinel just needs to be truthy; keep it short so the cache
                # footprint stays tiny under a large-scale stuffing attempt.
                Cache.put(cls._lock_key(email), "1", cls._lock_duration_seconds())
                if should_lock_multi_ip:
                    reason = f"multi_ip(distinct={distinct_ips},count={count})"
                elif should_lock_single_ip:
                    reason = (
                        f"single_ip(ip={mask_ip(ip or '')},per_ip_count={per_ip_count})"
                    )
                else:
                    reason = f"legacy_no_ip(count={count})"
                Log.warning(
                    f"LoginAttemptTracker: locking account {email_mask(email)} — "
                    f"reason={reason}, window={cls._failure_window_seconds()}s",
                    category="security.login",
                )
            except Exception as e:
                Log.warning(
                    f"LoginAttemptTracker.record_failure: lock write failed for {email_mask(email)}: {e}",
                    category="security.login",
                )
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
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker.record_success: cache forget failed for {email_mask(email)}: {e}",
                category="security.login",
            )
        # Wipe the per-IP counters and the IP set so a fresh login session
        # doesn't carry stale per-IP buckets into the next window's multi-IP
        # threshold calculation.
        for ip_digest in cls._read_ip_set(email):
            try:
                Cache.forget(cls._per_ip_digest_key(email, ip_digest))
            except Exception as e:
                Log.debug(
                    f"LoginAttemptTracker.record_success: per-IP forget failed "
                    f"for {email_mask(email)}: {e}",
                    category="security.login",
                )
        try:
            Cache.forget(cls._ip_set_key(email))
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker.record_success: ip-set forget failed for {email_mask(email)}: {e}",
                category="security.login",
            )

    @classmethod
    def clear_lockout(cls, email: str | None) -> None:
        """Clear BOTH the failure counter and the lock sentinel.

        Called from a password-reset success path: proving inbox ownership
        AND knowledge of the freshly-set password is a strictly stronger
        signal than the typed-password budget the lockout guards. Without
        this clear, an attacker who knows a victim's email could trip the
        lockout from a throwaway IP and the owner couldn't recover until the
        TTL elapses — turning the brute-force defense into a DoS vector.

        Forgets both keys (one ``forget`` each) so a half-cleared state never
        persists. Cache-down degrades to "lockout stays in place" — same
        fail-open policy as the other methods.

        Also drops the per-email distinct-IP set + every per-IP counter so
        the multi-IP gate starts cold after a legitimate reset.
        """
        if not email:
            return
        try:
            Cache.forget(cls._failure_key(email))
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker.clear_lockout: counter forget failed for {email_mask(email)}: {e}",
                category="security.login",
            )
        try:
            Cache.forget(cls._lock_key(email))
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker.clear_lockout: lock forget failed for {email_mask(email)}: {e}",
                category="security.login",
            )
        for ip_digest in cls._read_ip_set(email):
            try:
                Cache.forget(cls._per_ip_digest_key(email, ip_digest))
            except Exception as e:
                Log.debug(
                    f"LoginAttemptTracker.clear_lockout: per-IP forget failed "
                    f"for {email_mask(email)}: {e}",
                    category="security.login",
                )
        try:
            Cache.forget(cls._ip_set_key(email))
        except Exception as e:
            Log.debug(
                f"LoginAttemptTracker.clear_lockout: ip-set forget failed for {email_mask(email)}: {e}",
                category="security.login",
            )

    @staticmethod
    def _digest(value: str) -> str:
        """HMAC identifiers before they enter shared cache keys/values."""
        secret_value = (
            config("security.identifier_hmac_key")
            or config("app.key")
            or config("auth.guards.jwt.secret")
        )
        if not secret_value:
            raise AuthenticationConfigurationException(
                "A security identifier HMAC key, app key, or JWT secret is required"
            )
        secret = str(secret_value).encode()
        return hmac.new(
            secret, value.strip().lower().encode(), hashlib.sha256
        ).hexdigest()
