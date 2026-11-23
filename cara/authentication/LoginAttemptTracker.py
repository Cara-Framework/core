"""Per-account brute-force protection for login flows.

Per-IP request throttling (e.g. a ``throttle:login`` middleware) slows the
common single-IP brute-force case but leaves a distributed-attack blind
spot: N addresses × the per-IP ceiling against one account stays under
every per-IP limit while still affording a credential-guessing budget.

This helper closes that gap by counting failures **per identity (email)**
in the cache, per source and across sources, inside the rolling
``FAILURE_WINDOW_SECONDS`` window. A successful login clears the counters so
a legitimate typo-then-recover doesn't escalate.

The source is locked, not the account (DoS hardening)
-----------------------------------------------------
A per-email lockout is itself a DoS vector: anyone who knows an address could
keep its owner out of sign-in, sudo and the impersonation proof. So the lock
lands on the attempt:

  * ``MAX_FAILURES`` misses from one source lock THAT SOURCE out of that
    account for ``LOCK_DURATION_SECONDS``. The owner elsewhere is untouched.
  * Only ``ACCOUNT_LOCK_FAILURES`` misses across sources lock the account.
    With every source stopped at ``MAX_FAILURES``, that takes ten sources by
    default: a real distributed guess, the shape this helper exists for, and
    a bound on how many guesses any pool of addresses can buy.
  * A password reset opens both, because proving the inbox outranks both.

Storage is the ``Cache`` facade so the state survives restarts and is shared
across workers without sticky sessions. The cache is the authority for this
security gate: when it cannot answer, authentication stops with a retryable
service-unavailable response instead of silently removing account protection.

An admitted attempt holds a reservation (in-flight accounting)
---------------------------------------------------------------
``assert_unlocked`` used to be check-then-act: attempts between the check and
their recorded outcome were counted nowhere, so a burst from many sources
all passed the check before the first failure landed (800 admitted against a
50-failure account lock). Now an admitted attempt RESERVES a slot against the
account limit with an atomic counter: it is admitted only while recorded
failures plus attempts in flight (itself included) stay within
``login_account_lock_failures``. ``record_failure`` turns the reservation into
a counted failure (the failure is counted before the slot is released, so
the attempt is never counted nowhere); ``record_success`` releases it. An
attempt that never records an outcome (a crashed or cancelled request, the
password step of a 2FA sign-in) is released by expiry: the counters are
windowed by ``login_attempt_reservation_seconds`` and a window is counted for
two windows, so a lost slot frees itself within twice that time.

The release pairs with the reservation THIS request made (held on the
request object, or on the calling context outside a request), never with a
counter guessed from the clock: a release into the wrong window would
refund a slot another attempt still holds.

Only the account limit is reserved. One source's burst is bounded by the
per-IP ``throttle:login`` the login routes carry; a per-source reservation
would hold a slot across the 2FA step (the password step records no outcome)
and refuse the owner's second factor after four typos.

All thresholds are env-overridable via ``config("security.*")``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import weakref
from contextvars import ContextVar
from typing import Any

from cara.configuration import config
from cara.exceptions import (
    AuthenticationConfigurationException,
    ServiceUnavailableException,
)
from cara.facades import Cache, Log
from cara.http import current_request
from cara.support import email_mask, mask_ip

from .LoginLocked import LoginLocked

# Maximum entries to keep in the per-email distinct-IP set, whose only job is
# finding the per-IP counters to clear. Bounds cache footprint under a
# wide-fanout distributed attack; a counter past it ages out with its window.
_IP_SET_CAP = 10
# Maximum locked sources one account remembers for a password reset to open.
# One past it stays locked until its own TTL ends, which is the safe direction.
_LOCKED_SOURCES_CAP = 64
_logger = logging.getLogger("cara.authentication.login_attempts")
# The reservation's own clock, a module attribute so a test moves time
# without sleeping. Windows only need to agree to well under a window across
# hosts, which NTP-synchronised hosts do.
_clock = time.time
# Reservations this request holds, keyed weakly on the request object: the
# tracker runs in a worker thread per call (``run_in_thread`` copies the
# context, so a ContextVar set there is lost), while the request object is
# the one thing the check and the outcome share.
_HELD_LOCK = threading.Lock()
_HELD_BY_REQUEST: weakref.WeakKeyDictionary[Any, list[str]] = weakref.WeakKeyDictionary()
_HELD_OUTSIDE_REQUEST: ContextVar[list[str] | None] = ContextVar(
    "cara_login_reservations", default=None
)


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
    def _account_lock_failures() -> int:
        """Failures across every source that engage the account-wide lock.

        Default 50: each source stops at ``login_max_failures``, so reaching
        it takes ten sources in one window. Under NIST SP 800-63B's ceiling
        of 100 consecutive failures on one account.
        """
        return LoginAttemptTracker._positive_setting(
            "security.login_account_lock_failures",
            50,
        )

    @staticmethod
    def _reservation_seconds() -> int:
        """Window of the in-flight counters. A slot whose attempt never
        records an outcome frees itself within twice this; it must outlast
        a slow sign-in (password hashing plus a database round trip)."""
        return LoginAttemptTracker._positive_setting(
            "security.login_attempt_reservation_seconds",
            30,
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
    def _in_flight_prefix(email: str) -> str:
        return f"login_inflight:{LoginAttemptTracker._digest(email)}:"

    @staticmethod
    def _source_lock_key(email: str, ip_digest: str) -> str:
        return f"login_locked_ip:{LoginAttemptTracker._digest(email)}:{ip_digest}"

    @staticmethod
    def _locked_sources_key(email: str) -> str:
        return f"login_locked_ips:{LoginAttemptTracker._digest(email)}"

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
            "Sign-in isn't working right now. Try again in a minute.",
            retry_after=5,
        )

    @classmethod
    def assert_unlocked(cls, email: str | None, ip: str | None) -> None:
        """Raise ``LoginLocked`` if the account, or this source against it, is
        currently locked.

        Empty / missing email is a no-op — let validation surface the
        missing-field error from the request layer instead of masking it
        with a 429 here.

        With a source ``ip`` the check is the attempt's admission: it reserves
        a slot against the account limit, released by ``record_failure`` /
        ``record_success`` or by expiry (see the module docstring), and
        refuses with ``LoginLocked`` while the slots are all held. Without
        one it only reads the locks (``AccountLock`` asks that way after a
        failure); nothing is reserved because no outcome will follow.
        """
        if not email:
            return
        try:
            locked_until = Cache.get(cls._lock_key(email), strict=True)
            if not locked_until and isinstance(ip, str) and ip.strip():
                locked_until = Cache.get(
                    cls._source_lock_key(email, cls._digest(ip.strip())), strict=True
                )
        except Exception as exc:
            raise cls._cache_unavailable("lock read", email, exc) from exc
        if locked_until:
            # The cache TTL gives us the remaining window; we don't store it
            # explicitly so a recovered cache instance with a fresh key still
            # emits the right retry hint.
            raise LoginLocked(cls._lock_duration_seconds())
        if isinstance(ip, str) and ip.strip():
            cls._reserve(email)

    @classmethod
    def _reserve(cls, email: str) -> None:
        """Take one in-flight slot against the account limit, or refuse.

        Order matters: the slot is taken (atomic INCR) BEFORE the earlier
        window and the failure count are read, and ``record_failure`` counts
        the failure BEFORE it releases its slot. Any attempt is therefore in
        the in-flight sum, the failure count, or both at every instant, so
        concurrent admissions cannot all see room that only one of them has.
        """
        window = cls._reservation_seconds()
        index = int(_clock() // window)
        prefix = cls._in_flight_prefix(email)
        key = f"{prefix}{index}"
        # A window's counter is read during its own window and the next.
        ttl = 2 * window + 1
        try:
            in_flight = Cache.increment(key, 1, ttl)
            # INCRBY 0 is the counter namespace's read (``get`` reads values,
            # not counters); it may create an empty counter, which is harmless.
            in_flight += Cache.increment(f"{prefix}{index - 1}", 0, ttl)
            failures = Cache.increment(
                cls._failure_key(email), 0, cls._failure_window_seconds()
            )
        except Exception as exc:
            raise cls._cache_unavailable("reservation", email, exc) from exc
        if int(failures) + int(in_flight) > cls._account_lock_failures():
            cls._refund(email, key)
            # No lock was written: the slots free themselves as the attempts
            # in flight finish, so the hint is the reservation window, not
            # the lock duration.
            raise LoginLocked(window)
        held = cls._held()
        with _HELD_LOCK:
            held.append(key)

    @staticmethod
    def _held() -> list[str]:
        """The reservations the current request (or, outside a request, the
        calling context) holds."""
        request = current_request.get(None)
        if request is None:
            held = _HELD_OUTSIDE_REQUEST.get()
            if held is None:
                held = []
                _HELD_OUTSIDE_REQUEST.set(held)
            return held
        with _HELD_LOCK:
            held = _HELD_BY_REQUEST.get(request)
            if held is None:
                held = _HELD_BY_REQUEST[request] = []
            return held

    @classmethod
    def _release(cls, email: str) -> None:
        """Release the newest slot this request holds for ``email``. An
        attempt whose slot is not held here (it expired with its window, or
        the check ran in another request) releases nothing: refunding a
        counter this request did not raise would free another attempt's slot.
        """
        prefix = cls._in_flight_prefix(email)
        held = cls._held()
        with _HELD_LOCK:
            position = next(
                (i for i in range(len(held) - 1, -1, -1) if held[i].startswith(prefix)),
                None,
            )
            if position is None:
                return
            # Popped before the refund, so a retried outcome cannot refund twice.
            key = held.pop(position)
        cls._refund(email, key)

    @classmethod
    def _refund(cls, email: str, key: str) -> None:
        ttl = 2 * cls._reservation_seconds() + 1
        try:
            if Cache.increment(key, -1, ttl) < 0:
                # The window's counter expired under the attempt and the
                # refund re-created it below zero; undo, or the negative
                # would hand a later attempt a slot nobody released.
                Cache.increment(key, 1, ttl)
        except Exception as exc:
            raise cls._cache_unavailable("reservation release", email, exc) from exc

    @classmethod
    def _read_ip_set(cls, email: str) -> list[str]:
        """Return the JSON-encoded distinct-IP list for ``email``."""
        return cls._read_digests(cls._ip_set_key(email), email)

    @classmethod
    def _read_digests(cls, key: str, email: str) -> list[str]:
        """Return the JSON-encoded digest list under ``key``.

        Stored as a JSON list under one cache key rather than a native set
        (the ``Cache`` facade doesn't expose set ops); a capped list gives the
        same contract with a much smaller surface to test/fake.
        """
        try:
            raw = Cache.get(key, strict=True)
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
        """Bump the per-source and per-email counters; lock the source at
        ``login_max_failures``, the account at ``login_account_lock_failures``.

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
        # distinct-IP set, which is how a success finds the counters to clear.
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

        if per_ip_count >= cls._max_failures():
            cls._lock_source(email, ip, ip_digest)
        if count >= cls._account_lock_failures():
            cls._lock_account(email, count)
        # Last: the failure is counted above, so freeing the slot now never
        # leaves the attempt counted nowhere.
        cls._release(email)
        return count

    @classmethod
    def _lock_source(cls, email: str, ip: str, ip_digest: str) -> None:
        """Lock this source out of this account, and remember it for a reset:
        the lock outlives the failure window, and the IP set with it."""
        duration = cls._lock_duration_seconds()
        locked = cls._read_digests(cls._locked_sources_key(email), email)
        if ip_digest not in locked and len(locked) < _LOCKED_SOURCES_CAP:
            locked.append(ip_digest)
        try:
            # Sentinel just needs to be truthy; keep it short so the cache
            # footprint stays tiny under a large-scale stuffing attempt.
            Cache.put(cls._source_lock_key(email, ip_digest), "1", duration, strict=True)
            Cache.put(
                cls._locked_sources_key(email),
                json.dumps(locked),
                duration,
                strict=True,
            )
        except Exception as exc:
            raise cls._cache_unavailable("source lock write", email, exc) from exc
        Log.warning(
            f"LoginAttemptTracker: locking source {mask_ip(ip)} out of "
            f"{email_mask(email)}, window={cls._failure_window_seconds()}s",
            category="security.login",
        )

    @classmethod
    def _lock_account(cls, email: str, count: int) -> None:
        try:
            Cache.put(
                cls._lock_key(email),
                "1",
                cls._lock_duration_seconds(),
                strict=True,
            )
        except Exception as exc:
            raise cls._cache_unavailable("lock write", email, exc) from exc
        Log.warning(
            f"LoginAttemptTracker: locking account {email_mask(email)} — "
            f"reason=failures_across_sources(count={count}), "
            f"window={cls._failure_window_seconds()}s",
            category="security.login",
        )

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
        # doesn't carry stale per-IP buckets into the next window.
        for ip_digest in cls._read_ip_set(email):
            try:
                Cache.forget(cls._per_ip_digest_key(email, ip_digest))
            except Exception as exc:
                raise cls._cache_unavailable("per-IP clear", email, exc) from exc
        try:
            Cache.forget(cls._ip_set_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("IP-set clear", email, exc) from exc
        cls._release(email)

    @classmethod
    def clear_lockout(cls, email: str | None) -> None:
        """Clear the failure counters and every lock, account and source.

        Called from a password-reset success path: proving inbox ownership
        AND knowledge of the freshly-set password is a strictly stronger
        signal than the typed-password budget the lockout guards. Without
        this clear, an attacker who knows a victim's email could trip the
        lockout from a throwaway IP and the owner couldn't recover until the
        TTL elapses — turning the brute-force defense into a DoS vector.

        Cache failures are surfaced. A partial clear remains safe and a retry
        completes the idempotent cleanup.

        Also drops the per-email distinct-IP set + every per-IP counter so
        the counters start cold after a legitimate reset.
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
        for ip_digest in cls._read_digests(cls._locked_sources_key(email), email):
            try:
                Cache.forget(cls._source_lock_key(email, ip_digest))
            except Exception as exc:
                raise cls._cache_unavailable("source lock clear", email, exc) from exc
        try:
            Cache.forget(cls._locked_sources_key(email))
        except Exception as exc:
            raise cls._cache_unavailable("locked-sources clear", email, exc) from exc
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
