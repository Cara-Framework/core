"""The per-source poll cooldown — claimed before a run, kept only if spent.

Poll-style jobs (feed sweeps, inventory refreshes, discovery runs) ask a
source for work on a timer. The cooldown is what stops a fleet of workers
asking the same source at once: the first to arrive claims a window with
SETNX + TTL and runs; everyone else sees the live key and stands down.

IT WAS A MIXIN'S PRIVATE PAIR UNTIL 2026-09-11, living inside
``MakesIdempotentBase`` beside the job lock, the result cache and the owner
fence. Two things separate it from those and earn it a file:

* THE LIFECYCLE IS NOT THE LOCK'S. A lock is held FOR a run and released
  after it, success or failure. A cooldown is a promise about the FUTURE —
  "nobody ask this source again for N minutes" — and a run that did no work
  has no right to make it. Keeping it on the exception path is how a
  transient failure silently consumed a whole retry budget: the retries met
  the live window, the gate returned ``None``, and the queue settled each
  one as a success.
* IT IS OPT-IN AND THE REST IS NOT. Every job gets a lock and a key; a job
  gets a cooldown only by returning :meth:`_claim_source_cooldown` from
  ``should_collect_again`` and configuring the attributes below. A mixin
  that carries an opt-in concern is the one part of the base class most
  jobs never touch.

The claim is OWNER-FENCED on release, like the job lock: the delete is a
compare-and-delete on the exact stamp this run wrote, so a window that
lapsed and was re-claimed by a later poll is never deleted from under it.
"""

from __future__ import annotations

import pendulum

from cara.configuration import config
from cara.facades import Cache, Log


class ClaimsSourceCooldown:
    """Claim and release one poll's cooldown window."""

    #: Cache-key namespace for the per-source poll cooldown.
    COOLDOWN_KEY_PREFIX = "collection_cooldown:"

    #: Per-source cooldown windows in minutes, e.g. ``{"reports": 5}``. A
    #: source absent from the map falls back to
    #: ``config("jobs.source_cooldown_minutes")`` and then to
    #: :attr:`default_source_cooldown_minutes`.
    source_cooldown_minutes: dict[str, int] = {}

    #: Cooldown for sources the map does not name.
    default_source_cooldown_minutes = 15

    #: Job attributes whose truthy values, in this order, make one poll
    #: distinct from another. They join the cooldown key after the source.
    cooldown_grain_attrs: tuple[str, ...] = ()

    #: When True, a job that resolves NO grain is never throttled. Set this
    #: wherever a source-only key would be a fleet-wide claim (multi-entity
    #: deployments); leave it False where the source alone IS the identity.
    cooldown_requires_grains = False

    #: The window this run claimed, as ``(key, stamp)`` — the pair
    #: :meth:`_release_source_cooldown` needs to prove ownership. Set to
    #: ``None`` by the orchestrator at the start of every run.
    _idempotency_cooldown_claim: tuple[str, str] | None = None

    def _claim_source_cooldown(self) -> bool:
        """Atomically claim this poll's cooldown window; ``True`` if it won.

        Poll-style jobs (feed sweeps, inventory refreshes, discovery runs)
        are dispatched by schedulers that can fire the same poll twice
        inside one window — two ticks racing, a redelivery, an operator
        re-run. Claiming a per-``(source, grains)`` key with SETNX + TTL
        lets exactly the first caller through; the key expires with the
        cooldown so the next window re-claims it. A ``get`` → ``put``
        check-then-act here does NOT work: both racing callers read
        "expired", both pass, and the upstream is polled twice.

        Three escape hatches, in order:

        * no ``source`` attribute — not a poll, never throttled;
        * a truthy ``force`` attribute — an operator asked for this run;
        * :attr:`cooldown_requires_grains` with no grain resolved — a
          source-only key is a GLOBAL claim across every entity, so fail
          open rather than let one job hold the whole fleet's key. The
          job's own durable gate (claim token, TTL, lifecycle row) is the
          real authority.

        A won claim is remembered on the instance so ``wrap_with_idempotency``
        can hand it back through :meth:`_release_source_cooldown` when the run
        does not spend it (callback raised, lease throttle redelivered).
        """
        source = getattr(self, "source", None)
        if not hasattr(self, "source"):
            return True

        if getattr(self, "force", None):
            Log.debug(
                "Force flag enabled - bypassing cooldown for %s",
                source,
                category="idempotency",
            )
            return True

        cooldown_minutes = self.source_cooldown_minutes.get(
            source,
            int(
                config(
                    "jobs.source_cooldown_minutes",
                    self.default_source_cooldown_minutes,
                )
            ),
        )

        # Grains come from the same identity tuple the idempotency key is
        # built from, so the cooldown key and the idempotency key can never
        # disagree about what makes two dispatches "the same poll".
        grains = [
            str(getattr(self, attr))
            for attr in self.cooldown_grain_attrs
            if getattr(self, attr, None)
        ]
        if self.cooldown_requires_grains and not grains:
            return True

        time_key = self.COOLDOWN_KEY_PREFIX + ":".join([str(source), *grains])

        stamp = pendulum.now("UTC").isoformat()
        if Cache.add(time_key, stamp, cooldown_minutes * 60):
            self._idempotency_cooldown_claim = (time_key, stamp)
            Log.debug(
                "Cooldown claim taken for %s (cooldown: %sm)",
                source,
                cooldown_minutes,
                category="idempotency",
            )
            return True

        Log.debug(
            "Cooldown active for %s (cooldown: %sm)",
            source,
            cooldown_minutes,
            category="idempotency",
        )
        return False

    def _release_source_cooldown(self) -> None:
        """Hand back a cooldown window this run claimed but did not spend.

        Owner-fenced like :meth:`release_job_lock`: the delete is a
        compare-and-delete on the exact stamp this run wrote, so a window that
        lapsed and was re-claimed by a later poll is never deleted from under
        it. Best-effort for the same reason as the lock release — this runs on
        the exception path, and a cache outage here must not replace the
        callback's own exception.
        """
        claim = self._idempotency_cooldown_claim
        if claim is None:
            return
        key, stamp = claim
        try:
            Cache.forget_if(key, stamp)
        except Exception as exc:
            Log.warning(
                "Failed to release cooldown claim %s: %s",
                key,
                exc,
                category="idempotency",
            )
        finally:
            self._idempotency_cooldown_claim = None
