"""
A schedule spec as the engine's trigger — the trigger side of ``APSchedulerDriver``.

Split out of ``APSchedulerDriver`` for the file budget (DOCTRINE §5) and bound
back onto the class there, so ``_build_trigger`` is its method in everything
but address. The crontab weekday translation lives beside it because this is
the only place a weekday number meets the engine.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from cara.exceptions import InvalidArgumentException

# Cron counts weekdays from Sunday (0 and 7 are Sunday); the engine counts
# from Monday. Numbers are rewritten to these names so an expression keeps
# its crontab meaning.
_CRON_WEEKDAY_NAMES = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")


def _cron_weekday_number(token: str, field: str) -> int:
    if token in _CRON_WEEKDAY_NAMES:
        return _CRON_WEEKDAY_NAMES.index(token)
    if token.isdigit() and 0 <= int(token) <= 7:
        return int(token)
    raise InvalidArgumentException(
        f"Cron day-of-week {field!r}: {token!r} is not a weekday (0-7 or sun-sat)."
    )


def _cron_weekdays(field: Any) -> Any:
    """``field`` in crontab weekday numbering, spelled for the engine.

    Crontab (and Laravel) count 0 = Sunday, 1 = Monday, 7 = Sunday again;
    the engine counts 0 = Monday. Handing numbers straight through ran
    ``* * * * 1`` on Tuesdays and ``1-5`` Tuesday to Saturday. Each item
    that carries a number — a value, a range, ``*/n`` or ``a-b/n`` — is
    expanded with crontab's meaning into weekday names, which both sides
    read alike. Named items (``mon``, ``mon-fri``) and ``*`` pass unchanged.
    """
    if field is None:
        return None
    text = str(field).strip().lower()
    out: list[str] = []
    for item in text.split(","):
        base, slash, step_text = item.partition("/")
        if not any(ch.isdigit() for ch in base) and not (base == "*" and slash):
            out.append(item)
            continue
        if slash and not step_text.isdigit():
            raise InvalidArgumentException(
                f"Cron day-of-week {text!r}: bad step in {item!r}."
            )
        step = int(step_text) if slash else 1
        if step < 1:
            raise InvalidArgumentException(
                f"Cron day-of-week {text!r}: bad step in {item!r}."
            )
        if base == "*":
            first, last = 0, 6
        elif "-" in base:
            low, _, high = base.partition("-")
            first = _cron_weekday_number(low, text)
            last = _cron_weekday_number(high, text)
        else:
            first = _cron_weekday_number(base, text)
            # ``a/n`` runs from ``a`` to the end of the week, as in cron.
            last = max(first, 6) if slash else first
        if first > last:
            raise InvalidArgumentException(
                f"Cron day-of-week {text!r}: range {item!r} runs backwards."
            )
        out.extend(_CRON_WEEKDAY_NAMES[day % 7] for day in range(first, last + 1, step))
    # ``0,7`` or ``1-7`` name a day twice; the engine is given each once.
    return ",".join(dict.fromkeys(out))


def _build_trigger(self, spec: dict[str, Any]):
    sched_type = spec.get("type")
    if not sched_type:
        raise InvalidArgumentException("schedule_spec must include 'type' key.")

    from apscheduler.triggers.cron import CronTrigger  # local: heavy optional dep
    from apscheduler.triggers.date import DateTrigger  # local: heavy optional dep
    from apscheduler.triggers.interval import (
        IntervalTrigger,  # local: heavy optional dep
    )

    # Fall back to the scheduler-configured timezone (e.g. "UTC") when a
    # job spec doesn't set its own. Pre-fix this was bare
    # ``spec.get("timezone")`` → ``None`` for every dict-config cron, and
    # APScheduler resolves ``timezone=None`` to the LOCAL OS timezone at
    # construction, NOT the scheduler default — so on a non-UTC host every
    # "daily at 03:00" cron fired at 03:00 local while its SQL window used
    # pendulum.now("UTC"), shifting the job off its data window. (Masked in
    # prod only because the slim base image happens to be UTC.)
    tz = spec.get("timezone") or self._settings.get("timezone")

    if sched_type == "interval":
        period = {
            unit: spec.get(unit) or 0
            for unit in ("weeks", "days", "hours", "minutes", "seconds")
        }
        # The engine turns an all-zero period into ONE SECOND, so an
        # interval whose fields were dropped or read as 0 from the env
        # fired every second instead of failing.
        if timedelta(**period) <= timedelta(0):
            raise InvalidArgumentException(
                f"Non-positive interval ({', '.join(f'{u}={v}' for u, v in period.items() if v) or 'none'}); "
                "an interval must be longer than zero."
            )
        return IntervalTrigger(**period, timezone=tz)
    elif sched_type == "cron":
        expr = spec.get("expression", "")
        if not isinstance(expr, str) or not expr.strip():
            raise InvalidArgumentException("Cron type requires 'expression' string.")
        parts = expr.split()
        if len(parts) != 5:
            raise InvalidArgumentException(
                "Cron expression must have 5 fields: minute hour day month day_of_week"
            )
        minute, hour, day, month, day_of_week = parts
        return CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=_cron_weekdays(day_of_week),
            timezone=tz,
        )
    elif sched_type == "daily":
        return CronTrigger(
            hour=spec.get("hour", 0),
            minute=spec.get("minute", 0),
            timezone=tz,
        )
    elif sched_type == "hourly":
        return CronTrigger(minute=spec.get("minute", 0), timezone=tz)
    elif sched_type == "weekly":
        return CronTrigger(
            minute=spec.get("minute", 0),
            hour=spec.get("hour", 0),
            day="*",
            month="*",
            day_of_week=_cron_weekdays(spec.get("day_of_week")),
            timezone=tz,
        )
    elif sched_type in ("date", "at"):
        # "at" is the canonical name from the Scheduling contract and
        # ScheduleBuilder.at(); "date" mirrors APScheduler's own
        # trigger name.  Pre-fix only "date" was handled — every
        # ScheduleBuilder.at() call raised InvalidArgumentException
        # because the builder emits type="at" (matching the contract
        # docstring) but the driver only checked for "date".
        run_date = spec.get("run_date")
        if not run_date:
            raise InvalidArgumentException("Date/at type requires 'run_date'.")
        return DateTrigger(run_date=run_date, timezone=tz)
    else:
        raise InvalidArgumentException(f"Unknown schedule type: {sched_type}")
