"""Every exception cara defines must be reachable from ``CaraException``.

§9 asks for ONE taxonomy. An exception rooted straight at a stdlib base
carries no ``status_code``, so ``DefaultExceptionHandler.get_status_code``
falls through to "default to 500 for unknown exceptions" — which is how a
tampered pagination cursor (bad CLIENT input) used to be answered with a
500 plus an ERROR-level traceback, burning the error budget and paging
oncall for a fault the client caused.

The allowlist below is the set of classes still outside the taxonomy on
2026-08-09. It is a ceiling, not a target: adding a NEW orphan fails this
test, and an entry that a later change roots is simply no longer
exercised. Shrink it; never grow it.
"""

from __future__ import annotations

import ast
import pathlib

_CARA_ROOT = pathlib.Path(__file__).resolve().parents[2] / "cara"

_STDLIB_EXCEPTION_BASES = {
    "ArithmeticError",
    "AssertionError",
    "AttributeError",
    "BaseException",
    "Exception",
    "IOError",
    "ImportError",
    "KeyError",
    "LookupError",
    "NotImplementedError",
    "OSError",
    "RuntimeError",
    "StopIteration",
    "TypeError",
    "ValueError",
}

#: Classes outside the ``CaraException`` taxonomy as of 2026-08-09.
#: ``{relative path: {class names}}``. SHRINK ONLY.
#:
#: The ``HttpFake`` / ``Expectation`` entries are deliberate: they inherit
#: ``AssertionError`` so pytest renders them as test failures, and they are
#: never raised in a served request. The rest are genuine debt, each owned
#: by a module outside the exceptions lane.
#:
#: Shrunk on 2026-08-09: ``OptionalDependencyError``,
#: ``UnknownDeclaredResource``, ``ConflictingErrorStatus``,
#: ``UntypedErrorResponse``, ``JobCancelledException``,
#: ``JobThrottledException``, ``UnsafeOutboundUrl``,
#: ``ProcessFailedException`` and ``StepFailed`` were rooted in place, each
#: keeping its stdlib base as a SECOND base where callers catch it.
#: ``CurrencyMismatch`` remains — it is owned by the money lane.
_ALLOWED_ORPHANS: dict[str, set[str]] = {
    "http/client/HttpFake.py": {"StrayHttpRequestError", "FakeExhaustedError"},
    "support/Money.py": {"CurrencyMismatch"},
    "testing/Expectation.py": {"ExpectationFailed"},
}


def _base_names(node: ast.ClassDef) -> list[str]:
    names: list[str] = []
    for base in node.bases:
        if isinstance(base, ast.Name):
            names.append(base.id)
        elif isinstance(base, ast.Attribute):
            names.append(base.attr)
    return names


def _collect() -> tuple[dict[str, list[str]], list[tuple[str, str, list[str]]]]:
    """Return ``(name -> bases)`` for every cara class, plus every class def."""
    bases_by_name: dict[str, list[str]] = {}
    definitions: list[tuple[str, str, list[str]]] = []
    for path in sorted(_CARA_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative = str(path.relative_to(_CARA_ROOT))
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                bases = _base_names(node)
                bases_by_name.setdefault(node.name, bases)
                definitions.append((relative, node.name, bases))
    return bases_by_name, definitions


def _rooted_in_cara_exception(
    bases: list[str], bases_by_name: dict[str, list[str]]
) -> bool:
    seen: set[str] = set()
    frontier = list(bases)
    while frontier:
        name = frontier.pop()
        if name == "CaraException":
            return True
        if name in seen or name not in bases_by_name:
            continue
        seen.add(name)
        frontier.extend(bases_by_name[name])
    return False


def test_every_exception_cara_defines_is_rooted_at_cara_exception() -> None:
    bases_by_name, definitions = _collect()

    offenders: list[str] = []
    for relative, class_name, bases in definitions:
        if class_name == "CaraException":
            continue
        if not any(base in _STDLIB_EXCEPTION_BASES for base in bases):
            continue
        if _rooted_in_cara_exception(bases, bases_by_name):
            continue
        if class_name in _ALLOWED_ORPHANS.get(relative, set()):
            continue
        offenders.append(f"cara/{relative}::{class_name}({', '.join(bases)})")

    assert not offenders, (
        "These exceptions inherit a stdlib base without joining the "
        "CaraException taxonomy, so they carry no status_code and the "
        "handler answers 500 for them (§9). Root them in the matching "
        "cara/exceptions/types module — keep the stdlib base as a SECOND "
        "base where callers rely on it:\n  " + "\n  ".join(sorted(offenders))
    )


def test_invalid_cursor_is_no_longer_an_orphan() -> None:
    """It was the worst of them: tampered client input answered as a 500."""
    from cara.exceptions import CaraException
    from cara.http import InvalidCursor

    assert issubclass(InvalidCursor, CaraException)
    assert "http/Cursor.py" not in _ALLOWED_ORPHANS


def test_the_queue_contracts_survive_joining_the_taxonomy() -> None:
    """The two rooted queue exceptions carry load-bearing worker flags.

    ``JobProcessor`` reads ``do_not_retry`` and ``is_throttle`` off the raised
    exception with ``getattr``. ``do_not_retry`` is why a cancelled job stops
    instead of burning the 1s/5s/30s retry schedule re-running work an
    operator deliberately stopped; ``is_throttle`` is why a throttled job
    spends the STARVATION budget rather than dead-lettering roughly six
    seconds into a 300s gate. Rooting a class is a base-class change and must
    not disturb either — nor the constructor kwargs the raisers pass.
    """
    from cara.exceptions import CaraException, QueueException
    from cara.queues.contracts.JobCancelledException import JobCancelledException
    from cara.queues.contracts.JobThrottledException import JobThrottledException

    for klass in (JobCancelledException, JobThrottledException):
        assert issubclass(klass, QueueException)
        assert issubclass(klass, CaraException)

    cancelled = JobCancelledException("stopped by operator", tracking_id="job-7")
    assert cancelled.do_not_retry is True
    assert cancelled.tracking_id == "job-7"
    assert str(cancelled) == "stopped by operator"

    throttled = JobThrottledException("slow down", key="tenant-3", retry_after=300)
    assert throttled.is_throttle is True
    assert throttled.key == "tenant-3"
    assert throttled.retry_after == 300
    assert str(throttled) == "slow down"

    assert not hasattr(cancelled, "is_throttle"), (
        "a cancellation must not be routed through the throttle budget"
    )
    assert not hasattr(throttled, "do_not_retry"), (
        "a throttle must stay retryable — it is a delay, not a decision"
    )


def test_the_rooted_orphans_keep_the_stdlib_base_callers_catch() -> None:
    """Root them, do not strand their existing ``except`` clauses (§5)."""
    from cara.commands.OptionalDependencyError import OptionalDependencyError
    from cara.exceptions import CaraException
    from cara.openapi.ConflictingErrorStatus import ConflictingErrorStatus
    from cara.openapi.UnknownDeclaredResource import UnknownDeclaredResource
    from cara.openapi.UntypedErrorResponse import UntypedErrorResponse
    from cara.security import UnsafeOutboundUrl
    from cara.support.ProcessFailedException import ProcessFailedException
    from cara.workflows.StepFailed import StepFailed

    runtime_rooted = (
        OptionalDependencyError,
        ProcessFailedException,
        UnknownDeclaredResource,
        ConflictingErrorStatus,
        UntypedErrorResponse,
    )
    for klass in runtime_rooted:
        assert issubclass(klass, CaraException), klass
        assert issubclass(klass, RuntimeError), klass

    assert issubclass(UnsafeOutboundUrl, CaraException)
    assert issubclass(UnsafeOutboundUrl, ValueError), (
        "the SSRF gate's callers catch ValueError around URL policy"
    )
    assert issubclass(StepFailed, CaraException)


def test_the_ssrf_gate_still_rejects_through_its_value_error_base() -> None:
    """Driven through the real gate, not by constructing the class."""
    import pytest as _pytest

    from cara.exceptions import CaraException
    from cara.security import assert_outbound_url_safe

    with _pytest.raises(ValueError) as raised:
        assert_outbound_url_safe("http://169.254.169.254/latest/meta-data/")

    assert isinstance(raised.value, CaraException), (
        "an SSRF rejection outside the taxonomy is invisible to the "
        "except CaraException a worker wraps around an outbound hop"
    )


def test_a_failing_subprocess_raises_inside_the_taxonomy() -> None:
    """Driven through a real non-zero exit, not by constructing the class."""
    import sys

    import pytest as _pytest

    from cara.exceptions import CaraException
    from cara.support.Process import Process
    from cara.support.ProcessFailedException import ProcessFailedException

    result = Process.command([sys.executable, "-c", "raise SystemExit(3)"]).run()

    with _pytest.raises(ProcessFailedException) as raised:
        result.throw_on_failure()

    assert isinstance(raised.value, CaraException)
    assert isinstance(raised.value, RuntimeError)
    assert "exited with code 3" in str(raised.value)
