"""Gate: a reference row describes the class it names — all of it, only it.

The class scanner read the 2,500 characters after a ``class`` line, whatever
they held. Audit 2026-11-23 found what that published:

* an attribute behind a long docstring was never reached — three Synkronus
  models with a ``__table__`` were listed as having none, two Cheapa envelope
  jobs as having no queue;
* a short class took the attributes of the class AFTER it;
* the timeout pattern stopped at the first digits it met, so
  ``timeout = 60 * 60`` was printed as a 60-second budget and
  ``4 * MARKETPLACE_CALL_SECONDS`` as 4 (eleven Synkronus jobs, one Cheapa).

A class is now read to its own end and no further, and a timeout is a number
only when the class states one.
"""

from __future__ import annotations

from cara.docs import command_names
from cara.docs.Inventory import gen_jobs, gen_models, scan_classes

from ._fixtures import make_checkout, manifest_for, write

_LONG_DOCSTRING = '"""' + ("Why this exists, at length. " * 120) + '"""'


def _models_page(root) -> str:
    manifest = manifest_for(root, "alpha")
    gen_models(manifest, "2026-01-01 00:00", lambda _line: None)
    return (manifest.reference / "models.md").read_text(encoding="utf-8")


def _jobs_page(root) -> str:
    manifest = manifest_for(root, "alpha")
    gen_jobs(manifest, "2026-01-01 00:00", lambda _line: None)
    return (manifest.reference / "jobs.md").read_text(encoding="utf-8")


def test_an_attribute_behind_a_long_docstring_is_still_the_classes(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(
        root / "commons" / "models" / "Guardrail.py",
        f"""
class Guardrail(Model):
    {_LONG_DOCSTRING}

    __table__ = "guardrail"
""",
    )
    assert len(_LONG_DOCSTRING) > 2500

    assert "| Guardrail | `guardrail` |" in _models_page(root)


def test_a_class_does_not_take_the_attributes_of_the_class_after_it(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(
        root / "commons" / "models" / "Widget.py",
        '''
class Stamps(Base):
    """Shared columns; not a table."""


class Widget(Model):
    __table__ = "widget"
''',
    )

    page = _models_page(root)

    # ``Stamps`` used to be listed as a second model backed by ``widget``.
    assert "| Widget | `widget` |" in page
    assert "Stamps" not in page
    assert "_Total: 1 models._" in page


def test_a_computed_timeout_is_not_printed_as_its_first_digits(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    jobs = root / "services" / "app" / "jobs"
    write(jobs / "HourJob.py", "class HourJob(BaseJob):\n    timeout = 60 * 60\n")
    write(
        jobs / "CallsJob.py",
        "class CallsJob(BaseJob):\n    timeout = 4 * MARKETPLACE_CALL_SECONDS\n",
    )
    write(
        jobs / "PlainJob.py",
        "class PlainJob(BaseJob):\n    timeout = 300  # five minutes\n",
    )
    write(jobs / "TypedJob.py", "class TypedJob(BaseJob):\n    timeout: int = 1_200\n")

    page = _jobs_page(root)

    def timeout_of(job: str) -> str:
        row = next(line for line in page.splitlines() if line.startswith(f"| {job} "))
        return row.split("|")[4].strip()

    assert timeout_of("HourJob") == "—"
    assert timeout_of("CallsJob") == "—"
    assert timeout_of("PlainJob") == "300"
    assert timeout_of("TypedJob") == "1_200"


def test_a_queue_named_inside_the_constructor_is_still_read(tmp_path):
    """An envelope job names its lane in ``__init__``, far below the class
    line; the reference has always read it there, and still does at any
    distance."""
    root = make_checkout(tmp_path, "alpha")
    write(
        root / "commons" / "contracts" / "envelopes" / "IngestJob.py",
        f"""
class IngestJob(ShouldQueue, Queueable):
    {_LONG_DOCSTRING}

    def __init__(self) -> None:
        self.queue = lane_for(
            queue_prefix="maintenance.catalog", job_class=type(self).__name__
        )
""",
    )

    assert "| IngestJob | `maintenance.catalog` |" in _jobs_page(root)


def test_a_command_named_behind_a_long_docstring_is_in_the_inventory(tmp_path):
    # The claim verifier judges ``craft x:y`` against this inventory: a
    # command it cannot see reads "no such command" and fails the gate.
    root = make_checkout(tmp_path, "alpha")
    write(
        root / "services" / "app" / "commands" / "Sweep.py",
        f"""
class Sweep(CommandBase):
    {_LONG_DOCSTRING}

    name = "maintenance:sweep"
""",
    )

    assert "maintenance:sweep" in command_names(root)


def test_a_file_that_does_not_parse_still_lists_its_classes(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    directory = root / "services" / "app" / "commands"
    write(
        directory / "Broken.py",
        'class Broken(CommandBase):\n    name = "broken:run"\n\ndef oops(:\n',
    )

    rows = scan_classes(
        [directory], {"name": r'^\s*name\s*=\s*["\']([^"\']+)["\']'}, base=root
    )

    assert [(cls, attrs["name"]) for cls, _bases, _rel, attrs in rows] == [
        ("Broken", "broken:run")
    ]
