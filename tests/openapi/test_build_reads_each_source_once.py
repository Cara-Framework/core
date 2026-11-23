"""One spec build reads each source once, and reads the same thing.

Audit 2026-11-23: building the Synkronus document took 15.5 s, and most of it
was the same work done again — a controller parsed by six extractors and the
call follower (8,735 parses of some 3,300 files), the error contract scanned
once per question asked of it, a function's own bound names and typed locals
re-read for every set of facts a caller pins on it (20,882 scans of 11,419
functions), an action's status bindings found by walking every PAIR of its
functions. None of that changes an answer, so each is now read once (7.5 s;
the document is byte-identical).

These pins hold the two halves of that: the count of reads, and — because a
cache is only as good as its invalidation — that a changed source is still
re-read and the readers still see exactly what ``ast.walk`` shows them.
"""

from __future__ import annotations

import ast
import gc
import importlib
import os
import subprocess
import sys
import weakref
from pathlib import Path

import pytest

from cara.openapi import (
    ControllerContractExtractor,
    ErrorContractExtractor,
    controller_action_functions,
    cursor_paginated_actions,
    null_data_actions,
)
from tests.openapi._reachable_fixtures import STATUSES, error, errors, tree

# The submodules by name: the package's lazy exports bind ``cara.openapi.X``
# to the class ``X``, not to the module that defines it.
Calls = importlib.import_module("cara.openapi.Calls")
Controllers = importlib.import_module("cara.openapi.Controllers")
Sources = importlib.import_module("cara.openapi.Sources")

_OPENAPI_PACKAGE = Path(Sources.__file__).parent


@pytest.fixture
def parses(monkeypatch: pytest.MonkeyPatch):
    """Every file ``ast.parse`` is handed while the test runs, by filename."""
    seen: list[str] = []
    real = ast.parse

    def counting(source, filename="<unknown>", *args, **kwargs):
        seen.append(str(filename))
        return real(source, filename, *args, **kwargs)

    monkeypatch.setattr(ast, "parse", counting)
    return seen


def _under(parses: list[str], root: Path) -> list[str]:
    return sorted(name for name in parses if name.startswith(str(root)))


# -- the walkers every reader now goes through ---------------------------


def _framework_trees() -> list[ast.Module]:
    return [
        ast.parse(path.read_text(encoding="utf-8"))
        for path in sorted(_OPENAPI_PACKAGE.glob("*.py"))
    ]


def test_the_fast_walk_yields_what_ast_walk_yields_in_its_order():
    # Order is load-bearing: a later definition of a name replaces an earlier
    # one in several readers, so a walk in another order changes answers.
    for module in _framework_trees():
        expected = list(ast.walk(module))
        found = list(Sources._walk(module))
        assert len(found) == len(expected)
        assert all(a is b for a, b in zip(found, expected, strict=True))


def test_the_statement_walk_is_ast_walk_less_the_expressions():
    for module in _framework_trees():
        expected = [
            node
            for node in ast.walk(module)
            if node is module or isinstance(node, Sources._STATEMENT_HOLDERS)
        ]
        found = list(Sources._statements(module))
        assert len(found) == len(expected)
        assert all(a is b for a, b in zip(found, expected, strict=True))


def test_the_statement_walk_reaches_statements_in_every_kind_of_block():
    module = ast.parse(
        """
def outer():
    try:
        import a
    except ValueError:
        import b
    else:
        import c
    finally:
        import d
    match value:
        case 1:
            import e
    with manager:
        for item in items:
            while item:
                if item:
                    import f
                else:
                    import g
    class Inner:
        import h
        async def method(self):
            import i
"""
    )

    imported = [
        alias.name
        for node in Sources._statements(module)
        if isinstance(node, ast.Import)
        for alias in node.names
    ]

    assert sorted(imported) == list("abcdefghi")


# -- one parse per file ---------------------------------------------------


def test_a_file_is_parsed_once_for_as_long_as_its_text_is_the_same(
    tmp_path: Path, parses: list[str]
):
    path = tmp_path / "Thing.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")

    first = Sources._parse_source(path)
    second = Sources._parse_source(path)

    assert first is second
    assert _under(parses, tmp_path) == [str(path)]


def test_a_rewritten_file_is_parsed_again_even_within_one_clock_tick(
    tmp_path: Path,
):
    # Same size, same mtime: only the text differs. A cache keyed on the
    # file's stat would hand back the old tree here.
    path = tmp_path / "Thing.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")
    stat = path.stat()
    before = Sources._parse_source(path)

    path.write_text("VALUE = 2\n", encoding="utf-8")
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    after = Sources._parse_source(path)

    assert after is not before
    assert after.body[0].value.value == 2


def test_the_parse_cache_starts_over_instead_of_growing_without_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(Sources, "_PARSED", {})
    monkeypatch.setattr(Sources, "_PARSED_LIMIT", 3)

    for index in range(8):
        path = tmp_path / f"M{index}.py"
        path.write_text(f"VALUE = {index}\n", encoding="utf-8")
        Sources._parse_source(path)

    assert len(Sources._PARSED) <= 3


_CONTROLLER = """
class ThingController:
    async def index(self, request, response):
        return response.paginated([], {})

    async def show(self, request, response):
        return self._respond(response)

    def _respond(self, response):
        return response.envelope(None)
"""


def test_every_reader_of_a_controllers_tree_shares_one_parse(
    tmp_path: Path, parses: list[str]
):
    controllers = tmp_path / "app" / "controllers"
    tree(tmp_path, {"app/controllers/ThingController.py": _CONTROLLER})

    first = controller_action_functions(controllers)
    second = controller_action_functions(controllers)
    paged = cursor_paginated_actions(controllers)
    nulls = null_data_actions(controllers)
    ControllerContractExtractor(controllers, STATUSES).extract()

    # Five readers, the call follower among them: one parse of the
    # controller (and the follower's one read of the package it sits in).
    assert _under(parses, controllers) == [
        str(controllers / "ThingController.py"),
        str(controllers / "__init__.py"),
    ]
    assert len([name for name in parses if "ThingController" in name]) == 1
    assert "<unknown>" not in parses
    assert paged == {"ThingController@index"}
    assert nulls == {"ThingController@show"}
    assert [(c, a) for c, a, _ in first] == [(c, a) for c, a, _ in second]
    # The same functions, in a list of the caller's own.
    assert all(a[2] == b[2] for a, b in zip(first, second, strict=True))
    assert first is not second


def test_an_edited_controller_is_resolved_again(tmp_path: Path):
    controllers = tmp_path / "app" / "controllers"
    tree(tmp_path, {"app/controllers/ThingController.py": _CONTROLLER})
    assert null_data_actions(controllers) == {"ThingController@show"}

    path = controllers / "ThingController.py"
    path.write_text(
        _CONTROLLER.replace("response.envelope(None)", "response.envelope({})"),
        encoding="utf-8",
    )

    assert null_data_actions(controllers) == set()


def test_an_added_or_removed_controller_is_seen(tmp_path: Path):
    controllers = tmp_path / "app" / "controllers"
    tree(tmp_path, {"app/controllers/ThingController.py": _CONTROLLER})
    assert {c for c, _, _ in controller_action_functions(controllers)} == {
        "ThingController"
    }

    other = controllers / "OtherController.py"
    other.write_text(_CONTROLLER.replace("Thing", "Other"), encoding="utf-8")
    assert {c for c, _, _ in controller_action_functions(controllers)} == {
        "OtherController",
        "ThingController",
    }

    other.unlink()
    assert {c for c, _, _ in controller_action_functions(controllers)} == {
        "ThingController"
    }


# -- the error contract: one scan per extractor ---------------------------


def _error_sources(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "types.py").write_text(
        """
class PaymentRequiredException(Exception):
    status_code = 402
    error_type = "payment_required"
""",
        encoding="utf-8",
    )
    (root / "handler.py").write_text(
        """
def refuse(response):
    return response.json({"error": "nope"}, 403)
""",
        encoding="utf-8",
    )


def test_every_question_of_one_error_extractor_is_one_read_of_the_source(
    tmp_path: Path, parses: list[str]
):
    _error_sources(tmp_path)
    extractor = ErrorContractExtractor((tmp_path,), framework_roots=())

    assert extractor.discriminators() == ["payment_required"]
    assert extractor.statuses() == {"payment_required": 402}
    assert extractor.untyped_responses() == ["handler.py:refuse"]
    assert [row.type for row in extractor.extract()] == ["payment_required"]
    assert extractor.exception_statuses() == {"PaymentRequiredException": 402}

    # Five answers used to be four full scans plus a fifth parse of each file.
    assert _under(parses, tmp_path) == sorted(
        str(tmp_path.resolve() / name) for name in ("handler.py", "types.py")
    )


def test_an_answer_handed_out_is_the_callers_own_copy(tmp_path: Path):
    _error_sources(tmp_path)
    extractor = ErrorContractExtractor((tmp_path,), framework_roots=())

    extractor.extract().clear()
    extractor.untyped_responses().clear()

    assert extractor.discriminators() == ["payment_required"]
    assert extractor.untyped_responses() == ["handler.py:refuse"]


def test_a_new_extractor_reads_the_source_as_it_now_is(tmp_path: Path):
    _error_sources(tmp_path)
    assert ErrorContractExtractor((tmp_path,), framework_roots=()).statuses() == {
        "payment_required": 402
    }

    (tmp_path / "types.py").write_text(
        """
class GoneException(Exception):
    status_code = 410
    error_type = "gone"
""",
        encoding="utf-8",
    )

    assert ErrorContractExtractor((tmp_path,), framework_roots=()).statuses() == {
        "gone": 410
    }


# -- the call follower: a function's own source is read once --------------

_FACT_VARIANTS = {
    "app/support/Guard.py": """
from app.exceptions import EntityNotFound, InvalidInput


def guard(strict, loud):
    marker = 1
    if strict:
        raise EntityNotFound("missing")
    if loud:
        raise InvalidInput("bad")
    return marker
""",
    "app/controllers/ThingController.py": """
from app.support.Guard import guard


class ThingController:
    async def lenient(self, request, response):
        guard(False, False)
        return response.envelope({})

    async def strict(self, request, response):
        guard(True, False)
        return response.envelope({})

    async def loud(self, request, response):
        guard(False, True)
        return response.envelope({})
""",
}


def test_a_function_run_under_several_facts_reads_its_own_source_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    tree(tmp_path, _FACT_VARIANTS)
    read: list[str] = []
    real = Calls._bound_names

    def counting(function):
        read.append(function.name)
        return real(function)

    monkeypatch.setattr(Calls, "_bound_names", counting)

    contracts = ControllerContractExtractor(
        tmp_path / "app" / "controllers", STATUSES
    ).extract()

    # Three callers pin three different sets of facts on ``guard``: three
    # scans of it, one reading of the names it binds.
    assert read.count("guard") == 1

    def statuses(action: str) -> set[int]:
        responses = contracts[f"ThingController@{action}"].responses
        return {variant.status for variant in responses if variant.kind == "error"}

    # And each scan still runs under its own facts.
    assert statuses("lenient") == set()
    assert statuses("strict") == {404}
    assert statuses("loud") == {400}


def test_typed_locals_are_read_per_receiver_not_per_fact(tmp_path: Path):
    # The same method reached with two different literal arguments, calling
    # through a local it types by construction: both variants follow it.
    tree(
        tmp_path,
        {
            "app/support/Finder.py": """
from app.exceptions import EntityNotFound


class Finder:
    def find(self):
        raise EntityNotFound("missing")
""",
            "app/controllers/ThingController.py": """
from app.exceptions import ConflictException
from app.support.Finder import Finder


class ThingController:
    async def one(self, request, response):
        return self._load(True)

    async def two(self, request, response):
        return self._load(False)

    def _load(self, strict):
        finder = Finder()
        if strict:
            raise ConflictException("held")
        return finder.find()
""",
        },
    )

    assert errors(tmp_path, "one") == {error(409)}
    assert errors(tmp_path, "two") == {error(404)}


# -- an action's status bindings: one walk per function, not per pair -----


def test_an_actions_self_calls_are_indexed_in_one_walk_per_function(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    helpers = "\n".join(
        f"""
    def _step{index}(self, response):
        return self._step{index + 1}(response)
"""
        for index in range(7)
    )
    tree(
        tmp_path,
        {
            "app/controllers/ThingController.py": f"""
class ThingController:
    async def store(self, request, response):
        return self._step0(response)
{helpers}
    def _step7(self, response):
        return self._respond(response, status=201)

    def _respond(self, response, status=200):
        return response.envelope({{}}, None, status)
"""
        },
    )
    walked: list[str] = []
    real = Controllers._walk

    def counting(node):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            walked.append(node.name)
        return real(node)

    monkeypatch.setattr(Controllers, "_walk", counting)
    functions = next(
        functions
        for _, action, functions in controller_action_functions(
            tmp_path / "app" / "controllers"
        )
        if action == "store"
    )
    walked.clear()

    calls = Controllers._self_calls(functions)
    bindings = {
        function.name: Controllers._status_bindings(function, calls)
        for function in functions
    }

    # Ten functions: ten walks. Finding each one's callers by walking all ten
    # again was a hundred.
    assert len(functions) == 10
    assert sorted(walked) == sorted(function.name for function in functions)
    # The helper's status is its default and what its one caller, eight
    # delegations below the action, passes it.
    assert bindings["_respond"] == {"status": {200, 201}}
    assert bindings["_step3"] == {}
    contract = ControllerContractExtractor(
        tmp_path / "app" / "controllers", STATUSES
    ).extract()["ThingController@store"]
    assert [variant.status for variant in contract.responses] == [200, 201]


# -- what a finished build leaves behind ----------------------------------


def test_a_finished_build_frees_its_source_index_without_the_cycle_collector(
    tmp_path: Path,
):
    # Every scan used to be a reference cycle holding the whole index, so
    # the parsed modules of a build stayed alive until a full collection —
    # and were walked again by each collection the interpreter runs at exit.
    tree(tmp_path, _FACT_VARIANTS)
    extractor = ControllerContractExtractor(tmp_path / "app" / "controllers", STATUSES)
    gc.collect()
    gc.disable()
    try:
        assert extractor.extract()
        index = weakref.ref(extractor._reach()[1]._index)
        assert index() is not None

        del extractor

        assert index() is None
    finally:
        gc.enable()


_EXIT_PROBE = """
import atexit
import sys
from pathlib import Path

loaded = {}


def report():
    print(len(loaded["sources"]._PARSED), len(loaded["controllers"]._ACTIONS))


# Registered before the package is imported, so it runs after the package's
# own exit hooks: it sees what is left once they are done.
atexit.register(report)

import importlib

loaded["sources"] = importlib.import_module("cara.openapi.Sources")
loaded["controllers"] = importlib.import_module("cara.openapi.Controllers")
actions = loaded["controllers"].controller_action_functions(Path(sys.argv[1]))
assert actions and loaded["sources"]._PARSED and loaded["controllers"]._ACTIONS
"""


def test_the_parsed_sources_are_dropped_before_the_interpreter_shuts_down(
    tmp_path: Path,
):
    # Shutdown runs the cycle collector several times over everything still
    # alive; a build's parsed trees made that four seconds of a twelve-second
    # ``generate_openapi.py``. They are only a cache: gone before it starts.
    tree(tmp_path, {"app/controllers/ThingController.py": _CONTROLLER})

    finished = subprocess.run(
        [sys.executable, "-c", _EXIT_PROBE, str(tmp_path / "app" / "controllers")],
        capture_output=True,
        text=True,
        check=True,
        # The directory this ``cara`` is imported from, then whatever else
        # this interpreter resolves its dependencies through.
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [str(_OPENAPI_PACKAGE.parents[1]), *filter(None, sys.path)]
            ),
        },
    )

    assert finished.stdout.split() == ["0", "0"]
