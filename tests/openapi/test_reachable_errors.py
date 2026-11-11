"""An action's contract lists the typed errors raised below its controller.

Live 2026-11-11: ``PATCH /listings/{id}/content`` answered 400 — the
``InvalidInput`` its service raises — and the published document did not say
so; only what the controller's own methods raised was read. The extractor now
follows the calls the source pins (typed constructor attributes, imports,
barrels, kernel aliases, handed-over callables) and documents what can escape,
minus what an enclosing ``try`` catches. A call the source does not pin is not
guessed.
"""

from __future__ import annotations

from cara.openapi import ControllerContractExtractor
from tests.openapi._reachable_fixtures import STATUSES as _STATUSES
from tests.openapi._reachable_fixtures import error as _error
from tests.openapi._reachable_fixtures import errors as _errors
from tests.openapi._reachable_fixtures import tree as _tree

_SERVICE = """
from app.exceptions import InvalidInput


class ThingService:
    async def update_content(self, thing, data):
        def mutate(locked):
            if not data:
                raise InvalidInput("Nothing to save.")
        return run(mutate)
"""


def test_an_error_the_service_raises_is_documented(tmp_path):
    _tree(
        tmp_path,
        {
            "app/services/ThingService.py": _SERVICE,
            "app/controllers/ThingController.py": """
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService) -> None:
        self.things = things

    async def update_content(self, request, response):
        row = await self.things.update_content(request, {})
        return response.envelope(row)
""",
        },
    )

    assert _errors(tmp_path, "update_content") == {_error(400)}


def test_a_mixin_action_reaches_the_service_its_controller_holds(tmp_path):
    _tree(
        tmp_path,
        {
            "app/services/ThingService.py": _SERVICE,
            "app/controllers/ThingContentMixin.py": """
class ThingContentMixin:
    async def update_content(self, request, response):
        return response.envelope(await self.things.update_content(request, {}))
""",
            "app/controllers/ThingController.py": """
from app.controllers.ThingContentMixin import ThingContentMixin
from app.services.ThingService import ThingService


class ThingController(ThingContentMixin):
    def __init__(self, things: ThingService | None = None) -> None:
        self.things = things or ThingService()
""",
        },
    )

    assert _errors(tmp_path, "update_content") == {_error(400)}


def test_a_lookup_helper_through_a_barrel_and_a_handed_over_finder(tmp_path):
    _tree(
        tmp_path,
        {
            "app/support/Lookup.py": """
from app.exceptions import EntityNotFound


async def find_or_fail(finder, *args, missing):
    found = await run_in_thread(finder, *args)
    if found is None:
        raise EntityNotFound(missing)
    return found
""",
            "app/support/__init__.py": """
_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "find_or_fail": (".Lookup", "find_or_fail"),
}
""",
            "app/repositories/ThingRepository.py": """
from app.exceptions import ConflictException


class ThingRepository:
    def find(self, public_id):
        if locked(public_id):
            raise ConflictException("Locked.")
""",
            "app/controllers/ThingController.py": """
from app.repositories.ThingRepository import ThingRepository
from app.support import find_or_fail


class ThingController:
    def __init__(self, repo: ThingRepository) -> None:
        self.repo = repo

    async def show(self, request, response, public_id):
        thing = await find_or_fail(self.repo.find, public_id, missing="Gone.")
        return response.envelope(thing)
""",
        },
    )

    assert _errors(tmp_path, "show") == {_error(404), _error(409)}


def test_an_error_caught_on_the_way_up_is_not_documented(tmp_path):
    _tree(
        tmp_path,
        {
            "app/services/ThingService.py": """
import contextlib

from app.exceptions import ConflictException, EntityNotFound, InvalidInput


class ThingService:
    def open(self):
        raise EntityNotFound("Gone.")

    def check(self):
        raise InvalidInput("Bad.")

    def claim(self):
        raise ConflictException("Taken.")

    def answer(self):
        try:
            self.open()
        except EntityNotFound:
            return None
        try:
            self.check()
        except Exception as exc:
            log(exc)
            raise
        with contextlib.suppress(ConflictException):
            self.claim()
""",
            "app/controllers/ThingController.py": """
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService) -> None:
        self.things = things

    async def store(self, request, response):
        return response.envelope(self.things.answer())
""",
        },
    )

    # The 404 is caught and the 409 suppressed; the 400 is re-raised.
    assert _errors(tmp_path, "store") == {_error(400)}


def test_a_kernel_alias_resolves_a_deep_barrel_import(tmp_path):
    _tree(
        tmp_path,
        {
            "commons/gates/Refused.py": """
class Refused(Exception):
    status_code = 409
""",
            "commons/gates/Doors.py": """
from .Refused import Refused


def open_door(thing):
    raise Refused("Held.")
""",
            "app/controllers/ThingController.py": """
from app.gates.Doors import open_door


class ThingController:
    async def store(self, request, response):
        open_door(request)
        return response.envelope({})
""",
        },
    )

    assert _errors(tmp_path, "store") == set()
    assert _errors(tmp_path, "store", module_aliases={"app.gates": "commons.gates"}) == {
        _error(409)
    }


def test_a_status_passed_to_the_constructor_is_the_one_answered(tmp_path):
    _tree(
        tmp_path,
        {
            "app/support/Abort.py": """
from app.exceptions import HttpException


def abort(status_code, message=None):
    raise HttpException(message, status_code=status_code)
""",
            "app/controllers/ThingController.py": """
from app.exceptions import HttpException, ServiceException
from app.support.Abort import abort


class ThingController:
    async def gone(self, request, response):
        raise HttpException("Gone.", 404)

    async def aborted(self, request, response):
        abort(401)

    async def faulted(self, request, response):
        raise ServiceException("Broken.")
""",
        },
    )

    assert _errors(tmp_path, "gone") == {_error(404)}
    # A status handed in as a variable is not pinned at the raise.
    assert _errors(tmp_path, "aborted") == set()
    # 500 is the fault every operation can answer; it is not published.
    assert _errors(tmp_path, "faulted") == set()


def test_an_error_a_factory_returns_is_documented(tmp_path):
    # Live 2026-11-11 review: ``raise self.invitations.not_redeemable(...)``
    # (404), ``raise _cache_unavailable(...)`` (503) and ``raise
    # ValidationFailed.field(...)`` build the exception in a function whose
    # return annotation names it — the source pins the class, so it is read.
    _tree(
        tmp_path,
        {
            "app/services/ThingService.py": """
from app.exceptions import ConflictException, EntityNotFound, InvalidInput


def refusal(reason) -> InvalidInput | ConflictException:
    return InvalidInput(reason) if reason else ConflictException("Taken.")


def anything() -> Exception:
    return RuntimeError("Not a typed answer.")


class ThingService:
    @staticmethod
    def not_redeemable(thing) -> EntityNotFound:
        return EntityNotFound(f"{thing} is gone.")

    def open(self, thing):
        raise self.not_redeemable(thing)

    def claim(self, reason):
        try:
            self.open(reason)
        except Exception as exc:
            raise refusal(reason) from exc
        raise anything()
""",
            "app/controllers/ThingController.py": """
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService) -> None:
        self.things = things

    async def store(self, request, response):
        self.things.claim(request)
        return response.envelope({})

    async def show(self, request, response):
        self.things.open(request)
        return response.envelope({})
""",
        },
    )

    # The 404 ``claim`` meets is caught; the union factory raises in its place.
    assert _errors(tmp_path, "store") == {_error(400), _error(409)}
    assert _errors(tmp_path, "show") == {_error(404)}


def test_a_class_a_classmethod_raises_and_a_typed_local(tmp_path):
    _tree(
        tmp_path,
        {
            "app/support/Refused.py": """
class Refused(Exception):
    status_code = 409

    @classmethod
    def unless(cls, allowed):
        if not allowed:
            raise cls("Held.")
""",
            "app/controllers/ThingController.py": """
from app.exceptions import InvalidInput as Invalid
from app.support.Refused import Refused


class ThingController:
    async def store(self, request, response):
        Refused.unless(request)
        return response.envelope({})

    async def index(self, request, response, cursor):
        invalid = Invalid("Bad cursor.")
        if not cursor:
            raise invalid
        return response.envelope([])
""",
        },
    )

    # ``cls`` is the class the classmethod runs on; ``invalid`` is the local
    # every assignment types, under the name it was imported as.
    assert _errors(tmp_path, "store") == {_error(409)}
    assert _errors(tmp_path, "index") == {_error(400)}


def test_a_call_the_source_does_not_pin_is_not_guessed(tmp_path):
    _tree(
        tmp_path,
        {
            "app/services/Strict.py": """
from app.exceptions import InvalidInput


class Strict:
    def run(self):
        raise InvalidInput("Strict.")
""",
            "app/services/Lenient.py": """
from app.exceptions import ConflictException


class Lenient:
    def run(self):
        raise ConflictException("Lenient.")
""",
            "app/controllers/ThingController.py": """
from app.services.Lenient import Lenient
from app.services.Strict import Strict


class ThingController:
    def __init__(
        self, strict: Strict, handlers: dict, one_of: Strict | Lenient
    ) -> None:
        self.handlers = handlers
        self.either = strict
        self.untyped = make_service()
        self.one_of = one_of

    def swap(self) -> None:
        self.either = Lenient()

    async def store(self, request, response, kind):
        self.handlers[kind]()
        self.either.run()
        self.untyped.run()
        self.one_of.run()
        runner = pick()
        runner.run()
        return response.envelope({})
""",
        },
    )

    # ``either`` holds two classes, ``one_of`` is annotated with a union and
    # the rest are not typed at all: none is guessed (each candidate raises,
    # so picking either one would show), and what flows through them is an
    # ``@raises`` declaration.
    assert _errors(tmp_path, "store") == set()


def test_the_same_tree_extracts_the_same_contract(tmp_path):
    _tree(
        tmp_path,
        {
            "app/services/ThingService.py": _SERVICE,
            "app/controllers/ThingController.py": """
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService) -> None:
        self.things = things

    async def update_content(self, request, response):
        return response.envelope(await self.things.update_content(request, {}))
""",
        },
    )
    directory = tmp_path / "app" / "controllers"

    assert (
        ControllerContractExtractor(directory, _STATUSES).extract()
        == ControllerContractExtractor(directory, _STATUSES).extract()
    )


def test_a_declaration_the_calls_already_show_is_redundant(tmp_path):
    _tree(
        tmp_path,
        {
            "app/services/ThingService.py": _SERVICE,
            "app/controllers/ThingController.py": '''
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService, handlers: dict) -> None:
        self.things = things
        self.handlers = handlers

    async def update_content(self, request, response):
        """@raises(InvalidInput)"""
        return response.envelope(await self.things.update_content(request, {}))

    async def store(self, request, response, kind):
        """@raises(ConflictException)"""
        return response.envelope(self.handlers[kind]())
''',
        },
    )
    extractor = ControllerContractExtractor(tmp_path / "app" / "controllers", _STATUSES)

    # The 400 is read from the service; the 409 flows through a dict of
    # callables nothing pins, so only its declaration can say it.
    assert extractor.redundant_declarations() == [
        "ThingController.update_content: InvalidInput"
    ]
