"""``ReachableErrors`` reads a raise the way Python runs it.

Review 2026-11-11 (fix4 openapi): mutating the call follower showed these
rules unpinned — reverting any one of them left every test green. Each test
below fails on exactly that revert: the first matching ``except`` decides, a
``try``'s ``else`` is outside its handlers, ``super()`` continues after the
defining class, a builtin base is caught by its own bases, a variable status
is not pinned, a subclass answers its base's status, typed parameters and
function-local imports are followed, and a declaration is redundant only when
every action that runs it already reaches it.
"""

from __future__ import annotations

from cara.openapi import ControllerContractExtractor
from tests.openapi._reachable_fixtures import STATUSES, error, errors, tree


def test_the_first_matching_handler_decides_and_else_is_outside_the_try(tmp_path):
    tree(
        tmp_path,
        {
            "app/services/ThingService.py": """
from app.exceptions import EntityNotFound, InvalidInput


class Refused(KeyError):
    status_code = 409


class ThingService:
    def check(self):
        raise InvalidInput("Bad.")

    def lookup(self):
        raise Refused("Held.")

    def first_handler_wins(self):
        try:
            self.check()
        except InvalidInput:
            raise
        except Exception:
            return None

    def else_is_outside(self):
        try:
            pass
        except EntityNotFound:
            return None
        else:
            raise EntityNotFound("Gone.")

    def builtin_base(self):
        try:
            self.lookup()
        except LookupError:
            return None
""",
            "app/controllers/ThingController.py": """
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService) -> None:
        self.things = things

    async def store(self, request, response):
        return response.envelope(self.things.first_handler_wins())

    async def show(self, request, response):
        return response.envelope(self.things.else_is_outside())

    async def index(self, request, response):
        return response.envelope(self.things.builtin_base())
""",
        },
    )

    # ``except InvalidInput: raise`` runs, never the later ``except Exception``.
    assert errors(tmp_path, "store") == {error(400)}
    # A raise in ``else`` is not caught by that ``try``'s handlers.
    assert errors(tmp_path, "show") == {error(404)}
    # ``Refused(KeyError)`` is a ``LookupError``.
    assert errors(tmp_path, "index") == set()


def test_super_continues_after_the_defining_class(tmp_path):
    tree(
        tmp_path,
        {
            "app/services/Base.py": """
from app.exceptions import ConflictException


class Base:
    def save(self):
        raise ConflictException("Taken.")
""",
            "app/services/Child.py": """
from app.services.Base import Base


class Child(Base):
    def save(self):
        return super().save()
""",
            "app/controllers/ThingController.py": """
from app.services.Child import Child


class ThingController:
    def __init__(self, child: Child) -> None:
        self.child = child

    async def store(self, request, response):
        return response.envelope(self.child.save())
""",
        },
    )

    assert errors(tmp_path, "store") == {error(409)}


def test_status_from_the_constructor_or_the_nearest_base(tmp_path):
    tree(
        tmp_path,
        {
            "app/support/Doors.py": """
from app.exceptions import ConflictException


class Held(Exception):
    status_code = 409

    def __init__(self, message="", status_code=None):
        super().__init__(message)
        self.status_code = status_code or 409


class Locked(ConflictException):
    pass


def refuse(code):
    raise Held("Held.", status_code=code)


def refuse_plainly():
    raise Held("Held.")


def lock():
    raise Locked("Locked.")
""",
            "app/controllers/ThingController.py": """
from app.support.Doors import lock, refuse, refuse_plainly


class ThingController:
    async def store(self, request, response):
        refuse(request.code)
        return response.envelope({})

    async def show(self, request, response):
        refuse_plainly()
        return response.envelope({})

    async def index(self, request, response):
        lock()
        return response.envelope([])
""",
        },
    )
    statuses = {**STATUSES, "Held": 409}

    def found(action):
        contracts = ControllerContractExtractor(
            tmp_path / "app" / "controllers", statuses
        ).extract()
        return {
            variant
            for variant in contracts[f"ThingController@{action}"].responses
            if variant.kind == "error"
        }

    # A status handed in as a variable is not pinned, whatever the class's own.
    assert found("store") == set()
    assert found("show") == {error(409)}
    # ``Locked`` states no status: it answers its base's inherited one.
    assert found("index") == {error(409)}


def test_typed_parameters_guarded_imports_and_local_imports(tmp_path):
    tree(
        tmp_path,
        {
            "app/services/ThingService.py": """
from app.exceptions import InvalidInput


class ThingService:
    def run(self):
        raise InvalidInput("Bad.")
""",
            "app/support/Helpers.py": """
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.ThingService import ThingService

try:
    from app.support.fast import probe
except ImportError:
    from app.exceptions import EntityNotFound

    def probe(thing):
        raise EntityNotFound("Gone.")


def apply(service: ThingService) -> None:
    service.run()
""",
            "app/controllers/ThingController.py": """
class ThingController:
    async def store(self, request, response):
        from app.support.Helpers import apply  # local: an import cycle

        apply(request.service)
        return response.envelope({})

    async def show(self, request, response):
        from app.support.Helpers import probe

        probe(request)
        return response.envelope({})
""",
        },
    )

    assert errors(tmp_path, "store") == {error(400)}
    # ``probe`` is defined in the ``except ImportError`` fallback.
    assert errors(tmp_path, "show") == {error(404)}


def test_a_declaration_is_redundant_only_where_every_action_reaches_it(tmp_path):
    tree(
        tmp_path,
        {
            "app/services/ThingService.py": """
from app.exceptions import InvalidInput


class ThingService:
    def run(self):
        raise InvalidInput("Bad.")
""",
            "app/controllers/ThingController.py": '''
from app.services.ThingService import ThingService


class ThingController:
    def __init__(self, things: ThingService, handlers: dict) -> None:
        self.things = things
        self.handlers = handlers

    def _dispatch(self, kind):
        """@raises(InvalidInput)"""
        return self.handlers[kind]()

    async def show(self, request, response, kind):
        return response.envelope(self._dispatch(kind))

    async def store(self, request, response, kind):
        self.things.run()
        return response.envelope(self._dispatch(kind))
''',
        },
    )
    extractor = ControllerContractExtractor(tmp_path / "app" / "controllers", STATUSES)

    # ``show`` reaches the 400 only through the declaration, so it stays —
    # however late in the walk ``store``, which does reach it, comes.
    assert extractor.redundant_declarations() == []
    assert errors(tmp_path, "show") == {error(400)}
