"""The error contract a client branches on, read from source.

An HTTP status alone does not tell a caller what happened — two 403s can be
"you are not this tenant" and "finish two-factor setup" — so the envelope
carries a stable ``type``. These pins cover where those discriminators are
found, what status each one carries, and what happens when an error body
cannot be branched on at all.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.openapi.Errors import (
    FRAMEWORK_ERROR_ROOTS,
    ConflictingErrorStatus,
    ErrorContractExtractor,
    UntypedErrorResponse,
)


def _module(directory: Path, name: str, body: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(body, encoding="utf-8")
    return path


def _app_only(root: Path) -> ErrorContractExtractor:
    """An extractor with the framework's own roots taken out of the picture."""
    return ErrorContractExtractor((root,), framework_roots=())


class TestTypedExceptionClasses:
    def test_a_class_contributes_its_type_and_status(self, tmp_path: Path):
        _module(
            tmp_path,
            "types.py",
            '''
class PaymentRequiredException(Exception):
    """A plan boundary."""

    status_code = 402
    error_type = "payment_required"
''',
        )

        rows = _app_only(tmp_path).extract()

        assert [(row.type, row.status) for row in rows] == [("payment_required", 402)]

    def test_the_generic_handler_fallbacks_are_discriminators_too(self, tmp_path: Path):
        # They are class constants rather than dict literals, so a scan that
        # only read error bodies would publish a union missing the two types
        # every unhandled failure actually returns.
        _module(
            tmp_path,
            "handler.py",
            """
class DefaultExceptionHandler:
    _GENERIC_5XX_TYPE = "internal_error"
    _GENERIC_4XX_TYPE = "request_error"
""",
        )

        assert _app_only(tmp_path).statuses() == {
            "internal_error": 500,
            "request_error": 400,
        }

    def test_a_body_built_inside_a_class_inherits_that_class_status(self, tmp_path: Path):
        _module(
            tmp_path,
            "types.py",
            """
class ConflictException(Exception):
    status_code = 409

    def to_dict(self):
        return {"error": self.message, "type": "conflict"}
""",
        )

        assert _app_only(tmp_path).statuses() == {"conflict": 409}


class TestInlineEmissions:
    def test_an_inline_error_response_carries_its_status(self, tmp_path: Path):
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": "gone", "type": "thing_gone"}, 410)
""",
        )

        assert _app_only(tmp_path).statuses() == {"thing_gone": 410}

    def test_a_payload_bound_one_line_above_is_still_read(self, tmp_path: Path):
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        payload = {"error": "locked", "type": "thing_locked"}
        return response.json(payload, 423)
""",
        )

        assert _app_only(tmp_path).statuses() == {"thing_locked": 423}

    def test_a_message_that_is_not_a_literal_does_not_hide_the_body(self, tmp_path: Path):
        # Most real error messages are formatted or translated at the call
        # site. Requiring a literal there would silently drop the majority of
        # the contract.
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": f"no {name}", "type": "thing_missing"}, 404)
""",
        )

        assert _app_only(tmp_path).statuses() == {"thing_missing": 404}

    def test_a_success_response_is_not_an_error(self, tmp_path: Path):
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": None, "type": "ok"}, 200)
""",
        )

        rows = _app_only(tmp_path).extract()

        assert [row.type for row in rows] == ["ok"]
        # Collected as a literal body, but no 4xx/5xx emission settles a status
        # and no hint covers it, so nothing is claimed about the status.
        assert rows[0].status is None
        assert _app_only(tmp_path).statuses() == {}


class TestUntypedBodies:
    def test_an_error_body_without_a_type_is_reported(self, tmp_path: Path):
        _module(
            tmp_path,
            "WebhookController.py",
            """
class WebhookController:
    async def ingest(self, request, response):
        return response.json({"error": "webhook destination not found"}, 404)
""",
        )

        extractor = _app_only(tmp_path)

        assert extractor.untyped_responses() == ["WebhookController.py:ingest"]
        assert extractor.extract() == []

    def test_the_hole_is_named_by_function_so_it_survives_edits(self, tmp_path: Path):
        # A line-numbered label would move whenever an unrelated line above it
        # changed, so an application could not pin a deliberate hole.
        body = """
class WebhookController:
    async def ingest(self, request, response):
{padding}        return response.json({{"error": "nope"}}, 400)
"""
        _module(tmp_path, "WebhookController.py", body.format(padding=""))
        before = _app_only(tmp_path).untyped_responses()
        _module(tmp_path, "WebhookController.py", body.format(padding="        x = 1\n"))
        after = _app_only(tmp_path).untyped_responses()

        assert before == after == ["WebhookController.py:ingest"]

    def test_requiring_typed_responses_turns_a_hole_into_a_failure(self, tmp_path: Path):
        _module(
            tmp_path,
            "WebhookController.py",
            """
class WebhookController:
    async def ingest(self, request, response):
        return response.json({"error": "nope"}, 400)
""",
        )

        with pytest.raises(UntypedErrorResponse, match="WebhookController.py:ingest"):
            _app_only(tmp_path).require_typed_responses()

    def test_a_fully_typed_surface_requires_nothing(self, tmp_path: Path):
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": "gone", "type": "thing_gone"}, 410)
""",
        )

        _app_only(tmp_path).require_typed_responses()


class TestStatusResolution:
    def test_a_hint_settles_a_status_no_source_states(self, tmp_path: Path):
        # Socket frames carry the envelope with no HTTP status anywhere.
        _module(
            tmp_path,
            "Socket.py",
            """
def deny(send):
    return send({"error": "who are you", "type": "authentication_error"})
""",
        )

        assert _app_only(tmp_path).statuses() == {"authentication_error": 401}

    def test_an_unknown_status_is_left_unknown_rather_than_invented(self, tmp_path: Path):
        _module(
            tmp_path,
            "Socket.py",
            """
def deny(send):
    return send({"error": "bad frame", "type": "frame_rejected"})
""",
        )

        extractor = _app_only(tmp_path)

        assert [row.type for row in extractor.extract()] == ["frame_rejected"]
        assert extractor.statuses() == {}

    def test_a_stated_status_beats_a_hint(self, tmp_path: Path):
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": "x", "type": "request_error"}, 400)
""",
        )

        assert _app_only(tmp_path).statuses() == {"request_error": 400}

    def test_one_discriminator_with_two_statuses_is_a_contract_bug(self, tmp_path: Path):
        # A client that branches on the type would have to branch on the
        # status as well, which is exactly what the discriminator exists to
        # avoid — so this fails the build instead of picking a winner.
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": "a", "type": "thing_bad"}, 409)

    async def store(self, request, response):
        return response.json({"error": "b", "type": "thing_bad"}, 422)
""",
        )

        with pytest.raises(ConflictingErrorStatus, match="thing_bad"):
            _app_only(tmp_path).extract()

    def test_the_same_status_stated_twice_is_not_a_conflict(self, tmp_path: Path):
        _module(
            tmp_path,
            "Controller.py",
            """
class ThingController:
    async def show(self, request, response):
        return response.json({"error": "a", "type": "thing_bad"}, 409)

    async def store(self, request, response):
        return response.json({"error": "b", "type": "thing_bad"}, 409)
""",
        )

        assert _app_only(tmp_path).statuses() == {"thing_bad": 409}


class TestFrameworkRoots:
    def test_the_framework_finds_its_own_emitters_unasked(self):
        # An application that forgot to list a framework path used to publish
        # a union missing the errors the framework itself returns.
        discriminators = ErrorContractExtractor().discriminators()

        assert {"not_found", "internal_error", "request_error"}.issubset(discriminators)

    def test_every_declared_framework_root_exists(self):
        assert all(root.exists() for root in FRAMEWORK_ERROR_ROOTS)

    def test_application_roots_are_added_to_the_framework_s_own(self, tmp_path: Path):
        _module(
            tmp_path,
            "types.py",
            """
class TenantRequiredException(Exception):
    status_code = 403
    error_type = "tenant_required"
""",
        )

        discriminators = ErrorContractExtractor((tmp_path,)).discriminators()

        assert "tenant_required" in discriminators
        assert "not_found" in discriminators

    def test_a_single_file_root_is_accepted(self, tmp_path: Path):
        path = _module(
            tmp_path,
            "OnlyThis.py",
            """
class OnlyThisException(Exception):
    status_code = 418
    error_type = "only_this"
""",
        )
        _module(
            tmp_path,
            "NotThis.py",
            """
class NotThisException(Exception):
    status_code = 400
    error_type = "not_this"
""",
        )

        rows = ErrorContractExtractor((path,), framework_roots=()).extract()

        assert [row.type for row in rows] == ["only_this"]

    def test_discriminators_are_sorted_so_the_artifact_is_stable(self, tmp_path: Path):
        _module(
            tmp_path,
            "types.py",
            """
class ZException(Exception):
    status_code = 400
    error_type = "zebra"


class AException(Exception):
    status_code = 400
    error_type = "aardvark"
""",
        )

        assert _app_only(tmp_path).discriminators() == ["aardvark", "zebra"]
