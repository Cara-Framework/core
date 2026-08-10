"""Controller-to-resource resolution across the splits a controller may take.

A routed action is allowed to be thin. It may delegate to a private handler,
inherit from a mixin in another file, or hand its rows to a house helper. Each
of those is a legal refactor, and each one silently deleted a route's response
contract from the artifact before this resolution existed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.openapi import (
    ControllerActionMapper,
    UnknownDeclaredResource,
    cursor_paginated_actions,
)


def _controllers(tmp_path: Path, **modules: str) -> Path:
    directory = tmp_path / "controllers"
    directory.mkdir()
    for name, source in modules.items():
        (directory / f"{name}.py").write_text(source, encoding="utf-8")
    return directory


class TestResolution:
    def test_collection_is_a_list_and_construction_is_a_row(self, tmp_path: Path):
        mapping = ControllerActionMapper(
            _controllers(
                tmp_path,
                ThingController="""
class ThingController:
    async def index(self, request, response):
        return ThingResource.collection(rows).to_response(response)

    async def show(self, request, response):
        return ThingResource(row).to_response(response)
""",
            ),
            {"ThingResource"},
        ).map()

        assert mapping["ThingController@index"] == ("ThingResource", True)
        assert mapping["ThingController@show"] == ("ThingResource", False)

    def test_a_paginated_response_forces_list_framing(self, tmp_path: Path):
        mapping = ControllerActionMapper(
            _controllers(
                tmp_path,
                ThingController="""
class ThingController:
    async def index(self, request, response):
        rows = [ThingResource(row).to_array() for row in page]
        return response.paginated(rows, limit=10, has_more=False)
""",
            ),
            {"ThingResource"},
        ).map()

        assert mapping["ThingController@index"] == ("ThingResource", True)

    def test_follows_delegation_more_than_one_hop(self, tmp_path: Path):
        """The serializer is often two hops down, not one.

        A single-hop walk resolves ``show -> _handle_show`` but stops before
        ``_handle_show -> _render``, which is where the resource actually is.
        """
        mapping = ControllerActionMapper(
            _controllers(
                tmp_path,
                ThingController="""
class ThingController:
    async def show(self, request, response):
        return await self._handle_show(request, response)

    async def _handle_show(self, request, response):
        return self._render(response, row)

    def _render(self, response, row):
        return ThingResource(row).to_response(response)
""",
            ),
            {"ThingResource"},
        ).map()

        assert mapping["ThingController@show"] == ("ThingResource", False)

    def test_an_action_inherited_from_another_file_still_resolves(self, tmp_path: Path):
        mapping = ControllerActionMapper(
            _controllers(
                tmp_path,
                _ThingMixin="""
class _ThingMixin:
    async def index(self, request, response):
        return ThingResource.collection(rows).to_response(response)
""",
                ThingController="""
class ThingController(_ThingMixin):
    pass
""",
            ),
            {"ThingResource"},
        ).map()

        assert mapping["ThingController@index"] == ("ThingResource", True)

    def test_a_registered_helper_stands_in_for_the_resource(self, tmp_path: Path):
        controllers = _controllers(
            tmp_path,
            FeedController="""
class FeedController:
    async def index(self, request, response):
        return response.envelope(batch_serialize(rows))
""",
        )

        assert ControllerActionMapper(controllers, {"RowResource"}).map() == {}
        assert ControllerActionMapper(
            controllers, {"RowResource"}, {"batch_serialize": ("RowResource", True)}
        ).map() == {"FeedController@index": ("RowResource", True)}


class TestDocstringDeclaration:
    def test_a_declaration_wins_over_every_ast_signal(self, tmp_path: Path):
        """Serialization may legitimately live below the controller.

        When a service serializes inside its own cache closure, the action
        never names a resource and the route would lose its schema. The
        docstring declaration is the explicit answer for that case.
        """
        mapping = ControllerActionMapper(
            _controllers(
                tmp_path,
                ThingController="""
class ThingController:
    async def show(self, request, response):
        \"\"\"Serialized by the service. @resource(DetailResource)\"\"\"
        return response.envelope(await self.service.detail())

    async def index(self, request, response):
        \"\"\"@resource(RowResource[])\"\"\"
        return response.envelope(await self.service.rows())
""",
            ),
            {"DetailResource", "RowResource"},
        ).map()

        assert mapping["ThingController@show"] == ("DetailResource", False)
        assert mapping["ThingController@index"] == ("RowResource", True)

    def test_a_typo_fails_loudly_instead_of_emitting_a_dangling_ref(self, tmp_path: Path):
        mapper = ControllerActionMapper(
            _controllers(
                tmp_path,
                ThingController="""
class ThingController:
    async def show(self, request, response):
        \"\"\"@resource(DetialResource)\"\"\"
        return response.envelope({})
""",
            ),
            {"DetailResource"},
        )

        with pytest.raises(UnknownDeclaredResource):
            mapper.map()


class TestCursorDetection:
    def test_the_framework_response_method_is_always_a_page(self, tmp_path: Path):
        controllers = _controllers(
            tmp_path,
            ThingController="""
class ThingController:
    async def index(self, request, response):
        return response.paginated(rows, limit=10, has_more=False)

    async def show(self, request, response):
        return response.envelope(row)
""",
        )

        assert cursor_paginated_actions(controllers) == {"ThingController@index"}

    def test_a_house_page_helper_is_only_a_page_once_registered(self, tmp_path: Path):
        controllers = _controllers(
            tmp_path,
            ThingController="""
class ThingController:
    async def index(self, request, response):
        return finish_cursor_page(response, rows, limit)
""",
        )

        assert cursor_paginated_actions(controllers) == set()
        assert cursor_paginated_actions(controllers, {"finish_cursor_page"}) == {
            "ThingController@index"
        }
