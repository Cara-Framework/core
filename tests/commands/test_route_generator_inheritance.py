from pathlib import Path

from cara.commands.core.RouteGeneratorCommand import RouteGeneratorCommand


def test_route_validation_follows_explicit_controller_mixins(tmp_path: Path) -> None:
    mixin = tmp_path / "_EdgeMixin.py"
    mixin.write_text(
        "class _EdgeMixin:\n"
        "    async def inherited_handler(self, request):\n"
        "        return request\n"
    )
    controller = tmp_path / "ExampleController.py"
    source = (
        "from ._EdgeMixin import _EdgeMixin\n\n"
        "class ExampleController(_EdgeMixin):\n"
        "    def local_handler(self):\n"
        "        return None\n"
    )
    controller.write_text(source)
    command = object.__new__(RouteGeneratorCommand)
    command.errors = []
    command.warnings = []
    route_info = {
        "class_name": "ExampleController",
        "route_groups": [
            {
                "routes": [
                    {
                        "methods": [
                            {"controller_method": "inherited_handler", "line_num": 1},
                            {"controller_method": "local_handler", "line_num": 2},
                        ]
                    }
                ]
            }
        ],
    }

    command._validate_controller_methods(route_info, source, controller)

    assert command.errors == []
    assert command.warnings == []


def test_route_validation_rejects_unrelated_imported_methods(tmp_path: Path) -> None:
    helper = tmp_path / "Helper.py"
    helper.write_text("def missing_handler():\n    return None\n")
    controller = tmp_path / "ExampleController.py"
    source = "from .Helper import missing_handler\n\nclass ExampleController:\n    pass\n"
    controller.write_text(source)
    command = object.__new__(RouteGeneratorCommand)
    command.errors = []
    command.warnings = []
    route_info = {
        "class_name": "ExampleController",
        "route_groups": [
            {
                "routes": [
                    {"methods": [{"controller_method": "missing_handler", "line_num": 1}]}
                ]
            }
        ],
    }

    command._validate_controller_methods(route_info, source, controller)

    assert len(command.errors) == 1
    assert "missing_handler" in command.errors[0]
