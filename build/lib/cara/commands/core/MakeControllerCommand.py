"""
Controller Generation Command for the Cara framework.
"""

from pathlib import Path

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="make:controller",
    help="Generate a new Controller class with enhanced options.",
    options={
        "--dry": "Show what would be generated without creating files",
        "--force": "Overwrite existing controller file",
        "--resource": "Generate a resource controller with CRUD methods",
        "--api": "Generate an API controller with JSON responses",
    },
)
class MakeControllerCommand(CommandBase):
    """Generate Controller classes with enhanced configuration."""

    def handle(self, name: str):
        """Handle controller generation."""
        self.info("🏗️  Controller Generation")

        try:
            controller_info = self._prepare_controller_info(name)
        except ValueError as e:
            self.error(f"❌ {e}")
            return

        if self._check_existing_file(controller_info):
            return

        if self.option("dry"):
            self._show_dry_run(controller_info)
            return

        try:
            self._generate_controller(controller_info)
        except Exception as e:
            self.error(f"❌ Failed to generate controller: {e}")

    def _prepare_controller_info(self, name: str) -> dict:
        """Prepare controller information."""
        if not name:
            raise ValueError("Controller name is required")

        class_name = self._clean_class_name(name)
        route_prefix = self._generate_route_prefix(class_name)
        route_name = self._generate_route_name(class_name)

        controllers_dir = Path(paths("controllers"))
        file_path = controllers_dir / f"{class_name}.py"

        return {
            "class_name": class_name,
            "route_prefix": route_prefix,
            "route_name": route_name,
            "file_path": file_path,
            "controllers_dir": controllers_dir,
            "is_resource": self.option("resource"),
            "is_api": self.option("api"),
        }

    def _clean_class_name(self, name: str) -> str:
        """Clean and format class name."""
        if name.endswith(".py"):
            name = name[:-3]
        name = "".join(word.capitalize() for word in name.replace("_", " ").split())
        if not name.endswith("Controller"):
            name += "Controller"
        return name

    def _generate_route_prefix(self, class_name: str) -> str:
        """Generate route prefix from class name."""
        name = class_name.replace("Controller", "")
        result = ""
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result += "-"
            result += char.lower()
        return result

    def _generate_route_name(self, class_name: str) -> str:
        """Generate route name from class name."""
        name = class_name.replace("Controller", "")
        result = ""
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result += "_"
            result += char.lower()
        return result

    def _check_existing_file(self, controller_info: dict) -> bool:
        """Check if controller file already exists."""
        if controller_info["file_path"].exists() and not self.option("force"):
            self.warning(f"⚠️  Controller already exists: {controller_info['file_path']}")
            self.info("💡 Use --force to overwrite existing controller")
            return True
        return False

    def _show_dry_run(self, controller_info: dict) -> None:
        """Show dry run preview."""
        self.info("🔍 DRY RUN MODE - No files will be created")
        self.info("📋 Controller configuration:")
        self.info(f"   Class Name: {controller_info['class_name']}")
        self.info(f"   Route Prefix: /{controller_info['route_prefix']}")
        self.info(f"   Route Name: {controller_info['route_name']}")
        self.info(f"   File Path: {controller_info['file_path']}")
        self.info(f"   Type: {'Resource' if controller_info['is_resource'] else 'Basic'}")
        self.info(f"   API: {'Yes' if controller_info['is_api'] else 'No'}")

    def _generate_controller(self, controller_info: dict) -> None:
        """Generate the controller file."""
        self.info("🔧 Configuration:")
        self.info(f"   Class: {controller_info['class_name']}")
        self.info(f"   Route: /{controller_info['route_prefix']}")
        self.info(f"   File: {controller_info['file_path']}")
        self.info(f"   Type: {'Resource' if controller_info['is_resource'] else 'Basic'}")

        controller_info["controllers_dir"].mkdir(parents=True, exist_ok=True)
        code = self._generate_controller_code(controller_info)

        self.info("⚡ Generating controller...")
        try:
            with open(controller_info["file_path"], "w", encoding="utf-8") as f:
                f.write(code)

            self.info("✅ Controller created successfully!")
            self.info(f"📁 Location: {controller_info['file_path']}")
            self._show_usage_tips(controller_info)

        except Exception as e:
            raise Exception(f"Failed to write controller file: {e}")

    def _generate_controller_code(self, controller_info: dict) -> str:
        """Generate the controller class code."""
        if controller_info["is_resource"]:
            stub_path = Path(paths("cara")) / "commands" / "stubs" / "Controller.stub"
        else:
            stub_path = (
                Path(paths("cara")) / "commands" / "stubs" / "BasicController.stub"
            )

        with open(stub_path, "r", encoding="utf-8") as f:
            stub_content = f.read()

        code = stub_content.replace("{{ class_name }}", controller_info["class_name"])
        code = code.replace("{{ route_prefix }}", controller_info["route_prefix"])
        code = code.replace("{{ route_name }}", controller_info["route_name"])
        code = code.replace(
            "{{ docstring }}",
            f"{controller_info['class_name']}\n\nGenerated by Cara framework make:controller command.",
        )

        return code

    def _show_usage_tips(self, controller_info: dict) -> None:
        """Show usage examples."""
        class_name = controller_info["class_name"]
        route_prefix = controller_info["route_prefix"]

        self.info("\n💡 Usage Tips:")
        self.info(f"   Import: from app.controllers import {class_name}")
        self.info("   Register routes (add to routes/api.py):")
        self.info(f"     from app.controllers import {class_name}")

        if controller_info["is_resource"]:
            self.info("   Generated routes:")
            self.info(f"     GET    /api/{route_prefix}      -> index()")
            self.info(f"     POST   /api/{route_prefix}      -> store()")
            self.info(f"     GET    /api/{route_prefix}/{{id}} -> show()")
            self.info(f"     PUT    /api/{route_prefix}/{{id}} -> update()")
            self.info(f"     DELETE /api/{route_prefix}/{{id}} -> destroy()")

        self.info("\n   Generate routes automatically:")
        self.info("     python craft routes:generate")
