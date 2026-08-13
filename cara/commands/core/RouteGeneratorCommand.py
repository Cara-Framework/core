"""
Enhanced Route Generator Command for the Cara framework.

This module provides a CLI command to generate routes from controller docstring annotations automatically.
Supports full Laravel-style routing features with validation and error checking.

Features:
- Route prefixes and nested groups
- Middleware (parametered and simple)
- Route aliases and names
- Parameter validation and constraints
- Route model binding
- Resource controllers
- Nested route groups
- Syntax validation before writing
- Rollback support on errors
"""

from __future__ import annotations

import shutil
import tempfile
import traceback
from pathlib import Path

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.exceptions import CaraException, StorageException
from cara.support import paths

from . import _RouteParsing, _RouteRendering


@command(
    name="routes:generate",
    help="Generate routes files (api.py, web.py, websocket.py) from enhanced controller docstring annotations",
    options=[
        {
            "name": "--dry",
            "help": "Show what would be generated without creating files",
            "is_flag": True,
        },
        {
            "name": "--overwrite",
            "help": "Overwrite existing routes files",
            "is_flag": True,
        },
        {
            "name": "--type",
            "help": "Generate specific route type only (api, web, websocket)",
            "type": str,
        },
        {
            "name": "--validate",
            "help": "Only validate syntax without generating files",
            "is_flag": True,
        },
        {
            "name": "--backup",
            "help": "Create backup before overwriting",
            "is_flag": True,
        },
        {
            "name": "--verbose",
            "help": "Show detailed parsing information",
            "is_flag": True,
        },
    ],
)
class RouteGeneratorCommand(CommandBase):
    """Generate robust route definitions from enhanced controller docstrings."""

    # Keep headroom for Ruff's multiline expansion. Controller annotations
    # render compactly, but formatting can roughly triple the physical lines.
    MAX_ROUTE_SHARD_LINES = 180

    def __init__(self, application=None):
        super().__init__(application)
        self.errors = []
        self.warnings = []
        self.parsed_routes = []
        self.backup_file = None

    def handle(self, type: str | None = None):
        """
        Generate route definitions from enhanced controller docstring annotations.

        Enhanced docstring format:

        @Compiler
            user_id: (int|min:1)
            slug: (string|alpha_dash|max:50)

        @routes.api(prefix="/users", middleware=["auth", "throttle:60,1"])
            @get(path="/", method="index", as="users.index")
            @post(path="/", method="store", as="users.store")
            @get(path="/@id:int", method="show", as="users.show")
            @put(path="/@id:int", method="update", as="users.update")
            @delete(path="/@id:int", method="destroy", as="users.destroy")

        @routes.websocket(prefix="/ws")
            @connect(path="/job/@job_id:int", method="handle_job_status", as="websocket.job.status")
            @connect(path="/notifications", method="handle_notifications", as="websocket.notifications")

        @routes.web(prefix="/admin", middleware=["auth", "admin"])
            @get(path="/dashboard", method="dashboard", as="admin.dashboard")
            @get(path="/users", method="users", as="admin.users")
        """
        self.info("🛣️  Enhanced Route Generator")

        # Determine which route types to generate
        route_types = []
        if type:
            if type in ["api", "web", "websocket"]:
                route_types = [type]
            else:
                self.error(f"❌ Invalid route type: {type}. Use: api, web, websocket")
                return 1
        else:
            route_types = ["api", "web", "websocket"]

        # Step 1: Scan and parse controllers
        self.info("🔍 Scanning controllers for route annotations...")

        try:
            controllers = self._find_controllers()
            if not controllers:
                self.warning("⚠️  No controllers found in app/controllers directory")
                return 0

            self.info(f"📋 Found {len(controllers)} controller(s)")

        except Exception as e:
            self.error(f"❌ Failed to scan controllers: {e}")
            return 1

        # Step 2: Parse with enhanced validation
        try:
            route_data = self._parse_controllers_enhanced(controllers)
            if not route_data and not self.errors:
                self.warning("⚠️  No route annotations found in controllers")
                return 0

        except Exception as e:
            self.error(f"❌ Failed to parse controllers: {e}")
            if self.option("verbose"):
                self.error(f"Stack trace: {traceback.format_exc()}")
            return 1

        # Step 3: Show validation results
        if self.errors:
            self._show_validation_errors()
            return 1

        if self.warnings:
            self._show_warnings()

        # Step 4: Validate mode
        if self.option("validate"):
            self.success("✅ All route definitions are valid!")
            return 0

        # Step 5: Generate routes by type
        generated_files = []
        for route_type in route_types:
            try:
                type_data = self._filter_routes_by_type(route_data, route_type)
                if not type_data:
                    self.info(f"⚠️  No {route_type} routes found")
                    continue

                artifacts = self._generate_route_artifacts(type_data, route_type)
                wrote_all = True
                for output_file, content in artifacts.items():
                    if not self._validate_generated_syntax(content):
                        return 1

                    if self.option("dry"):
                        self._show_dry_run(
                            output_file,
                            content,
                            type_data,
                            route_type,
                        )
                    else:
                        if self._write_routes_file_safe(output_file, content):
                            generated_files.append(output_file)
                        else:
                            wrote_all = False

                if not self.option("dry") and wrote_all:
                    self._remove_stale_route_shards(
                        route_type,
                        keep=set(artifacts),
                    )

            except Exception as e:
                self.error(f"❌ Failed to generate {route_type} routes: {e}")
                if self.option("verbose"):
                    self.error(f"Stack trace: {traceback.format_exc()}")
                return 1

        if not self.option("dry") and generated_files:
            self.success(f"✅ Generated {len(generated_files)} route files:")
            for file in generated_files:
                self.info(f"  📄 {file}")

        return 0

    def _show_dry_run(
        self, output_file: str, content: str, route_data: list[dict], route_type: str
    ):
        """Show dry run output for specific route type."""
        self.info(
            f"🏃 Dry run - {route_type.upper()} routes would be written to: {output_file}"
        )
        self.info("=" * 60)
        for line in content.split("\n")[:30]:  # Show first 30 lines
            self.info(line)
        if len(content.split("\n")) > 30:
            self.info("... (truncated)")
        self.info("=" * 60)

    def _validate_parameters(self, output_file: str) -> bool:
        """Validate command parameters."""
        output_path = Path(paths("base")) / output_file

        # Check if we can write to the output directory
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.error(f"❌ Cannot create output directory: {e}")
            return False

        return True

    def _validate_generated_syntax(self, content: str) -> bool:
        """Validate the generated Python syntax."""
        self.info("🔍 Validating generated syntax...")

        try:
            # Try to compile the generated code
            compile(content, "<generated_routes>", "exec")
            self.info("  ✓ Syntax validation passed")
            return True
        except SyntaxError as e:
            self.error(f"❌ Generated code has syntax error: {e}")
            self.error(f"   Line {e.lineno}: {e.text}")
            return False
        except Exception as e:
            self.error(f"❌ Error validating syntax: {e}")
            return False

    def _show_validation_errors(self):
        """Show all validation errors."""
        self.error(f"❌ Found {len(self.errors)} validation error(s):")
        for error in self.errors:
            self.error(f"  • {error}")

    def _show_warnings(self):
        """Show all warnings."""
        self.warning(f"⚠️  Found {len(self.warnings)} warning(s):")
        for warning in self.warnings:
            self.warning(f"  • {warning}")

    def _show_enhanced_dry_run(
        self, output_file: str, content: str, route_data: list[dict]
    ):
        """Show enhanced dry run information."""
        self.info("🔍 DRY RUN MODE - No files will be created")
        self.info(f"📁 Would create/update: {output_file}")

        # Show statistics
        total_routes = sum(
            len(method)
            for controller in route_data
            for group in controller["route_groups"]
            for route in group["routes"]
            for method in route["methods"]
        )

        self.info("📊 Statistics:")
        self.info(f"   Controllers: {len(route_data)}")
        self.info(f"   Total Routes: {total_routes}")

        if self.option("verbose"):
            self.info("")
            self.info("📄 Generated content preview:")
            self.info("=" * 60)
            self.console.print(f"[dim]{content}[/dim]")
            self.info("=" * 60)

    def _write_routes_file_safe(self, output_file: str, content: str) -> bool:
        """Write routes file with backup and rollback support."""
        output_path = Path(paths("base")) / output_file

        # Create backup if file exists
        if output_path.exists() and self.option("backup"):
            self.backup_file = output_path.with_suffix(".py.backup")
            try:
                shutil.copy2(output_path, self.backup_file)
                self.info(f"📦 Backup created: {self.backup_file}")
            except Exception as e:
                self.warning(f"⚠️  Could not create backup: {e}")

        # Check if file exists and not overwrited
        if output_path.exists() and not self.option("overwrite"):
            self.warning(f"⚠️  File already exists: {output_file}")
            self.info("💡 Use --overwrite to overwrite existing file")
            return False

        # Create directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file atomically
        self.info("⚡ Generating routes file...")

        try:
            # Write to temporary file first
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=output_path.parent,
                prefix=".routes_temp_",
                suffix=".py",
                delete=False,
            ) as temp_file:
                temp_file.write(content)
                temp_path = Path(temp_file.name)

            # Test the generated file
            if self._test_generated_file(temp_path):
                # Move to final location
                temp_path.rename(output_path)

                self.success("✅ Routes generated successfully!")
                self.info(f"📁 Location: {output_file}")

                if self.backup_file:
                    self.info(f"📦 Backup available: {self.backup_file}")

                self._show_usage_tips()
                return True
            else:
                # Clean up temp file
                temp_path.unlink(missing_ok=True)
                raise CaraException("Generated file failed validation tests")

        except Exception as e:
            # Rollback if we have a backup
            if self.backup_file and self.backup_file.exists():
                try:
                    shutil.copy2(self.backup_file, output_path)
                    self.info("🔄 Rolled back to backup file")
                except OSError, RuntimeError, AttributeError, ConnectionError:
                    pass

            raise StorageException(f"Failed to write routes file: {e}") from e

    def _test_generated_file(self, file_path: Path) -> bool:
        """Test the generated routes file."""
        try:
            # Read and compile
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            compile(content, str(file_path), "exec")
            return True

        except Exception as e:
            self.error(f"❌ Generated file failed validation: {e}")
            return False

    def _get_import_path(self, file_path: Path) -> str:
        """Generate import path from file path."""
        # Convert file path to module path
        # app/controllers/AppController.py -> app.controllers.AppController
        relative_path = file_path.relative_to(Path(paths("base")))
        module_path = str(relative_path.with_suffix(""))
        return module_path.replace("/", ".")

    def _show_usage_tips(self):
        """Show helpful usage tips after generation."""
        self.info("\n💡 Enhanced Usage Tips:")
        self.info("   • Import routes in your main application file")
        self.info("   • Use enhanced docstring format for full Laravel features")
        self.info("   • Run with --validate to check syntax without generating")
        self.info("   • Use --backup to create safety backups")
        self.info("   • Run 'craft routes:list' to view all registered routes")
        self.info("   • Check controller methods exist before adding to docstrings")

    _class_methods = _RouteParsing._route_class_methods
    _extract_http_method = _RouteParsing._route_extract_http_method
    _find_controllers = _RouteParsing._route_find_controllers
    _imported_classes = staticmethod(_RouteParsing._route_imported_classes)
    _is_http_method_line = _RouteParsing._route_is_http_method_line
    _parse_compiler_variable = _RouteParsing._route_parse_compiler_variable
    _parse_controller_file_enhanced = _RouteParsing._route_parse_controller_file_enhanced
    _parse_controllers_enhanced = _RouteParsing._route_parse_controllers_enhanced
    _parse_enhanced_docstring = _RouteParsing._route_parse_enhanced_docstring
    _parse_http_method = _RouteParsing._route_parse_http_method
    _parse_route_parameters = _RouteParsing._route_parse_route_parameters
    _parse_routes_group = _RouteParsing._route_parse_routes_group
    _validate_controller_methods = _RouteParsing._route_validate_controller_methods
    _validate_parsed_data = _RouteParsing._route_validate_parsed_data

    _build_route_instance = _RouteRendering._route_build_route_instance
    _build_route_params = _RouteRendering._route_build_route_params
    _chunk_route_groups = _RouteRendering._route_chunk_route_groups
    _filter_routes_by_type = _RouteRendering._route_filter_routes_by_type
    _generate_compiler_lines = staticmethod(
        _RouteRendering._route_generate_compiler_lines
    )
    _generate_controller_route_groups = (
        _RouteRendering._route_generate_controller_route_groups
    )
    _generate_enhanced_routes_content = (
        _RouteRendering._route_generate_enhanced_routes_content
    )
    _generate_route_aggregator_content = (
        _RouteRendering._route_generate_route_aggregator_content
    )
    _generate_route_artifacts = _RouteRendering._route_generate_route_artifacts
    _generate_route_group = _RouteRendering._route_generate_route_group
    _generate_route_methods_for_group = (
        _RouteRendering._route_generate_route_methods_for_group
    )
    _generate_route_shard_content = staticmethod(
        _RouteRendering._route_generate_route_shard_content
    )
    _generate_routes_content_by_type = (
        _RouteRendering._route_generate_routes_content_by_type
    )
    _normalize_route_path = _RouteRendering._route_normalize_route_path
    _remove_stale_route_shards = _RouteRendering._route_remove_stale_route_shards
