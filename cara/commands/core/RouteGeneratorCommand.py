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

import ast
import re
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List, Optional

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="routes:generate",
    help="Generate routes files (api.py, web.py, websocket.py) from enhanced controller docstring annotations",
    options={
        "--dry": "Show what would be generated without creating files",
        "--overwrite": "Overwrite existing routes files",
        "--type=?": "Generate specific route type only (api, web, websocket)",
        "--validate": "Only validate syntax without generating files",
        "--backup": "Create backup before overwriting",
        "--verbose": "Show detailed parsing information",
    },
)
class RouteGeneratorCommand(CommandBase):
    """Generate robust route definitions from enhanced controller docstrings."""

    def __init__(self, application=None):
        super().__init__(application)
        self.errors = []
        self.warnings = []
        self.parsed_routes = []
        self.backup_file = None

    def handle(self, type: str = None):
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

                content = self._generate_routes_content_by_type(type_data, route_type)
                output_file = f"routes/{route_type}.py"

                # Syntax validation
                if not self._validate_generated_syntax(content):
                    return 1

                if self.option("dry"):
                    self._show_dry_run(output_file, content, type_data, route_type)
                else:
                    self._write_routes_file_safe(output_file, content)
                    generated_files.append(output_file)

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

    def _filter_routes_by_type(
        self, route_data: List[Dict], route_type: str
    ) -> List[Dict]:
        """Filter routes by type (api, web, websocket)."""
        filtered_data = []

        for controller_info in route_data:
            filtered_controller = {
                "class_name": controller_info["class_name"],
                "file_path": controller_info["file_path"],
                "compiler_vars": controller_info["compiler_vars"],
                "route_groups": [],
                "global_middleware": controller_info["global_middleware"],
                "global_prefix": controller_info["global_prefix"],
            }

            # Filter route groups by type
            for group in controller_info["route_groups"]:
                group_type = group.get("type", "api")  # Default to api if not specified
                if group_type == route_type:
                    filtered_controller["route_groups"].append(group)

            # Only include controller if it has routes of this type
            if filtered_controller["route_groups"]:
                filtered_data.append(filtered_controller)

        return filtered_data

    def _generate_routes_content_by_type(
        self, route_data: List[Dict], route_type: str
    ) -> str:
        """Generate routes content for specific type."""
        content_lines = []

        # Header
        route_type_title = route_type.title()
        header = [
            '"""',
            f"{route_type_title} Routes - Auto-generated by enhanced routes:generate command",
            "",
            f"This file contains all {route_type} routes parsed from controller docstrings.",
            "DO NOT EDIT MANUALLY - Use controller docstring annotations instead.",
            "",
            "Generated routes support:",
            "- Route groups with prefixes and middleware",
            "- Named routes and aliases",
            "- Parameter constraints and validation",
            "- Middleware (simple and parametered)",
            '"""',
            "",
            "from cara.routing import Route",
            "",
        ]

        content_lines.extend(header)

        # Generate route parameter validation and compilation
        content_lines.append("# Route Parameter Configuration")
        for controller_info in route_data:
            if controller_info["compiler_vars"]:
                for var_name, constraints in controller_info["compiler_vars"].items():
                    # Same compiler logic as before
                    rules = constraints.split("|")
                    regex_rules = []
                    validation_rules = []

                    for rule in rules:
                        if rule.strip() in [
                            "int",
                            "integer",
                            "string",
                            "alpha",
                            "alphanum",
                            "slug",
                            "uuid",
                            "bool",
                            "any",
                        ]:
                            regex_rules.append(rule.strip())
                        elif rule.strip().startswith("regex:"):
                            regex_rules.append(rule.strip())
                        else:
                            validation_rules.append(rule.strip())

                    if regex_rules:
                        regex_pattern = regex_rules[0]
                        if regex_pattern.startswith("regex:"):
                            pattern = regex_pattern[6:]
                            content_lines.append(
                                f'Route.compile("{var_name}", r"{pattern}")'
                            )
                        else:
                            content_lines.append(
                                f'Route.compile("{var_name}", "{regex_pattern}")'
                            )

                    if validation_rules:
                        validation_chain = "|".join(validation_rules)
                        content_lines.append(
                            f'Route.validate("{var_name}", "{validation_chain}")'
                        )

        content_lines.append("")

        # Generate register_routes function
        content_lines.append("def register_routes():")
        content_lines.append(f'    """Register {route_type} routes."""')

        if not route_data:
            content_lines.append("    return []")
        elif len(route_data) == 1:
            # Single controller - use same nested structure as multiple controllers
            controller_info = route_data[0]
            route_groups = self._generate_controller_route_groups(controller_info)
            if route_groups:
                if route_type == "api":
                    content_lines.append('    return Route.prefix("/api").routes(')
                    for group in route_groups:
                        content_lines.append(f"        {group}")
                    content_lines.append("    )")
                else:
                    # For websocket and web routes, return as list
                    if len(route_groups) == 1:
                        content_lines.append(f"    return {route_groups[0]}")
                    else:
                        content_lines.append("    return [")
                        for group in route_groups:
                            content_lines.append(f"        {group},")
                        content_lines.append("    ]")
            else:
                content_lines.append("    return []")
        else:
            # Multiple controllers
            if route_type == "api":
                content_lines.append('    return Route.prefix("/api").routes(')
                all_groups = []
                for controller_info in route_data:
                    route_groups = self._generate_controller_route_groups(controller_info)
                    all_groups.extend(route_groups)
                for group in all_groups:
                    content_lines.append(f"        {group},")
                content_lines.append("    )")
            else:
                content_lines.append("    return [")
                for controller_info in route_data:
                    route_groups = self._generate_controller_route_groups(controller_info)
                    for group in route_groups:
                        content_lines.append(f"        {group},")
                content_lines.append("    ]")

        content_lines.append("")
        return "\n".join(content_lines)

    def _show_dry_run(
        self, output_file: str, content: str, route_data: List[Dict], route_type: str
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

    def _find_controllers(self) -> List[Path]:
        """Find all controller files in app/controllers directory."""
        controllers_dir = Path(paths("controllers"))

        if not controllers_dir.exists():
            return []

        controllers = []
        for file_path in controllers_dir.rglob("*.py"):
            if file_path.name != "__init__.py":
                controllers.append(file_path)

        return controllers

    def _parse_controllers_enhanced(self, controller_files: List[Path]) -> List[Dict]:
        """Parse controller files with enhanced validation."""
        route_data = []

        for controller_file in controller_files:
            try:
                controller_info = self._parse_controller_file_enhanced(controller_file)
                if controller_info:
                    route_data.append(controller_info)
                    # Count total routes
                    total_routes = sum(
                        len(route["methods"])
                        for group in controller_info.get("route_groups", [])
                        for route in group.get("routes", [])
                    )
                    if self.option("verbose"):
                        self.info(f"  ✓ {controller_file.stem} -> {total_routes} routes")
                    else:
                        self.info(f"  ✓ {controller_file.stem}")

            except Exception as e:
                error_msg = f"Failed to parse {controller_file.stem}: {e}"
                self.errors.append(error_msg)
                if self.option("verbose"):
                    self.error(f"  ❌ {error_msg}")
                    self.error(f"     Stack trace: {traceback.format_exc()}")

        return route_data

    def _parse_controller_file_enhanced(self, file_path: Path) -> Optional[Dict]:
        """Parse a single controller file with enhanced features."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception as e:
            raise Exception(f"Cannot read file: {e}")

        # Parse AST to find class
        try:
            tree = ast.parse(content)
        except SyntaxError as e:
            raise Exception(f"Python syntax error in file: {e}")

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if this is a controller class
                if node.name.endswith("Controller"):
                    docstring = ast.get_docstring(node)
                    if docstring:
                        route_info = self._parse_enhanced_docstring(
                            docstring, node.name, file_path
                        )
                        if route_info:
                            # Validate controller methods exist
                            self._validate_controller_methods(
                                route_info, content, file_path
                            )
                            return route_info

        return None

    def _parse_enhanced_docstring(
        self, docstring: str, class_name: str, file_path: Path
    ) -> Optional[Dict]:
        """Parse enhanced docstring format with full Laravel-style features."""
        original_lines = docstring.split("\n")

        result = {
            "class_name": class_name,
            "file_path": file_path,
            "compiler_vars": {},
            "route_groups": [],
            "global_middleware": [],
            "global_prefix": "",
        }

        current_section = None
        current_group = None
        current_route = None

        for line_num, original_line in enumerate(original_lines, 1):
            line = original_line.strip()
            if not line:
                continue

            try:
                # Calculate indent level
                current_indent = len(original_line) - len(original_line.lstrip())

                # Parse sections based on markers
                if line.startswith("@Compiler"):
                    current_section = "compiler"
                    current_group = None
                    current_route = None
                    if self.option("verbose"):
                        self.info("    Found @Compiler section")
                    continue

                elif line.startswith("@routes."):
                    current_section = "route_group"
                    current_group = self._parse_routes_group(line, line_num, file_path)
                    result["route_groups"].append(current_group)
                    current_route = None
                    if self.option("verbose"):
                        self.info(
                            f"    Found @routes.{current_group['type']}: {current_group['prefix']}"
                        )
                    continue

                elif line.startswith("@Route") and current_section == "route_group":
                    # Legacy @Route support
                    current_route = self._parse_route_definition(
                        line, line_num, file_path
                    )
                    if current_group:
                        current_group["routes"].append(current_route)
                        if self.option("verbose"):
                            self.info(f"      Found @Route: {current_route['name']}")
                    continue

                elif (
                    line.startswith("@")
                    and current_section == "route_group"
                    and current_group
                ):
                    # Check if it's a direct HTTP method (new syntax)
                    if self._is_http_method_line(line):
                        method_info = self._parse_http_method(line, line_num, file_path)
                        if method_info:
                            # Create a temporary route container for the method
                            temp_route = {
                                "name": "",
                                "middleware": [],
                                "methods": [method_info],
                                "line_num": line_num,
                            }
                            current_group["routes"].append(temp_route)
                            if self.option("verbose"):
                                self.info(
                                    f"        Found HTTP method: {method_info['http_method']} -> {method_info['controller_method']}"
                                )
                        continue

                    # Legacy: HTTP method definition within a @Route
                    elif current_route:
                        method_info = self._parse_http_method(line, line_num, file_path)
                        if method_info:
                            current_route["methods"].append(method_info)
                            if self.option("verbose"):
                                self.info(
                                    f"        Found HTTP method: {method_info['http_method']} -> {method_info['controller_method']}"
                                )
                        continue

                # Parse compiler variables
                if current_section == "compiler" and ":" in line:
                    self._parse_compiler_variable(
                        line, result["compiler_vars"], line_num, file_path
                    )

            except Exception as e:
                error_msg = f"Error parsing line {line_num} in {file_path.name}: {e}"
                self.errors.append(error_msg)
                if self.option("verbose"):
                    self.error(f"    Parse error: {error_msg}")

        # Validate parsed data
        self._validate_parsed_data(result, file_path)

        return result if result["route_groups"] else None

    def _is_http_method_line(self, line: str) -> bool:
        """Check if a line contains a valid HTTP method or WebSocket method."""
        method_match = re.match(r"@(\w+)\(", line)
        if not method_match:
            return False

        method = method_match.group(1).lower()
        valid_methods = [
            "get",
            "post",
            "put",
            "patch",
            "delete",
            "head",
            "options",
            "ws",
            "connect",
        ]
        return method in valid_methods

    def _parse_routes_group(self, line: str, line_num: int, file_path: Path) -> Dict:
        """Parse @routes.api(prefix="/api", middleware=["auth"]) definition."""
        group = {
            "type": "api",  # Default to api
            "prefix": "",
            "middleware": [],
            "routes": [],
            "line_num": line_num,
        }

        # Extract route type from @routes.TYPE(...)
        type_match = re.search(r"@routes\.(\w+)\(", line)
        if type_match:
            group["type"] = type_match.group(1)

        # Extract prefix
        prefix_match = re.search(r'prefix=["\']([^"\']+)["\']', line)
        if prefix_match:
            group["prefix"] = prefix_match.group(1)

        # Extract middleware
        middleware_match = re.search(r"middleware=\[([^\]]+)\]", line)
        if middleware_match:
            middleware_str = middleware_match.group(1)
            # Parse middleware list: ["auth", "throttle:60,1"]
            middleware_items = re.findall(r'["\']([^"\']+)["\']', middleware_str)
            group["middleware"] = middleware_items

        return group

    def _parse_route_group(self, line: str, line_num: int, file_path: Path) -> Dict:
        """Parse @RouteGroup(type="api", prefix="/api", middleware=["auth"]) definition. (Legacy support)"""
        group = {
            "type": "api",  # Default to api
            "prefix": "",
            "middleware": [],
            "routes": [],
            "line_num": line_num,
        }

        # Extract type
        type_match = re.search(r'type=["\']([^"\']+)["\']', line)
        if type_match:
            group["type"] = type_match.group(1)

        # Extract prefix
        prefix_match = re.search(r'prefix=["\']([^"\']+)["\']', line)
        if prefix_match:
            group["prefix"] = prefix_match.group(1)

        # Extract middleware
        middleware_match = re.search(r"middleware=\[([^\]]+)\]", line)
        if middleware_match:
            middleware_str = middleware_match.group(1)
            # Parse middleware list: ["auth", "throttle:60,1"]
            middleware_items = re.findall(r'["\']([^"\']+)["\']', middleware_str)
            group["middleware"] = middleware_items

        return group

    def _parse_route_definition(self, line: str, line_num: int, file_path: Path) -> Dict:
        """Parse @Route(name="/users/{id}", middleware=["auth"]) definition."""
        route = {
            "name": "",
            "middleware": [],
            "methods": [],
            "line_num": line_num,
        }

        # Extract name (path)
        name_match = re.search(r'name=["\']([^"\']+)["\']', line)
        if name_match:
            route["name"] = name_match.group(1)
        else:
            self.errors.append(
                f"Missing 'name' in @Route at line {line_num} in {file_path.name}"
            )

        # Extract middleware
        middleware_match = re.search(r"middleware=\[([^\]]+)\]", line)
        if middleware_match:
            middleware_str = middleware_match.group(1)
            middleware_items = re.findall(r'["\']([^"\']+)["\']', middleware_str)
            route["middleware"] = middleware_items

        # Note: constraints are handled by compile method, not where parameter

        return route

    def _parse_http_method(
        self, line: str, line_num: int, file_path: Path
    ) -> Optional[Dict]:
        """Parse @get(path="/path", method="method", as="route.name") or @connect(path="/path", method="method", as="ws.name") definition."""

        # Extract and validate HTTP method
        http_method = self._extract_http_method(line, line_num, file_path)
        if not http_method:
            return None

        # Parse route parameters
        params = self._parse_route_parameters(line, line_num, file_path)
        if not params:
            return None

        # Convert connect to ws for route generation
        if http_method == "connect":
            http_method = "ws"

        return {
            "http_method": http_method,
            "controller_method": params["method"],
            "path": params["path"],
            "as": params.get("as"),
            "middleware": params.get("middleware", []),
            "line_num": line_num,
        }

    def _extract_http_method(
        self, line: str, line_num: int, file_path: Path
    ) -> Optional[str]:
        """Extract and validate HTTP method from decorator line."""
        method_match = re.match(r"@(\w+)\(", line)
        if not method_match:
            self.errors.append(
                f"Invalid route method format at line {line_num} in {file_path.name}"
            )
            return None

        http_method = method_match.group(1).lower()

        # Validate method types
        valid_methods = [
            "get",
            "post",
            "put",
            "patch",
            "delete",
            "head",
            "options",
            "ws",
            "connect",
        ]

        if http_method not in valid_methods:
            self.errors.append(
                f"Invalid route method '{http_method}' at line {line_num} in {file_path.name}"
            )
            return None

        return http_method

    def _parse_route_parameters(
        self, line: str, line_num: int, file_path: Path
    ) -> Optional[Dict]:
        """Parse explicit route parameters: path="/path", method="method", as="name", middleware=["auth"]"""
        params = {}

        # Extract path parameter
        path_match = re.search(r'path\s*=\s*["\']([^"\']*)["\']', line)
        if path_match:
            params["path"] = path_match.group(1)
        else:
            self.errors.append(
                f"Missing 'path' parameter at line {line_num} in {file_path.name}"
            )
            return None

        # Extract method parameter
        method_match = re.search(r'method\s*=\s*["\']([^"\']+)["\']', line)
        if method_match:
            params["method"] = method_match.group(1)
        else:
            self.errors.append(
                f"Missing 'method' parameter at line {line_num} in {file_path.name}"
            )
            return None

        # Extract optional as parameter
        as_match = re.search(r'as\s*=\s*["\']([^"\']+)["\']', line)
        if as_match:
            params["as"] = as_match.group(1)

        # Extract optional middleware parameter
        middleware_match = re.search(r"middleware\s*=\s*\[([^\]]+)\]", line)
        if middleware_match:
            middleware_str = middleware_match.group(1)
            params["middleware"] = re.findall(r'["\']([^"\']+)["\']', middleware_str)

        return params

    def _parse_compiler_variable(
        self, line: str, compiler_vars: Dict, line_num: int, file_path: Path
    ):
        """Parse compiler variable: user_id: (int|min:1)"""
        var_match = re.match(r"(\w+):\s*\(([^)]+)\)", line)
        if var_match:
            var_name, constraints = var_match.groups()
            compiler_vars[var_name] = constraints
        else:
            self.warnings.append(
                f"Invalid compiler variable format at line {line_num} in {file_path.name}: {line}"
            )

    def _validate_controller_methods(
        self, route_info: Dict, file_content: str, file_path: Path
    ):
        """Validate that controller methods actually exist."""
        # Extract method names from the file
        try:
            tree = ast.parse(file_content)
            existing_methods = set()

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    existing_methods.add(node.name)

            # Check all route methods
            for group in route_info["route_groups"]:
                for route in group["routes"]:
                    for method in route["methods"]:
                        method_name = method["controller_method"]
                        if method_name not in existing_methods:
                            self.errors.append(
                                f"Method '{method_name}' not found in controller {route_info['class_name']} "
                                f"at line {method['line_num']} in {file_path.name}"
                            )

        except Exception as e:
            self.warnings.append(f"Could not validate methods in {file_path.name}: {e}")

    def _validate_parsed_data(self, result: Dict, file_path: Path):
        """Validate the parsed route data for consistency."""
        # Check for duplicate route names
        route_names = set()

        for group in result["route_groups"]:
            for route in group["routes"]:
                for method in route["methods"]:
                    if method.get("as"):
                        route_name = method["as"]
                        if route_name in route_names:
                            self.errors.append(
                                f"Duplicate route name '{route_name}' in {file_path.name}"
                            )
                        route_names.add(route_name)

    def _generate_enhanced_routes_content(self, route_data: List[Dict]) -> str:
        """Generate enhanced routes content with route groups."""
        content_lines = []

        # Header
        header = [
            '"""',
            "API Routes - Auto-generated by enhanced routes:generate command",
            "",
            "This file contains all application routes parsed from controller docstrings.",
            "DO NOT EDIT MANUALLY - Use controller docstring annotations instead.",
            "",
            "Generated routes support:",
            "- Route groups with prefixes and middleware",
            "- Named routes and aliases",
            "- Parameter constraints and validation",
            "- Middleware (simple and parametered)",
            '"""',
            "",
            "from cara.routing import Route",
            "",
        ]

        content_lines.extend(header)

        # Generate route parameter validation and compilation
        content_lines.append("# Route Parameter Configuration")
        for controller_info in route_data:
            if controller_info["compiler_vars"]:
                for var_name, constraints in controller_info["compiler_vars"].items():
                    # Separate regex patterns from validation rules
                    rules = constraints.split("|")
                    regex_rules = []
                    validation_rules = []

                    for rule in rules:
                        if rule.strip() in [
                            "int",
                            "integer",
                            "string",
                            "alpha",
                            "alphanum",
                            "slug",
                            "uuid",
                            "bool",
                            "any",
                        ]:
                            regex_rules.append(rule.strip())
                        elif rule.strip().startswith("regex:"):
                            regex_rules.append(rule.strip())
                        else:
                            validation_rules.append(rule.strip())

                    # Set regex pattern if any
                    if regex_rules:
                        # Use the first regex rule for Route.compile
                        regex_pattern = regex_rules[0]
                        if regex_pattern.startswith("regex:"):
                            pattern = regex_pattern[6:]  # Remove "regex:" prefix
                            content_lines.append(
                                f'Route.compile("{var_name}", r"{pattern}")'
                            )
                        else:
                            content_lines.append(
                                f'Route.compile("{var_name}", "{regex_pattern}")'
                            )

                    # Set validation rules if any
                    if validation_rules:
                        validation_chain = "|".join(validation_rules)
                        content_lines.append(
                            f'Route.validate("{var_name}", "{validation_chain}")'
                        )

        content_lines.append("")

        # Generate register_routes function
        content_lines.append("def register_routes():")
        content_lines.append('    """Register all application routes."""')

        if len(route_data) == 1:
            # Single controller - return the route group directly
            controller_info = route_data[0]
            route_groups = self._generate_controller_route_groups(controller_info)
            if route_groups:
                content_lines.append(f"    return {route_groups[0]}")
        else:
            # Multiple controllers - wrap in a common prefix
            content_lines.append('    return Route.prefix("/api").routes(')

            # Generate route groups for each controller
            all_groups = []
            for controller_info in route_data:
                route_groups = self._generate_controller_route_groups(controller_info)
                all_groups.extend(route_groups)

            for group in all_groups:
                content_lines.append(f"        {group},")

            content_lines.append("    )")

        content_lines.append("")

        return "\n".join(content_lines)

    def _generate_controller_route_groups(self, controller_info: Dict):
        """Generate route groups for a single controller."""
        class_name = controller_info["class_name"]
        route_groups = []

        for group in controller_info["route_groups"]:
            group_instance = self._generate_route_group(group, class_name)
            route_groups.append(group_instance)

        return route_groups

    def _generate_route_group(self, group: Dict, class_name: str):
        """Generate a single route group instance."""
        prefix = group["prefix"]
        middleware = group["middleware"]

        # Generate routes within the group
        group_routes = []
        for route in group["routes"]:
            route_methods = self._generate_route_methods_for_group(
                route, class_name, middleware or []
            )
            group_routes.extend(route_methods)

        # Build route group string
        group_parts = []
        if prefix:
            group_parts.append(f'Route.prefix("{prefix}")')

        if middleware:
            middleware_str = ", ".join(f'"{m}"' for m in middleware)
            if group_parts:
                group_parts.append(f".middleware([{middleware_str}])")
            else:
                group_parts.append(f"Route.middleware([{middleware_str}])")

        if not group_parts:
            group_parts.append("Route")

        # Add routes using Cara's syntax - pass routes as individual arguments, not a list
        routes_str = ",\n            ".join(group_routes)
        group_str = (
            "".join(group_parts) + f".routes(\n            {routes_str}\n        )"
        )

        return group_str

    def _generate_route_methods_for_group(
        self,
        route: Dict,
        class_name: str,
        group_middleware: List[str],
    ):
        """Generate HTTP/WebSocket methods for a route within a group."""
        return [
            self._build_route_instance(method, class_name) for method in route["methods"]
        ]

    def _build_route_instance(self, method: Dict, class_name: str) -> str:
        """Build a single Route instance string."""
        http_method = method["http_method"].lower()
        controller_method = method["controller_method"]
        route_path = self._normalize_route_path(method.get("path", ""))

        # Build route parameters
        params = self._build_route_params(
            path=route_path,
            controller=f"{class_name}@{controller_method}",
            name=method.get("as"),
            middleware=method.get("middleware", []),
        )

        # Generate route instance
        params_str = ", ".join(params)
        route_method = "ws" if http_method == "ws" else http_method

        return f"Route.{route_method}({params_str})"

    def _normalize_route_path(self, path: str) -> str:
        """Convert {param} to @param format for Cara routing."""
        if not path:
            return ""
        return re.sub(r"\{(\w+)\}", r"@\1", path)

    def _build_route_params(
        self, path: str, controller: str, name: str = None, middleware: List[str] = None
    ) -> List[str]:
        """Build route parameters list."""
        params = [f'"{path}"', f'"{controller}"']

        # Add optional middleware
        if middleware:
            middleware_str = ", ".join(f'"{m}"' for m in middleware)
            params.append(f"middleware=[{middleware_str}]")

        # Add optional route name
        if name:
            params.append(f'name="{name}"')

        return params

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
        self, output_file: str, content: str, route_data: List[Dict]
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

    def _write_routes_file_safe(self, output_file: str, content: str):
        """Write routes file with backup and rollback support."""
        output_path = Path(paths("base")) / output_file

        # Create backup if file exists
        if output_path.exists() and self.option("backup"):
            self.backup_file = output_path.with_suffix(".py.backup")
            try:
                import shutil

                shutil.copy2(output_path, self.backup_file)
                self.info(f"📦 Backup created: {self.backup_file}")
            except Exception as e:
                self.warning(f"⚠️  Could not create backup: {e}")

        # Check if file exists and not overwrited
        if output_path.exists() and not self.option("overwrite"):
            self.warning(f"⚠️  File already exists: {output_file}")
            self.info("💡 Use --overwrite to overwrite existing file")
            return

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
            else:
                # Clean up temp file
                temp_path.unlink(missing_ok=True)
                raise Exception("Generated file failed validation tests")

        except Exception as e:
            # Rollback if we have a backup
            if self.backup_file and self.backup_file.exists():
                try:
                    import shutil

                    shutil.copy2(self.backup_file, output_path)
                    self.info("🔄 Rolled back to backup file")
                except Exception:
                    pass

            raise Exception(f"Failed to write routes file: {e}")

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
