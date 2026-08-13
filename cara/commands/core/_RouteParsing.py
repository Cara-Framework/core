"""Controller route annotation parsing and validation."""

from __future__ import annotations

import ast
import re
import traceback
from pathlib import Path

from cara.exceptions import CaraException, StorageException
from cara.support import paths


def _route_find_controllers(self) -> list[Path]:
    """Find all controller files in app/controllers directory."""
    controllers_dir = Path(paths("controllers"))

    if not controllers_dir.exists():
        return []

    controllers = []
    for file_path in controllers_dir.rglob("*.py"):
        if file_path.name != "__init__.py":
            controllers.append(file_path)

    return controllers


def _route_parse_controllers_enhanced(self, controller_files: list[Path]) -> list[dict]:
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


def _route_parse_controller_file_enhanced(self, file_path: Path) -> dict | None:
    """Parse a single controller file with enhanced features."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        raise StorageException(f"Cannot read file: {e}") from e

    # Parse AST to find class
    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        raise CaraException(f"Python syntax error in file: {e}") from e

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("Controller"):
            docstring = ast.get_docstring(node)
            if docstring:
                route_info = self._parse_enhanced_docstring(
                    docstring, node.name, file_path
                )
                if route_info:
                    # Validate controller methods exist
                    self._validate_controller_methods(route_info, content, file_path)
                    return route_info

    return None


def _route_parse_enhanced_docstring(
    self, docstring: str, class_name: str, file_path: Path
) -> dict | None:
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

    for line_num, original_line in enumerate(original_lines, 1):
        line = original_line.strip()
        if not line:
            continue

        try:
            # Calculate indent level
            len(original_line) - len(original_line.lstrip())

            # Parse sections based on markers
            if line.startswith("@Compiler"):
                current_section = "compiler"
                current_group = None
                if self.option("verbose"):
                    self.info("    Found @Compiler section")
                continue

            elif line.startswith("@routes."):
                current_section = "route_group"
                current_group = self._parse_routes_group(line, line_num, file_path)
                result["route_groups"].append(current_group)
                if self.option("verbose"):
                    self.info(
                        f"    Found @routes.{current_group['type']}: {current_group['prefix']}"
                    )
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


def _route_is_http_method_line(self, line: str) -> bool:
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


def _route_parse_routes_group(self, line: str, line_num: int, file_path: Path) -> dict:
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


def _route_parse_http_method(
    self, line: str, line_num: int, file_path: Path
) -> dict | None:
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


def _route_extract_http_method(
    self, line: str, line_num: int, file_path: Path
) -> str | None:
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


def _route_parse_route_parameters(
    self, line: str, line_num: int, file_path: Path
) -> dict | None:
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


def _route_parse_compiler_variable(
    self, line: str, compiler_vars: dict, line_num: int, file_path: Path
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


def _route_validate_controller_methods(
    self, route_info: dict, file_content: str, file_path: Path
):
    """Validate route handlers across the controller's static MRO."""
    try:
        existing_methods = self._class_methods(
            file_path,
            route_info["class_name"],
            source=file_content,
        )

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


def _route_class_methods(
    self,
    file_path: Path,
    class_name: str,
    *,
    source: str | None = None,
    seen: set[tuple[Path, str]] | None = None,
) -> set[str]:
    """Collect methods declared by one class and resolvable local bases.

    Controllers intentionally keep route docstrings on the thin edge class
    while cohesive handlers live in imported mixins. Validation therefore
    follows only the class's explicit bases; scanning every function in an
    imported module would let unrelated helpers satisfy a route by accident.
    """
    path = file_path.resolve()
    visited = seen if seen is not None else set()
    identity = (path, class_name)
    if identity in visited or not path.is_file():
        return set()
    visited.add(identity)

    tree = ast.parse(source if source is not None else path.read_text())
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    target = classes.get(class_name)
    if target is None:
        return set()

    methods = {
        node.name
        for node in target.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    imports = self._imported_classes(tree, path)
    for base in target.bases:
        if not isinstance(base, ast.Name):
            continue
        if base.id in classes:
            methods.update(
                self._class_methods(path, base.id, source=source, seen=visited)
            )
            continue
        imported = imports.get(base.id)
        if imported is not None:
            base_path, imported_name = imported
            methods.update(self._class_methods(base_path, imported_name, seen=visited))
    return methods


def _route_imported_classes(
    tree: ast.Module, file_path: Path
) -> dict[str, tuple[Path, str]]:
    """Resolve direct ``from module import Class`` bases to source files."""
    resolved: dict[str, tuple[Path, str]] = {}
    project_root = Path(paths("base")).resolve()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if node.level:
            module_root = file_path.parent
            for _ in range(node.level - 1):
                module_root = module_root.parent
            module_path = module_root.joinpath(*node.module.split("."))
        else:
            module_path = project_root.joinpath(*node.module.split("."))
        source_path = module_path.with_suffix(".py")
        if not source_path.is_file():
            source_path = module_path / "__init__.py"
        if not source_path.is_file():
            continue
        for alias in node.names:
            resolved[alias.asname or alias.name] = (source_path, alias.name)
    return resolved


def _route_validate_parsed_data(self, result: dict, file_path: Path):
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
