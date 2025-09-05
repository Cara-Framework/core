"""
ModelDiscoverer: Discover and parse model files.
"""

import ast
import re
from pathlib import Path
from typing import Dict, List, Optional

from cara.support import paths


class ModelDiscoverer:
    """Discover model files and extract Field.* definitions."""

    # Field types that take field names as first argument
    FIELD_TYPES_WITH_NAMES = {
        "string", "text", "integer", "tiny_integer", "small_integer", 
        "medium_integer", "big_integer", "unsigned_integer", "unsigned_big_integer",
        "decimal", "boolean", "enum", "uuid", "json", "timestamp", 
        "date", "time", "datetime", "id", "increments", "big_increments", "float"
    }
    
    # Field types that don't take field names
    FIELD_TYPES_WITHOUT_NAMES = {"timestamps", "soft_deletes", "foreign", "foreign_key"}

    def __init__(self):
        # Don't resolve path at init time - do it at runtime when needed
        self.models_dir = None

    def discover_models(self) -> List[Dict]:
        """Discover all model files by scanning for classes that inherit from Model"""
        models = []
        
        # Get project root
        project_root = Path(paths("")).parent if paths("") else Path.cwd()
        
        # Scan project root with max 5 levels deep
        models.extend(self._scan_path_for_models(project_root, max_depth=5))
        
        # Deduplicate by model name - keep first occurrence
        seen_names = set()
        unique_models = []
        for model in models:
            if model['name'] not in seen_names:
                # Exclude models from within Cara framework
                model_file = model.get('file', '')
                if '/cara/' in model_file and ('eloquent' in model_file or 'queues' in model_file):
                    continue
                    
                seen_names.add(model['name'])
                unique_models.append(model)
        
        return unique_models
        
    def _scan_path_for_models(self, path: Path, max_depth: int = 5, current_depth: int = 0) -> List[Dict]:
        """Recursively scan a path for model files with depth limit"""
        models = []
        
        # Stop if we've reached max depth
        if current_depth >= max_depth:
            return models
            
        try:
            for item in path.iterdir():
                # Skip hidden directories, venv, __pycache__, .git, etc.
                if (item.name.startswith('.') or 
                    item.name in ['venv', '__pycache__', 'node_modules', 'build', 'dist', '.git']):
                    continue
                    
                if item.is_dir():
                    # Recursively scan subdirectories
                    models.extend(self._scan_path_for_models(item, max_depth, current_depth + 1))
                elif item.is_file() and item.suffix == '.py':
                    # Skip __init__.py and test files
                    if (item.name.startswith('__') or 
                        item.name.startswith('test_') or
                        item.name.endswith('_test.py')):
                        continue
                        
                    try:
                        model_info = self._parse_model_file(item)
                        if model_info:
                            # Add file path to model info
                            model_info['file'] = str(item)
                            models.append(model_info)
                    except Exception:
                        # Skip files that can't be parsed
                        continue
        except PermissionError:
            # Skip directories we can't read
            pass
                
        return models
        
    def _discover_models_from_imports(self, init_file: Path) -> List[Dict]:
        """Discover models by parsing imports from __init__.py"""
        models = []
        
        try:
            with open(init_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    # Handle: from commons.models.core import User, Category
                    if node.module:
                        models.extend(self._resolve_import_models(node.module, node.names))
                elif isinstance(node, ast.Import):
                    # Handle: import commons.models.core.User
                    for alias in node.names:
                        if 'models' in alias.name:
                            models.extend(self._resolve_direct_import_models(alias.name))
                            
        except Exception as e:
            print(f"Warning: Could not parse {init_file}: {e}")
            
        return models
        
    def _resolve_import_models(self, module_path: str, names: List[ast.alias]) -> List[Dict]:
        """Resolve model files from import statements - generic implementation"""
        # Skip import resolution - rely on directory scanning instead
        # This keeps the framework completely app-agnostic
        return []
        
    def _resolve_direct_import_models(self, import_path: str) -> List[Dict]:
        """Handle direct imports - generic implementation"""
        # Skip import resolution - rely on directory scanning instead
        # This keeps the framework completely app-agnostic
        return []
        
    def _discover_models_from_packages(self, packages_dir: Path) -> List[Dict]:
        """Discover models from packages directory structure - generic implementation"""
        # Skip packages-specific discovery - rely on general directory scanning
        # This keeps the framework completely app-agnostic
        return []
        
    def _scan_directory_for_models(self, directory: Path) -> List[Dict]:
        """Scan a directory for model files."""
        models = []
        
        for py_file in directory.glob("*.py"):
            if py_file.name.startswith("__"):
                continue

            try:
                model_info = self._parse_model_file(py_file)
                if model_info:
                    models.append(model_info)
            except Exception:
                # Skip files that can't be parsed
                continue
                
        return models

    def resolve_dependency_order(self, models: List[Dict]) -> List[Dict]:
        """Resolve dependency order for models (FK dependencies first)."""
        
        # First pass: Resolve SQL dependencies for raw SQL models
        all_table_names = [model["table"] for model in models]
        
        for model in models:
            if model.get("needs_sql_dependency_resolution"):
                self._extract_raw_sql_dependencies(model, all_table_names)
        
        # Second pass: Build dependency graph
        dependency_graph = {}

        for model in models:
            table_name = model["table"]
            dependencies = []
            foreign_keys = []  # Track foreign keys for this model

            # Check for foreign key fields
            for field_name, field_info in model["fields"].items():
                # Check for explicit foreign key info from fluent API
                foreign_key_info = field_info.get("foreign_key")
                if foreign_key_info and foreign_key_info.get("on"):
                    referenced_table = foreign_key_info["on"]
                    dependencies.append(referenced_table)
                    foreign_keys.append(
                        {
                            "field": field_name,
                            "references_table": referenced_table,
                            "references_field": foreign_key_info.get("references", "id"),
                            "on_delete": foreign_key_info.get("on_delete", "RESTRICT"),
                        }
                    )
                # Fallback to old detection method
                elif self._is_foreign_key_field(field_name, field_info):
                    referenced_table = self._extract_referenced_table(
                        field_name, field_info
                    )
                    if referenced_table:
                        dependencies.append(referenced_table)
                        foreign_keys.append(
                            {
                                "field": field_name,
                                "references_table": referenced_table,
                                "references_field": "id",
                                "on_delete": "SET NULL"
                                if field_info.get("params", {}).get("nullable", False)
                                else "RESTRICT",
                            }
                        )

            dependency_graph[table_name] = dependencies
            model["foreign_keys"] = foreign_keys

        # Perform topological sort
        sorted_models = self._topological_sort(models, dependency_graph)

        return sorted_models

    def _parse_model_file(self, file_path: Path) -> Optional[Dict]:
        """Parse model file and extract Field.* structure."""
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content)

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                if self._is_model_class(node):
                    return self._extract_model_structure(node, file_path.stem, str(file_path))

        return None

    def _is_model_class(self, class_node: ast.ClassDef) -> bool:
        """Check if class extends Model."""
        for base in class_node.bases:
            if isinstance(base, ast.Name) and base.id == "Model":
                return True
        return False

    def _extract_model_structure(self, class_node: ast.ClassDef, filename: str, file_path: str = None) -> Dict:
        """Extract model structure from AST looking for table attribute and fields() method."""
        model_info = {
            "name": class_node.name,
            "filename": filename,
            "file": file_path,
            "table": None,
            "fields": {},
            "uses_soft_deletes": False,
            "has_fields_method": False,

        }

        # Check if model uses SoftDeletesMixin
        for base in class_node.bases:
            if isinstance(base, ast.Name) and base.id == "SoftDeletesMixin":
                model_info["uses_soft_deletes"] = True

        for node in class_node.body:
            if isinstance(node, ast.Assign):
                self._parse_model_attribute(node, model_info)
            elif isinstance(node, ast.FunctionDef) and node.name == "fields":
                model_info["has_fields_method"] = True
                self._parse_fields_method(node, model_info)


        # Set default table name if not specified
        if not model_info["table"]:
            model_info["table"] = self._snake_case(class_node.name)

        # Note: Runtime field processing removed to avoid database connection issues
        # AST parsing provides sufficient field information for migration generation

        return model_info

    def _parse_model_attribute(self, assign_node: ast.Assign, model_info: Dict):
        """Parse model class attributes looking for __columns__ dict, __table__, and other special attributes."""
        for target in assign_node.targets:
            if isinstance(target, ast.Name):
                # Parse __table__ attribute
                if target.id == "__table__" and isinstance(
                    assign_node.value, ast.Constant
                ):
                    model_info["table"] = assign_node.value.value

                # Parse __columns__ = {...} dict (old syntax)
                elif target.id == "__columns__" and isinstance(
                    assign_node.value, ast.Dict
                ):
                    self._parse_fields_dict(assign_node.value, model_info)

                # Parse __fillable__ = [...] (future feature)
                elif target.id == "__fillable__" and isinstance(
                    assign_node.value, ast.List
                ):
                    fillable = []
                    for element in assign_node.value.elts:
                        if isinstance(element, ast.Constant):
                            fillable.append(element.value)
                    model_info["fillable"] = fillable

                # Parse __guarded__ = [...] (future feature)
                elif target.id == "__guarded__" and isinstance(
                    assign_node.value, ast.List
                ):
                    guarded = []
                    for element in assign_node.value.elts:
                        if isinstance(element, ast.Constant):
                            guarded.append(element.value)
                    model_info["guarded"] = guarded

                # Parse __primary_key__ = "id" (future feature)
                elif target.id == "__primary_key__" and isinstance(
                    assign_node.value, ast.Constant
                ):
                    model_info["primary_key"] = assign_node.value.value

                # Parse __connection__ = "database_name" (future feature)
                elif target.id == "__connection__" and isinstance(
                    assign_node.value, ast.Constant
                ):
                    model_info["connection"] = assign_node.value.value

                # Parse __timestamps__ = False (future feature)
                elif target.id == "__timestamps__" and isinstance(
                    assign_node.value, ast.Constant
                ):
                    model_info["timestamps"] = assign_node.value.value

    def _parse_fields_dict(self, dict_node: ast.Dict, model_info: Dict):
        """Parse __columns__ = {...} dictionary and extract Field.* definitions."""
        for key, value in zip(dict_node.keys, dict_node.values):
            if isinstance(key, ast.Constant) and isinstance(value, ast.Call):
                field_name = key.value
                field_definition = self._extract_field_definition(value)
                if field_definition:
                    model_info["fields"][field_name] = field_definition

    def _extract_field_definition(self, call_node: ast.Call) -> Optional[Dict]:
        """Extract Field.* definition from AST call node (old syntax)."""
        if (
            isinstance(call_node.func, ast.Attribute)
            and isinstance(call_node.func.value, ast.Name)
            and call_node.func.value.id == "Field"
        ):
            field_type = call_node.func.attr
            params = {}

            # Extract positional arguments
            for i, arg in enumerate(call_node.args):
                if isinstance(arg, ast.Constant):
                    if field_type == "decimal":
                        if i == 0:
                            params["precision"] = arg.value
                        elif i == 1:
                            params["scale"] = arg.value
                    elif field_type == "string":
                        if i == 0:
                            params["length"] = arg.value

            # Extract keyword arguments
            for keyword in call_node.keywords:
                if isinstance(keyword.value, ast.Constant):
                    params[keyword.arg] = keyword.value.value
                elif isinstance(keyword.value, ast.List):
                    # Handle list values like options=["value1", "value2"]
                    list_values = []
                    for element in keyword.value.elts:
                        if isinstance(element, ast.Constant):
                            list_values.append(element.value)
                    params[keyword.arg] = list_values

            return {"type": field_type, "params": params}

        return None

    def _parse_fields_method(self, method_node: ast.FunctionDef, model_info: Dict):
        """Parse fields() method that returns Schema.build(lambda field: (...)) or dict with up/down."""
        for stmt in method_node.body:
            if isinstance(stmt, ast.Return):
                # Check if it returns a dict with 'up' and 'down' keys (raw SQL)
                if isinstance(stmt.value, ast.Dict):
                    self._parse_raw_sql_fields(stmt.value, model_info)
                # Check if it's Schema.build(lambda field: (...))
                elif isinstance(stmt.value, ast.Call):
                    if (
                        isinstance(stmt.value.func, ast.Attribute)
                        and isinstance(stmt.value.func.value, ast.Name)
                        and stmt.value.func.value.id == "Schema"
                        and stmt.value.func.attr == "build"
                    ):
                        # Extract lambda argument
                        if stmt.value.args and isinstance(stmt.value.args[0], ast.Lambda):
                            lambda_node = stmt.value.args[0]
                            self._parse_lambda_fields(lambda_node, model_info)

    def _parse_lambda_fields(self, lambda_node: ast.Lambda, model_info: Dict):
        """Parse lambda field: (...) body to extract field definitions."""
        if isinstance(lambda_node.body, ast.Tuple):
            # Handle tuple of field definitions
            for field_call in lambda_node.body.elts:
                if isinstance(field_call, ast.Call):
                    # Check if this is a separate foreign key definition
                    if self._is_separate_foreign_key_call(field_call):
                        foreign_key_def = self._extract_separate_foreign_key_definition(field_call)
                        if foreign_key_def:
                            field_name = foreign_key_def["field"]
                            # Add foreign key info to existing field
                            if field_name in model_info["fields"]:
                                model_info["fields"][field_name]["foreign_key"] = foreign_key_def
                    else:
                        # Always try to extract field definition first
                        field_def = self._extract_field_definition_new_syntax(field_call)
                        if field_def:
                            field_name = self._extract_field_name_from_call(field_call)
                            if field_name:
                                model_info["fields"][field_name] = field_def
                            else:
                                # Handle special fields without names (timestamps, soft_deletes)
                                field_type = field_def.get("type")
                                if field_type in ["timestamps", "soft_deletes"]:
                                    model_info["fields"][field_type] = field_def

    def _extract_field_definition_new_syntax(self, call_node: ast.Call) -> Optional[Dict]:
        """Extract field definition from new syntax: field.string("name").nullable()"""
        field_type = None
        params = {}
        foreign_key_info = {}

        # Traverse the call chain to extract field type and modifiers
        current = call_node
        while current:
            if isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    # This is a method call like .nullable() or .default(value)
                    method_name = current.func.attr

                    if method_name in (self.FIELD_TYPES_WITH_NAMES | self.FIELD_TYPES_WITHOUT_NAMES):
                        # This is the base field type
                        field_type = method_name
                        # Extract positional arguments (like precision/scale for decimal)
                        for i, arg in enumerate(current.args):
                            if isinstance(arg, ast.Constant):
                                if field_type == "decimal":
                                    if i == 1:  # First arg after field name
                                        params["precision"] = arg.value
                                    elif i == 2:  # Second arg after field name
                                        params["scale"] = arg.value
                                elif field_type == "string":
                                    if i == 1:  # Length parameter
                                        params["length"] = arg.value
                            elif field_type == "enum" and i == 1:
                                # Handle enum options list
                                if isinstance(arg, ast.List):
                                    options = []
                                    for opt in arg.elts:
                                        if isinstance(opt, ast.Constant):
                                            options.append(opt.value)
                                    params["options"] = options

                    elif method_name == "nullable":
                        params["nullable"] = True
                    elif method_name == "unique":
                        params["unique"] = True
                    elif method_name == "default":
                        if current.args and isinstance(current.args[0], ast.Constant):
                            params["default"] = current.args[0].value

                    # Foreign key methods
                    elif method_name == "foreign":
                        foreign_key_info["is_foreign"] = True
                    elif method_name == "references":
                        if current.args and isinstance(current.args[0], ast.Constant):
                            foreign_key_info["references"] = current.args[0].value
                    elif method_name == "on":
                        if current.args and isinstance(current.args[0], ast.Constant):
                            foreign_key_info["on"] = current.args[0].value
                    elif method_name == "on_delete":
                        if current.args and isinstance(current.args[0], ast.Constant):
                            foreign_key_info["on_delete"] = current.args[0].value
                    elif method_name == "on_update":
                        if current.args and isinstance(current.args[0], ast.Constant):
                            foreign_key_info["on_update"] = current.args[0].value

                    # Move to the object being called (chaining)
                    current = current.func.value
                else:
                    break
            else:
                break

        if field_type:
            result = {"type": field_type, "params": params}

            # Add foreign key information if this is a foreign key
            if foreign_key_info.get("is_foreign"):
                # Get field name for foreign key config
                field_name = self._extract_field_name_from_call(call_node)
                if field_name:
                    foreign_key_config = {
                        "field": field_name,
                        "references": foreign_key_info.get("references"),
                        "on": foreign_key_info.get("on"),
                        "on_delete": foreign_key_info.get("on_delete"),
                        "on_update": foreign_key_info.get("on_update"),
                    }
                    result["foreign_key"] = foreign_key_config

            return result
        return None

    def _extract_field_name_from_call(self, call_node: ast.Call) -> Optional[str]:
        """Extract field name from the first string argument in the call chain."""
        # We need to find the base field type call (like field.string("name"))
        # and extract the field name from there, not from modifier calls like .default(True)

        current = call_node
        while current:
            if isinstance(current, ast.Call):
                # Check if this is a base field type call
                if (
                    isinstance(current.func, ast.Attribute)
                    and isinstance(current.func.value, ast.Name)
                    and current.func.value.id == "field"
                ):
                    field_method = current.func.attr
                    if field_method in self.FIELD_TYPES_WITH_NAMES:
                        # This is the base field call, extract first string argument
                        if (
                            current.args
                            and isinstance(current.args[0], ast.Constant)
                            and isinstance(current.args[0].value, str)
                        ):
                            return current.args[0].value
                    elif field_method in self.FIELD_TYPES_WITHOUT_NAMES:
                        # Special fields that don't take field names
                        return None

                # Move to chained call
                if isinstance(current.func, ast.Attribute):
                    current = current.func.value
                else:
                    break
            else:
                break
        return None

    def _is_foreign_key_field(self, field_name: str, field_info: Dict) -> bool:
        """Check if field is a foreign key (ends with _id or explicitly marked)."""
        return field_name.endswith("_id") or field_info.get("params", {}).get(
            "foreign_key", False
        )

    def _extract_referenced_table(
        self, field_name: str, field_info: Dict
    ) -> Optional[str]:
        """Extract referenced table name from foreign key field."""
        # For fields ending with _id, assume table name is the prefix
        if field_name.endswith("_id"):
            return field_name[:-3]  # Remove _id suffix

        # Check for explicit references parameter
        return field_info.get("params", {}).get("references")

    def _topological_sort(
        self, models: List[Dict], dependency_graph: Dict[str, List[str]]
    ) -> List[Dict]:
        """Topological sort using a simple iterative approach."""
        model_lookup = {model["table"]: model for model in models}
        result = []
        remaining = list(models)
        processed_tables = set()
        
        # Keep iterating until all models are processed
        while remaining:
            # Find models with all dependencies satisfied
            ready_models = []
            for model in remaining:
                table_name = model["table"]
                dependencies = dependency_graph.get(table_name, [])
                # Check if all dependencies are already processed
                if all(dep in processed_tables or dep == table_name or dep not in model_lookup 
                       for dep in dependencies):
                    ready_models.append(model)
            
            if not ready_models:
                # If no models are ready, there might be a circular dependency
                # Add the first remaining model to break the cycle
                ready_models = [remaining[0]]
            
            # Add ready models to result and mark as processed
            for model in ready_models:
                result.append(model)
                processed_tables.add(model["table"])
                remaining.remove(model)
        
        return result

    def _snake_case(self, camel_str: str) -> str:
        """Convert CamelCase to snake_case."""
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_str)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _parse_raw_sql_fields(self, dict_node: ast.Dict, model_info: Dict):
        """Parse fields() method that returns {'up': function, 'down': function}."""
        has_up_function = False
        has_down_function = False
        
        for key, value in zip(dict_node.keys, dict_node.values):
            if isinstance(key, ast.Constant) and key.value in ['up', 'down']:
                # Check if value is a function (ast.Name referring to a local function)
                if isinstance(value, ast.Name):
                    if key.value == 'up':
                        has_up_function = True
                    elif key.value == 'down':
                        has_down_function = True
        
        if has_up_function:
            model_info["has_raw_sql"] = True
            model_info["has_up_function"] = True
            model_info["has_down_function"] = has_down_function
            
            # Extract SQL dependencies from the up() function (will be called later with all_known_tables)
            model_info["needs_sql_dependency_resolution"] = True

    def _is_separate_foreign_key_call(self, call_node: ast.Call) -> bool:
        """Check if this is a separate foreign key call: field.foreign("field_name").references("id").on("table")"""
        # Traverse the call chain to look for 'foreign' method
        current = call_node
        while current:
            if isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    if (isinstance(current.func.value, ast.Name) and
                        current.func.value.id == "field" and
                        current.func.attr == "foreign"):
                        return True
                    # Move to the object being called (chaining)
                    current = current.func.value
                else:
                    break
            else:
                break
        return False

    def _extract_separate_foreign_key_definition(self, call_node: ast.Call) -> Optional[Dict]:
        """Extract separate foreign key definition from field.foreign("field_name").references("id").on("table")"""
        field_name = None
        references = None
        on_table = None
        on_delete = None
        on_update = None

        # Use the same traversal logic as _extract_field_definition_new_syntax
        current = call_node
        while current:
            if isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    method_name = current.func.attr

                    if method_name == "foreign" and current.args:
                        if isinstance(current.args[0], ast.Constant):
                            field_name = current.args[0].value
                    elif method_name == "references" and current.args:
                        if isinstance(current.args[0], ast.Constant):
                            references = current.args[0].value
                    elif method_name == "on" and current.args:
                        if isinstance(current.args[0], ast.Constant):
                            on_table = current.args[0].value
                    elif method_name == "onDelete" and current.args:
                        if isinstance(current.args[0], ast.Constant):
                            on_delete = current.args[0].value
                    elif method_name == "onUpdate" and current.args:
                        if isinstance(current.args[0], ast.Constant):
                            on_update = current.args[0].value

                    # Move to next in chain
                    current = current.func.value
                else:
                    break
            else:
                break

        if field_name and on_table:
            return {
                "field": field_name,
                "references": references or "id",
                "on": on_table,
                "on_delete": on_delete,
                "on_update": on_update
            }
        return None

    def _extract_raw_sql_dependencies(self, model_info: Dict, all_known_tables: List[str] = None):
        """Extract table dependencies from raw SQL in the up() function."""
        if all_known_tables is None:
            all_known_tables = []
        try:
            # Get the model file path
            model_file = model_info.get('file')
            if not model_file:
                return
                
            # Read the file content
            with open(model_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse the AST to find the up() function
            tree = ast.parse(content)
            
            # Find the up() function within the fields property
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == "up":
                    # Extract SQL from DB.statement() calls
                    sql_content = self._extract_sql_from_function(node)
                    if sql_content:
                        # Parse SQL for table references
                        dependencies = self._parse_sql_for_dependencies(sql_content, model_info, all_known_tables)
                        if dependencies:
                            # Add dependencies to fields for dependency resolution
                            for dep_table in dependencies:
                                field_name = f"_sql_dependency_{dep_table}"
                                model_info["fields"][field_name] = {
                                    "type": "sql_dependency",
                                    "foreign_key": {
                                        "on": dep_table,
                                        "references": "id"
                                    }
                                }
                        
        except Exception:
            # Silently continue if we can't extract dependencies
            pass

    def _extract_sql_from_function(self, function_node: ast.FunctionDef) -> str:
        """Extract SQL content from DB.statement() calls in function."""
        sql_parts = []
        
        for node in ast.walk(function_node):
            if isinstance(node, ast.Call):
                # Look for DB.statement("SQL") calls
                if (isinstance(node.func, ast.Attribute) and 
                    node.func.attr == "statement" and
                    isinstance(node.func.value, ast.Name) and
                    node.func.value.id == "DB"):
                    
                    # Extract string argument
                    if node.args and isinstance(node.args[0], ast.Constant):
                        sql_parts.append(node.args[0].value)
        
        return "\n".join(sql_parts)

    def _parse_sql_for_dependencies(self, sql: str, model_info: Dict, all_known_tables: List[str]) -> List[str]:
        """Parse SQL content to find table dependencies using known tables from discovery."""
        dependencies = []
        
        # Extract all potential table references from SQL
        potential_tables = set()
        
        # Pattern 1: FROM table_name
        from_matches = re.finditer(r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        for match in from_matches:
            potential_tables.add(match.group(1).lower())
        
        # Pattern 2: JOIN table_name
        join_matches = re.finditer(r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        for match in join_matches:
            potential_tables.add(match.group(1).lower())
        
        # Pattern 3: REFERENCES table_name (for foreign keys)
        ref_matches = re.finditer(r'REFERENCES\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
        for match in ref_matches:
            potential_tables.add(match.group(1).lower())
        
        # Pattern 4: Extract foreign key column patterns (column_id -> column table)
        fk_column_matches = re.finditer(r'([a-zA-Z_][a-zA-Z0-9_]*)_id', sql, re.IGNORECASE)
        for match in fk_column_matches:
            base_name = match.group(1).lower()
            potential_tables.add(base_name)
        
        # Filter potential tables against known tables from model discovery
        current_table = model_info.get('table', '').lower()
        known_tables_lower = [t.lower() for t in all_known_tables]
        
        for table_name in potential_tables:
            if (table_name != current_table and 
                table_name in known_tables_lower and
                table_name not in dependencies):
                dependencies.append(table_name)
        
        return dependencies

