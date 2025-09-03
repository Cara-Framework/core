"""
Model Generation Command for the Cara framework.

This module provides a CLI command to generate Eloquent model classes with enhanced UX.
"""

from pathlib import Path
from typing import Optional

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="make:model",
    help="Generate a new Eloquent model class with enhanced options.",
    options={
        "--dry": "Show what would be generated without creating files",
        "--table=?": "Specify the database table name",
        "--fillable=?": "Comma-separated list of fillable attributes",
        "--force": "Overwrite existing model file",
    },
)
class MakeModelCommand(CommandBase):
    """Generate Eloquent model classes with enhanced configuration."""

    def handle(
        self,
        name: str,
        table: Optional[str] = None,
        fillable: Optional[str] = None,
    ):
        """Handle model generation with enhanced options."""
        self.info("🏗️  Model Generation")

        # Validate and prepare
        try:
            model_info = self._prepare_model_info(name, table, fillable)
        except ValueError as e:
            self.error(f"❌ {e}")
            return

        # Check if file exists
        if self._check_existing_file(model_info):
            return

        # Dry run mode
        if self.option("dry"):
            self._show_dry_run(model_info)
            return

        # Generate model
        try:
            self._generate_model(model_info)
        except Exception as e:
            self.error(f"❌ Failed to generate model: {e}")
            return

    def _prepare_model_info(
        self, name: str, table: Optional[str], fillable: Optional[str]
    ) -> dict:
        """Prepare and validate model information."""
        if not name:
            raise ValueError("Model name is required")

        # Clean and validate name
        class_name = self._clean_class_name(name)
        if not class_name:
            raise ValueError("Invalid model name")

        # Determine table name
        table_name = table or self._generate_table_name(class_name)

        # Parse fillable attributes
        fillable_list = []
        if fillable:
            fillable_list = [attr.strip() for attr in fillable.split(",") if attr.strip()]

        # Determine file path
        models_dir = Path(paths("models"))
        file_path = models_dir / f"{class_name}.py"

        return {
            "class_name": class_name,
            "table_name": table_name,
            "fillable": fillable_list,
            "file_path": file_path,
            "models_dir": models_dir,
        }

    def _clean_class_name(self, name: str) -> str:
        """Clean and format class name."""
        # Remove .py extension if present
        if name.endswith(".py"):
            name = name[:-3]

        # Convert to PascalCase
        name = "".join(word.capitalize() for word in name.replace("_", " ").split())

        # Validate
        if not name.isidentifier():
            raise ValueError(f"'{name}' is not a valid Python class name")

        return name

    def _generate_table_name(self, class_name: str) -> str:
        """Generate table name from class name (simple pluralization)."""
        name = class_name.lower()

        # Simple pluralization rules
        if name.endswith("y"):
            return name[:-1] + "ies"
        elif name.endswith(("s", "sh", "ch", "x", "z")):
            return name + "es"
        else:
            return name + "s"

    def _check_existing_file(self, model_info: dict) -> bool:
        """Check if model file already exists."""
        if model_info["file_path"].exists() and not self.option("force"):
            self.warning(f"⚠️  Model already exists: {model_info['file_path']}")
            self.info("💡 Use --force to overwrite existing model")
            return True
        return False

    def _show_dry_run(self, model_info: dict) -> None:
        """Show what would be generated in dry run mode."""
        self.info("🔍 DRY RUN MODE - No files will be created")
        self.info("📋 Model configuration:")
        self.info(f"   Class Name: {model_info['class_name']}")
        self.info(f"   Table Name: {model_info['table_name']}")
        self.info(f"   File Path: {model_info['file_path']}")

        if model_info["fillable"]:
            self.info(f"   Fillable: {', '.join(model_info['fillable'])}")
        else:
            self.info("   Fillable: None specified")

        self.info("\n📄 Generated code preview:")
        code = self._generate_model_code(model_info)
        self.console.print(f"[dim]{code}[/dim]")

    def _generate_model(self, model_info: dict) -> None:
        """Generate the model file."""
        self.info("🔧 Configuration:")
        self.info(f"   Class: {model_info['class_name']}")
        self.info(f"   Table: {model_info['table_name']}")
        self.info(f"   File: {model_info['file_path']}")

        if model_info["fillable"]:
            self.info(f"   Fillable: {', '.join(model_info['fillable'])}")

        # Create directory if it doesn't exist
        model_info["models_dir"].mkdir(parents=True, exist_ok=True)

        # Generate code
        code = self._generate_model_code(model_info)

        # Write file
        self.info("⚡ Generating model...")

        try:
            with open(model_info["file_path"], "w", encoding="utf-8") as f:
                f.write(code)

            self.info("✅ Model created successfully!")
            self.info(f"📁 Location: {model_info['file_path']}")

            self._show_usage_tips(model_info)

        except Exception as e:
            raise Exception(f"Failed to write model file: {e}")

    def _generate_model_code(self, model_info: dict) -> str:
        """Generate the model class code."""
        class_name = model_info["class_name"]
        table_name = model_info["table_name"]
        fillable = model_info["fillable"]

        # Build fillable array
        fillable_code = ""
        if fillable:
            fillable_items = [f"'{attr}'" for attr in fillable]
            fillable_code = f"\n    __fillable__ = [{', '.join(fillable_items)}]"

        # Generate the model code
        code = f'''"""
{class_name} Model

Generated by Cara framework make:model command.
"""

from cara.orm import Model


class {class_name}(Model):
    """
    {class_name} Eloquent model.
    
    This model represents the '{table_name}' table in the database.
    """
    
    __table__ = '{table_name}'{fillable_code}
    
    # Add your model methods here
    pass
'''

        return code

    def _show_usage_tips(self, model_info: dict) -> None:
        """Show helpful usage tips after model creation."""
        class_name = model_info["class_name"]

        self.info("\n💡 Usage Tips:")
        self.info(f"   • Import: from app.models.{class_name} import {class_name}")
        self.info(f"   • Create: {class_name}.create({{'name': 'value'}})")
        self.info(f"   • Find: {class_name}.find(1)")
        self.info(f"   • All: {class_name}.all()")
        self.info("   • Don't forget to create a migration for the table!")
        self.info(
            f"   • Run: craft make:migration create_{model_info['table_name']}_table"
        )
