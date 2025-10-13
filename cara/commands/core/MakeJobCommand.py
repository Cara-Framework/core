"""
Job Generation Command for the Cara framework.

This module provides a CLI command to generate Job classes with enhanced UX.
"""

from pathlib import Path

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="make:job",
    help="Generate a new Job class with enhanced options.",
    options={
        "--dry": "Show what would be generated without creating files",
        "--force": "Overwrite existing job file",
        "--sync": "Generate a synchronous job (without ShouldQueue)",
    },
)
class MakeJobCommand(CommandBase):
    """Generate Job classes with enhanced configuration."""

    def handle(self, name: str):
        """Handle job generation with enhanced options."""
        self.info("🏗️  Job Generation")

        # Validate and prepare
        try:
            job_info = self._prepare_job_info(name)
        except ValueError as e:
            self.error(f"❌ {e}")
            return

        # Check if file exists
        if self._check_existing_file(job_info):
            return

        # Dry run mode
        if self.option("dry"):
            self._show_dry_run(job_info)
            return

        # Generate job
        try:
            self._generate_job(job_info)
        except Exception as e:
            self.error(f"❌ Failed to generate job: {e}")
            return

    def _prepare_job_info(self, name: str) -> dict:
        """Prepare job information."""
        if not name:
            raise ValueError("Job name is required")

        class_name = self._clean_class_name(name)
        jobs_dir = Path(paths("jobs"))  # Using proper path helper
        file_path = jobs_dir / f"{class_name}.py"

        return {
            "class_name": class_name,
            "file_path": file_path,
            "jobs_dir": jobs_dir,
            "is_sync": self.option("sync"),
        }

    def _clean_class_name(self, name: str) -> str:
        """Clean and format class name."""
        # Remove .py extension if present
        if name.endswith(".py"):
            name = name[:-3]

        # Convert to PascalCase preserving existing capitalization
        if "_" in name or " " in name:
            # Split on underscores and spaces, then capitalize each word
            name = "".join(word.capitalize() for word in name.replace("_", " ").split())
        else:
            # Preserve existing PascalCase
            name = name[0].upper() + name[1:] if name else ""

        if not name.endswith("Job"):
            name += "Job"

        # Validate
        if not name.isidentifier():
            raise ValueError(f"'{name}' is not a valid Python class name")

        return name

    def _check_existing_file(self, job_info: dict) -> bool:
        """Check if job file already exists."""
        if job_info["file_path"].exists() and not self.option("force"):
            self.warning(f"⚠️  Job already exists: {job_info['file_path']}")
            self.info("💡 Use --force to overwrite existing job")
            return True
        return False

    def _show_dry_run(self, job_info: dict) -> None:
        """Show what would be generated in dry run mode."""
        self.info("🔍 DRY RUN MODE - No files will be created")
        self.info("📋 Job configuration:")
        self.info(f"   Class Name: {job_info['class_name']}")
        self.info(f"   File Path: {job_info['file_path']}")
        self.info(f"   Type: {'Synchronous' if job_info['is_sync'] else 'Queueable'}")

        self.info("\n📄 Generated code preview:")
        code = self._generate_job_code(job_info)
        self.console.print(f"[dim]{code}[/dim]")

    def _generate_job(self, job_info: dict) -> None:
        """Generate the job file."""
        self.info("🔧 Configuration:")
        self.info(f"   Class: {job_info['class_name']}")
        self.info(f"   File: {job_info['file_path']}")
        self.info(f"   Type: {'Synchronous' if job_info['is_sync'] else 'Queueable'}")

        # Create directory if it doesn't exist
        job_info["jobs_dir"].mkdir(parents=True, exist_ok=True)

        # Generate code
        code = self._generate_job_code(job_info)

        # Write file
        self.info("⚡ Generating job...")

        try:
            with open(job_info["file_path"], "w", encoding="utf-8") as f:
                f.write(code)

            self.info("✅ Job created successfully!")
            self.info(f"📁 Location: {job_info['file_path']}")

            self._show_usage_tips(job_info)

        except Exception as e:
            raise Exception(f"Failed to write job file: {e}")

    def _generate_job_code(self, job_info: dict) -> str:
        """Generate the job class code."""
        class_name = job_info["class_name"]
        is_sync = job_info["is_sync"]

        # Load stub file
        stub_path = Path(paths("cara")) / "commands" / "stubs" / "Job.stub"

        with open(stub_path, "r", encoding="utf-8") as f:
            stub_content = f.read()

        # Replace placeholders
        code = stub_content.replace("{{ class_name }}", class_name)
        code = code.replace(
            "{{ docstring }}",
            f"{class_name}\n\nGenerated by Cara framework make:job command.",
        )

        # Modify for sync jobs if needed
        if is_sync:
            code = code.replace("Trackable, ShouldQueue, Queueable", "Queueable")
            code = code.replace("ShouldQueue, Queueable", "Queueable")
            code = code.replace(
                "from cara.queues.contracts import Queueable, ShouldQueue\nfrom cara.queues.tracking import Trackable",
                "from cara.queues.contracts import Queueable",
            )
            code = code.replace("async def handle", "def handle")
            code = code.replace(
                "    async def handle(self):",
                "    def handle(self):",
            )

        return code

    def _show_usage_tips(self, job_info: dict) -> None:
        """Show usage examples for the generated job."""
        class_name = job_info["class_name"]

        self.info("\n💡 Usage Tips:")
        self.info(f"   Import: from app.jobs import {class_name}")

        if not job_info["is_sync"]:
            self.info("   Context-aware dispatch (recommended):")
            self.info("     from cara.queues import Bus")
            self.info(
                f"     await Bus.dispatch({class_name}(), routing_key='processing.high')"
            )
            self.info("")
            self.info("   Traditional queue dispatch:")
            self.info(f"     {class_name}.dispatch().withRoutingKey('processing.high')")
            self.info("")
            self.info("   Explicit sync (testing/debugging):")
            self.info("     from cara.context import ExecutionContext")
            self.info("     with ExecutionContext.sync():")
            self.info(f"         await Bus.dispatch({class_name}())")
            self.info("")
            self.info("📋 Features:")
            self.info("   ✅ Automatic job tracking (job + job_logs tables)")
            self.info("   ✅ Conflict resolution (prevents duplicate jobs)")
            self.info("   ✅ Smart retry with exponential backoff")
            self.info("   ✅ Performance analytics")
        else:
            self.info("   Execute:")
            self.info(f"     {class_name}().handle()")
