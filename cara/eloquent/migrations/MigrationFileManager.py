import glob
import importlib.util
import os


class MigrationFileManager:
    """Single Responsibility: Handles migration file operations"""

    def __init__(self, migration_directory):
        self.migration_directory = migration_directory

    def get_migration_files(self):
        """Get all migration files from directory"""
        if not os.path.exists(self.migration_directory):
            return []

        pattern = os.path.join(self.migration_directory, "*.py")
        files = glob.glob(pattern)
        return [f for f in files if not os.path.basename(f).startswith("__")]

    def load_migration_class(self, file_path):
        """Load migration class from file"""
        spec = importlib.util.spec_from_file_location("migration", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Find migration class in module
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and hasattr(attr, "up")
                and hasattr(attr, "down")
                and attr_name != "Migration"
            ):
                return attr

        raise ValueError(f"No migration class found in {file_path}")

    def get_migration_name_from_file(self, file_path):
        """Extract migration name from file path"""
        filename = os.path.basename(file_path)
        return filename.replace(".py", "")

    def create_migration_file(self, name, content):
        """Create new migration file"""
        if not os.path.exists(self.migration_directory):
            os.makedirs(self.migration_directory)

        file_path = os.path.join(self.migration_directory, f"{name}.py")
        with open(file_path, "w") as f:
            f.write(content)

        return file_path
