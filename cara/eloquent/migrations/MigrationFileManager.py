from __future__ import annotations

import glob
import hashlib
import importlib.util
import os
import sys

from cara.exceptions import ORMException


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
        # Local import to avoid a package-__init__ circular (this module is
        # imported by ``cara.eloquent.migrations.__init__`` itself).
        from cara.eloquent.migrations.Migration import Migration

        # ``exec_module`` does not register an ad-hoc module in
        # ``sys.modules`` for us.  Decorators such as ``@dataclass(slots=True)``
        # resolve the class' module while the file is executing and fail when
        # that entry is absent (Python 3.14 surfaces this as a
        # ``NoneType.__dict__`` error).  Give each migration path a stable,
        # collision-free module name and publish it before execution, matching
        # normal import semantics.  A path-derived name prevents unrelated
        # files from overwriting one global ``migration`` entry.
        absolute_path = os.path.realpath(file_path)
        module_hash = hashlib.sha256(absolute_path.encode()).hexdigest()
        module_name = f"_cara_migration_{module_hash}"
        spec = importlib.util.spec_from_file_location(module_name, absolute_path)
        if spec is None or spec.loader is None:
            raise ORMException(f"Unable to load migration module from {file_path}")
        module = importlib.util.module_from_spec(spec)
        had_previous_module = module_name in sys.modules
        previous_module = sys.modules.get(module_name)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)

            # Find the Migration subclass defined in this module. Identify it by
            # ``issubclass(Migration)`` — NOT by duck-typing ``hasattr(attr,
            # "up")``. Migration files do ``from cara.facades import DB`` (Log,
            # etc.), and a facade is a *class* whose metaclass ``__getattr__``
            # resolves attributes through the container, so ``hasattr(DB, "up")``
            # round-trips to ``DatabaseManager`` (which has no ``up``) and logs a
            # spurious "Facade resolution failed for 'DB'" ERROR — 90+ per migrate
            # run, drowning real errors. ``issubclass`` only walks the MRO and
            # never touches the facade's attributes.
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, Migration)
                    and attr is not Migration
                ):
                    return attr

            raise ORMException(f"No migration class found in {file_path}")
        except BaseException:
            # Match import machinery: a successful module stays available for
            # runtime annotation/pickling lookups, while every failed load
            # restores the exact cache state that existed before execution.
            if had_previous_module:
                sys.modules[module_name] = previous_module
            else:
                sys.modules.pop(module_name, None)
            raise

    def get_migration_name_from_file(self, file_path):
        """Extract migration name from file path"""
        filename = os.path.basename(file_path)
        return filename.replace(".py", "")

    def checksum(self, file_path) -> str:
        """SHA-256 of the exact migration source applied to the database."""
        digest = hashlib.sha256()
        with open(file_path, "rb") as handle:
            for chunk in iter(lambda: handle.read(128 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def create_migration_file(self, name, content):
        """Create new migration file"""
        if not os.path.exists(self.migration_directory):
            os.makedirs(self.migration_directory)

        file_path = os.path.join(self.migration_directory, f"{name}.py")
        with open(file_path, "w") as f:
            f.write(content)

        return file_path
