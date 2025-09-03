"""
Loader Module for Dynamic Class and Object Loading.

This module provides a powerful class loading system for the Cara framework, implementing dynamic
class and object loading with support for module discovery, class filtering, and parameter loading.
"""

import inspect
import os
import pkgutil
from typing import Dict

from cara.exceptions import LoaderNotFoundException
from cara.support.ModuleLoader import load
from cara.support.Str import as_filepath


def parameters_filter(obj_name, obj):
    """
    Filter for parameter objects in modules.

    Args:
        obj_name: Name of the object
        obj: The object itself

    Returns:
        bool: True if object is a parameter
    """
    return (
        obj_name.isupper()
        and not obj_name.startswith("__")
        and not obj_name.endswith("__")
    )


class Loader:
    """
    Dynamic class and object loader.

    This class provides methods for dynamically loading classes and objects from modules and
    directories, with support for filtering and error handling.
    """

    def get_modules(self, files_or_directories, raise_exception=False):
        """
        Load modules from a list of directories or a single directory.

        Args:
            files_or_directories: Path or list of paths to search
            raise_exception: Whether to raise if a module cannot be loaded

        Returns:
            Dict[str, module]: Mapping of module name → module object
        """
        if not isinstance(files_or_directories, list):
            files_or_directories = [files_or_directories]

        modules = {}
        # Don't use as_filepath for filesystem paths, only for dotted paths
        module_paths = []
        for path in files_or_directories:
            # If path contains dots but is not a dotted module path (like receet.io),
            # treat it as filesystem path
            if "." in path and not path.startswith(".") and "/" in path:
                module_paths.append(path)
            else:
                module_paths.append(as_filepath(path))

        for module_loader, name, _ in pkgutil.iter_modules(module_paths):
            module = load(
                f"{os.path.relpath(module_loader.path)}.{name}",
                raise_exception=raise_exception,
            )
            modules[name] = module
        return modules

    def find(
        self,
        class_instance,
        paths,
        class_name,
        raise_exception=False,
    ):
        """
        Find a single class by name under one or more paths.

        Args:
            class_instance: Base class or interface that the target should subclass
            paths: List of dotted or filesystem paths to search
            class_name: Exact class name to find
            raise_exception: Whether to raise if not found

        Returns:
            The class object if found, otherwise None
        """
        classes = self.find_all(class_instance, paths, raise_exception)
        for name, obj in classes.items():
            if name == class_name:
                return obj
        if raise_exception:
            raise LoaderNotFoundException(
                f"No {class_instance} named {class_name} has been found in {paths}"
            )
        return None

    def find_all(self, class_instance, paths, raise_exception=False):
        """
        Find all classes that subclass `class_instance` under the given paths.

        Args:
            class_instance: Base class or interface
            paths: List of dotted or filesystem paths to search
            raise_exception: Whether to raise if none found

        Returns:
            Dict[str, class]: Mapping of class name → class object
        """
        classes: Dict[str, type] = {}
        for module in self.get_modules(paths, raise_exception).values():
            for obj_name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, class_instance):
                    if obj.__module__.startswith(module.__package__):
                        classes[obj_name] = obj
        if not classes and raise_exception:
            raise LoaderNotFoundException(
                f"No {class_instance} have been found in {paths}"
            )
        return classes

    def get_object(
        self,
        path_or_module,
        object_name,
        raise_exception=False,
    ):
        """
        Get a single object (class/function/variable) from a module.

        Args:
            path_or_module: Dotted path or module object
            object_name: Name of the object to retrieve
            raise_exception: Whether to raise if not found

        Returns:
            The object if found, otherwise None
        """
        return load(
            path_or_module,
            object_name,
            raise_exception=raise_exception,
        )

    def get_objects(
        self,
        path_or_module,
        filter_method=None,
        raise_exception=False,
    ):
        """
        Returns a dictionary of objects from the given path (file or dotted).

        The dictionary can be filtered if a callable is provided.

        Args:
            path_or_module: Dotted path or module object
            filter_method: Predicate to select objects
            raise_exception: Whether to raise if module not found

        Returns:
            Dict[str, object]: Mapping of object name → object
        """
        if isinstance(path_or_module, str):
            module = load(
                path_or_module,
                raise_exception=raise_exception,
            )
        else:
            module = path_or_module

        if not module:
            return {}

        return dict(inspect.getmembers(module, filter_method))

    def get_parameters(self, module_or_path):
        """
        Get all uppercase “parameters” from a module.

        Args:
            module_or_path: Dotted path or module object

        Returns:
            Dict[str, Any]: Mapping of parameter name → value
        """
        parameters = {}
        for name, obj in self.get_objects(module_or_path).items():
            if parameters_filter(name, obj):
                parameters[name] = obj
        return parameters
