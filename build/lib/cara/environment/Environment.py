"""
Environment Configuration System for the Cara framework.

This module provides a robust environment management system that handles loading and accessing
environment variables from various sources. It supports multiple environment files, environment-
specific overrides, and type casting of environment values.

The environment system follows the 12-factor app methodology for configuration management through
environment variables.
"""

import os
from pathlib import Path


class LoadEnvironment:
    """This class is used for loading the environment from .env and .env.* files."""

    # Track loaded environment files
    loaded_files = []

    def __init__(self, environment=None, override=True, only=None):
        """
        LoadEnvironment constructor.

        Keyword Arguments:
            env {string} -- An additional environment file that you want to load. (default: {None})
            override {bool} -- Whether or not the environment variables found should overwrite existing ones. (default: {False})
            only {string} -- If this is set then it will only load that environment. (default: {None})
        """
        from dotenv import load_dotenv

        self.env = load_dotenv

        if only:
            self._load_environment(only, override=override)
            return

        env_path = str(Path(".") / ".env")
        if Path(env_path).exists():
            self.env(env_path, override=override)
            LoadEnvironment.loaded_files.append(env_path)

        if os.environ.get("APP_ENV"):
            self._load_environment(os.environ.get("APP_ENV"), override=override)
        if environment:
            self._load_environment(environment, override=override)

        if "PYTEST_CURRENT_TEST" in os.environ:
            self._load_environment("testing", override=override)

    def _load_environment(self, environment, override=False):
        """
        Load the environment depending on the env file.

        Arguments:
            environment {string} -- Name of the environment file to load from

        Keyword Arguments:
            override {bool} -- Whether the environment file should overwrite existing environment keys. (default: {False})
        """
        env_path = str(Path(".") / ".env.{}".format(environment))
        if Path(env_path).exists():
            self.env(dotenv_path=env_path, override=override)
            LoadEnvironment.loaded_files.append(env_path)


def env(value, default="", cast=True):
    env_var = os.getenv(value, None)
    if env_var is None:
        env_var = default

    if not cast:
        return env_var

    # If not a str, return as-is (int/bool/etc)
    if not isinstance(env_var, str):
        return env_var

    # Now env_var is str
    if env_var == "":
        return default

    if env_var.isnumeric():
        return int(env_var)
    if env_var in ("false", "False"):
        return False
    if env_var in ("true", "True"):
        return True
    return env_var
