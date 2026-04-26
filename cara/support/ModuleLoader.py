"""Module loading utilities."""

import importlib

from cara.exceptions import LoaderNotFoundException
from cara.support.Str import modularize


def load(
    path,
    object_name=None,
    default=None,
    raise_exception=False,
):
    """
    Load the given object from a Python module located at path and returns a default value if not
    found. If no object name is provided, loads the module.

    Arguments:
        path {str} -- A file path or a dotted path of a Python module
        object {str} -- The object name to load in this module (None)
        default {str} -- The default value to return if object not found in module (None)
    Returns:
        {object} -- The value (or default) read in the module or the module if no object name
    """
    # modularize path if needed
    module_path = modularize(path)
    try:
        module = importlib.import_module(module_path)
    except Exception as e:
        error_message = (
            f"'{module_path}' not found OR error when importing this module: {str(e)}"
        )
        try:
            from cara.facades import Log

            Log.warning(error_message, category="cara.support.module_loader")
        except Exception:
            pass

        if raise_exception:
            raise LoaderNotFoundException(error_message) from e
        return None

    if object_name is None:
        return module

    # getattr raises AttributeError when the attribute is missing — not
    # KeyError. The previous code swallowed the wrong exception and then
    # crashed on a legitimately-missing attribute.
    try:
        return getattr(module, object_name)
    except AttributeError:
        if raise_exception:
            raise LoaderNotFoundException(f"{object_name} not found in {module_path}")
        return default
