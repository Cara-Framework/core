"""DocsCommand: the one product fact a documentation command needs, bound late.

Both documentation commands need exactly one thing from the product — its
:class:`~cara.docs.Manifest.DocsManifest` — and this is the single place that
resolves it.

WHY THE RESOLUTION IS LAZY. A product binds its manifest in a file that calls
:meth:`~cara.docs.Manifest.DocsManifest.discover_root`, and that call REQUIRES a
documentation tree: it raises when no ancestor holds ``docs/index.html``. That
strictness is correct — a docs run against the wrong root is worse than no run
— but it makes the manifest a runtime fact, not an import-time one.

The production image is exactly the environment that has no documentation tree.
Each deployable is built from its own directory as the build context, so the
workspace-level ``docs/`` never enters the image, while the command provider
still imports these commands at boot to register them. Resolving the manifest in
a class body therefore turns "this image carries no documentation" into
"this image cannot boot" — a container that dies on import while every
filesystem guard in development stays green, because in development the
documentation tree is always there.

So the binding is a PATH at import time and a manifest at call time:
``manifest_path`` is read only when a handler actually runs, which is only ever
in a checkout that has the documentation it is being asked to generate. Products
bind ``manifest_path``; in-process callers that already hold a manifest (the
framework's own tests) bind ``manifest`` directly. Neither form touches the
filesystem while the module is being imported.
"""

from __future__ import annotations

from pathlib import Path

from cara.commands.CommandBase import CommandBase
from cara.docs.Manifest import DocsManifest


class DocsCommand(CommandBase):
    """Base for documentation commands: carries the manifest binding only.

    Subclasses bind ONE of:

    * ``manifest_path`` — a path to the product's ``docs_manifest.py``, read on
      first use. This is what products bind; a ``Path`` literal costs nothing to
      import and cannot fail in an image without a documentation tree.
    * ``manifest`` — an already-constructed manifest, for callers that build one
      in process rather than reading it from a module.
    """

    manifest: DocsManifest | None = None
    manifest_path: Path | None = None

    def _manifest(self) -> DocsManifest:
        """Resolve the bound manifest, reading the manifest module on first use."""
        manifest = getattr(self, "manifest", None)
        if isinstance(manifest, DocsManifest):
            return manifest

        path = getattr(self, "manifest_path", None)
        if path is not None:
            # Cached on the INSTANCE, never the class: a class-level cache
            # would make one run's product the default for every later run in
            # the same process, which is precisely the cross-product bleed the
            # manifest exists to prevent.
            loaded = DocsManifest.load(Path(path))
            self.manifest = loaded
            return loaded

        raise TypeError(
            f"{type(self).__name__} must bind `manifest_path` (or `manifest`) to "
            "reach a DocsManifest; the framework command carries no product "
            "configuration of its own"
        )


__all__ = ["DocsCommand"]
