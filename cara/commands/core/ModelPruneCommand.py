"""``model:prune`` — prune rows from every MakesPrunable model (Laravel parity).

Discovers application models that mix in
:class:`cara.eloquent.concerns.MakesPrunable` and runs each one's batched
:meth:`prune`. A model becomes prunable by mixing in ``MakesPrunable`` and
overriding ``prunable()`` to return the query selecting expired rows::

    class OutboundClick(Model, MakesPrunable):
        def prunable(self):
            cutoff = pendulum.now("UTC").subtract(days=90).to_datetime_string()
            return self.query().where("created_at", "<", cutoff)

Usage::

    craft model:prune                       # prune every discovered model
    craft model:prune --model=OutboundClick # prune a single model by class name
    craft model:prune --batch=500           # override the per-batch size
    craft model:prune --pretend             # report counts without deleting

This is the reusable framework primitive; hand-written prune jobs in the
app stay as they are — a model can adopt ``MakesPrunable`` incrementally.
"""

from __future__ import annotations

from cara.commands import CommandBase, missing_optional
from cara.decorators import command


@command(
    name="model:prune",
    help="Prune expired rows from MakesPrunable models.",
    options={
        "--model=?": "Prune only this model (class name, e.g. OutboundClick)",
        "--batch=1000": "Rows deleted per batch",
        "--pretend": "Report how many rows WOULD be pruned without deleting",
    },
)
class ModelPruneCommand(CommandBase):
    """Prune expired rows from every (or one) MakesPrunable model."""

    def handle(self):
        # Lazy import: the eloquent layer pulls the optional 'db' extra
        # (psycopg2). Defer it so a DB-less surface can still import this
        # command module; fail LOUD here, not at module load.
        try:
            from cara.eloquent.concerns import MakesPrunable
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        self._MakesPrunable = MakesPrunable

        models = self._discover_prunable_models()
        if not models:
            self.warning("No MakesPrunable models found to prune.")
            return

        pretend = bool(self.option("pretend"))
        batch_size = self._batch_size()

        if pretend:
            self.info("Pretend mode — no rows will be deleted.")

        rows = []
        grand_total = 0
        for model_cls in models:
            count = self._prune_one(model_cls, batch_size, pretend)
            if count is None:
                rows.append((model_cls.__name__, "ERROR"))
                continue
            grand_total += count
            rows.append((model_cls.__name__, str(count)))

        verb = "would prune" if pretend else "pruned"
        self.table(["Model", f"Rows {verb}"], rows)
        self.success(f"Done — {verb} {grand_total} row(s) across {len(models)} model(s).")

    # ── discovery ───────────────────────────────────────────────────────

    def _discover_prunable_models(self) -> list[type]:
        """Resolve the set of model classes to prune.

        With ``--model=Name`` the discovery is scoped to that single class
        (matched by class name). Otherwise every model under the app's
        ``models`` module that mixes in ``MakesPrunable`` is returned, sorted
        by name for deterministic output.
        """
        requested = self.option("model")
        discovered = self._load_models()

        prunable = [cls for cls in discovered if self._is_prunable(cls)]

        if requested:
            match = [cls for cls in prunable if cls.__name__ == requested]
            if not match:
                # Surface whether the model exists but isn't prunable vs.
                # doesn't exist at all — the two need different fixes.
                exists = any(cls.__name__ == requested for cls in discovered)
                if exists:
                    self.error(
                        f"Model '{requested}' does not mix in MakesPrunable "
                        "(or does not override prunable())."
                    )
                else:
                    self.error(f"Model '{requested}' was not found.")
                return []
            return match

        return sorted(set(prunable), key=lambda c: c.__name__)

    def _load_models(self) -> list[type]:
        """Import the app's model classes via the framework module helper.

        Falls back to walking ``Model.__subclasses__()`` so already-imported
        models (e.g. from ``commons.models``) are still considered even if
        the app's ``models`` package layout differs.
        """
        from cara.support import get_classes

        classes: list[type] = []
        try:
            classes.extend(get_classes("models", self._MakesPrunable))
        except Exception as exc:  # noqa: BLE001 — discovery must not hard-crash the command
            self.debug(f"models module discovery skipped: {exc}")

        # Defence in depth: include any MakesPrunable Model subclass already
        # loaded into the interpreter. ``get_classes`` only sees what the
        # ``models`` package re-exports; subclasses live wherever they're
        # imported from.
        try:
            from cara.eloquent.models import Model

            classes.extend(
                cls for cls in self._all_subclasses(Model) if self._is_prunable(cls)
            )
        except Exception as exc:  # noqa: BLE001
            self.debug(f"subclass discovery skipped: {exc}")

        # De-dupe while preserving classes (a class may be reachable both ways).
        seen: set[type] = set()
        unique: list[type] = []
        for cls in classes:
            if cls not in seen:
                seen.add(cls)
                unique.append(cls)
        return unique

    @staticmethod
    def _all_subclasses(base: type) -> set[type]:
        """Recursively collect every subclass of ``base``."""
        found: set[type] = set()
        stack = list(base.__subclasses__())
        while stack:
            cls = stack.pop()
            if cls in found:
                continue
            found.add(cls)
            stack.extend(cls.__subclasses__())
        return found

    def _is_prunable(self, cls: type) -> bool:
        """A class is prunable if it mixes in MakesPrunable and overrides prunable()."""
        if not (isinstance(cls, type) and issubclass(cls, self._MakesPrunable)):
            return False
        if cls is self._MakesPrunable:
            return False
        # Must actually override prunable() — the base raises NotImplementedError.
        return cls.prunable is not self._MakesPrunable.prunable

    # ── execution ─────────────────────────────────────────────────────────

    def _batch_size(self) -> int:
        raw = self.option("batch", 1000)
        try:
            size = int(raw)
        except TypeError, ValueError:
            self.warning(f"Invalid --batch={raw!r}; falling back to 1000.")
            return 1000
        return size if size >= 1 else 1000

    def _prune_one(self, model_cls: type, batch_size: int, pretend: bool) -> int | None:
        """Prune (or count) one model. Returns the row count, or None on error."""
        try:
            instance = model_cls()
            if pretend:
                # Count the prunable set without deleting.
                return int(instance.prunable().count())
            return int(instance.prune(batch_size=batch_size))
        except Exception as exc:  # noqa: BLE001 — one bad model must not abort the rest
            self.error(f"× {model_cls.__name__}: prune failed: {exc}")
            return None
