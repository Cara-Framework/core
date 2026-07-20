"""MakeDataMigrationCommand: scaffold a plain hand-written migration.

The model-first ``make:migration`` only authors SCHEMA migrations from a model's
``fields``. One-time data BACKFILLS (``UPDATE``/``INSERT`` to populate a column,
seed a row, rewrite a value) have no model to drive them — so they are
hand-written, exactly the way Laravel does it: a PLAIN ``Migration`` with
``up()``/``down()`` that calls the query builder (``DB.table(...).update(...)``)
or a raw ``DB.statement(...)``. There is NO special "data migration" class — a
data migration is just a migration whose body mutates rows instead of schema.

This command scaffolds that blank ``Migration`` (Laravel's
``php artisan make:migration <name>`` equivalent), named with the SAME
sequential counter the schema generator uses so it sorts — and therefore runs —
AFTER every existing migration.
"""

from __future__ import annotations

import re

from cara.commands import CommandBase, missing_optional
from cara.decorators import command


@command(
    name="make:data-migration",
    help=(
        "Scaffold a blank hand-written migration (e.g. a data backfill) — a "
        "plain Migration with up()/down() you fill with DB.table()/DB.statement()."
    ),
)
class MakeDataMigrationCommand(CommandBase):
    def __init__(self, application):
        super().__init__(application)
        # Lazy DB import (optional 'db' extra: eloquent → psycopg2/faker),
        # mirroring MakeMigrationCommand — only when the command is invoked.
        try:
            from cara.eloquent.migrations.MigrationGenerator import (
                MigrationGenerator,
            )
        except ImportError as exc:
            raise missing_optional("db", exc) from exc
        self.generator = MigrationGenerator()

    def handle(self, name: str):
        """Scaffold ``NNNN_01_01_NNNNNN_<name>.py`` with a plain Migration."""
        if not name:
            self.error("Migration name is required (e.g. backfill_product_slugs)")
            return

        slug = self._slugify(name)
        if not slug:
            self.error(f"'{name}' is not a valid migration name")
            return

        class_name = "".join(part.capitalize() for part in slug.split("_"))
        content = self._scaffold(class_name)

        # Reuse the schema generator's counter + filename machinery so this file
        # shares the SINGLE monotonic sequence: the counter is one higher than
        # every existing migration, so the lexicographic full-filename sort
        # (which the executor/tracker order by) places it LAST → it runs after
        # the schema migrations it backfills. The name has no ``_table`` suffix,
        # so the schema-migration --overwrite globs never match it — a
        # regenerate can't clobber a hand-written backfill.
        filepath = self.generator.create_migration_file(slug, content)
        self.success(f"Created migration: \n{filepath}")
        self.info(
            "Write your data UPDATE/INSERT in up() (and its reverse in down()), "
            "then run: python craft migrate"
        )

    def _slugify(self, name: str) -> str:
        """Lower snake_case slug from a free-form name (strips ``.py``, splits
        spaces / underscores / camelCase humps, keeps ``[a-z0-9_]``)."""
        if name.endswith(".py"):
            name = name[:-3]
        name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
        name = name.replace(" ", "_").replace("-", "_").lower()
        name = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
        return re.sub(r"_+", "_", name)

    def _scaffold(self, class_name: str) -> str:
        """Render a plain Migration stub — Laravel-identical: up()/down() with a
        query-builder + raw-statement example for the data change."""
        return f'''"""{class_name} migration."""

from cara.eloquent.migrations import Migration
from cara.facades import DB

# No model owns a data change, so ``make:migration --overwrite`` (which rebuilds
# the directory as exactly one generated file per table) would delete this file.
# The marker exempts it. Remove it only if this migration becomes disposable.
MODEL_LESS = True


class {class_name}(Migration):
    def up(self):
        # Mutate rows here — the query builder (Laravel DB::table()->update()):
        # DB.table("product").where_null("slug").update({{"slug": "..."}})
        #
        # ...or a raw statement for anything the builder can't express:
        # DB.statement("UPDATE product SET slug = lower(name) WHERE slug IS NULL")
        pass

    def down(self):
        # Reverse the change where reversible (or document why it isn't):
        # DB.table("product").update({{"slug": None}})
        pass
'''
