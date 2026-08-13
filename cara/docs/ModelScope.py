"""ModelScope."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelScope:
    """How a product marks a model as scoped to one partition of its data.

    The engine only ever asks "is ``base`` among this class's bases?" — the
    partition itself (a tenant, an account, a region, a shop) is product
    vocabulary, so the words printed on the page travel WITH the class name
    rather than being hard-coded next to the AST walk. All three fields are
    required: a base with no label produces an unnamed column, and a label
    with no explanation produces a tick mark nobody can act on.

    * ``base`` — the mixin class name that appears in a scoped model's bases.
    * ``label`` — the column header; its lower-cased form also reads as the
      adjective in the summary line ("N of M models are tenant-scoped").
    * ``note`` — one sentence saying what the scope actually enforces.
    """

    base: str
    label: str
    note: str
