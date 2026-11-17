"""A product's invariant catalog: the one list of rules its auditor checks."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from .Invariant import Invariant


class InvariantCatalog:
    """An ordered, id-unique tuple of :class:`Invariant` and its cadences.

    The schedule, the metric label values and the guard tests all read the
    catalog; nothing restates it.
    """

    def __init__(self, cadences: Iterable[str], invariants: Iterable[Invariant]) -> None:
        self.cadences: tuple[str, ...] = tuple(cadences)
        self.invariants: tuple[Invariant, ...] = tuple(invariants)
        seen: set[str] = set()
        for entry in self.invariants:
            if entry.id in seen:
                raise ValueError(f"duplicate invariant id {entry.id!r}")
            seen.add(entry.id)
            if entry.cadence not in self.cadences:
                raise ValueError(
                    f"invariant {entry.id!r}: cadence {entry.cadence!r} is not one "
                    f"of {list(self.cadences)}"
                )

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(entry.id for entry in self.invariants)

    def __iter__(self) -> Iterator[Invariant]:
        return iter(self.invariants)

    def __len__(self) -> int:
        return len(self.invariants)

    def get(self, invariant_id: str) -> Invariant:
        """The entry ``invariant_id``; ``KeyError`` when unknown."""
        for entry in self.invariants:
            if entry.id == invariant_id:
                return entry
        raise KeyError(f"unknown invariant {invariant_id!r} — valid: {list(self.ids)}")

    def for_cadence(self, cadence: str) -> tuple[Invariant, ...]:
        """The invariants one cadence pass checks."""
        if cadence not in self.cadences:
            raise KeyError(f"unknown cadence {cadence!r} — valid: {list(self.cadences)}")
        return tuple(entry for entry in self.invariants if entry.cadence == cadence)


__all__ = ["InvariantCatalog"]
