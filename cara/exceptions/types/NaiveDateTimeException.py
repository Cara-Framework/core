"""NaiveDateTimeException."""

from __future__ import annotations

from .ModelException import ModelException


class NaiveDateTimeException(ModelException):
    """A write handed the ORM a date-time that carries no zone.

    Storage and the wire are UTC and every write is an INSTANT (DOCTRINE §7).
    A naive ``datetime`` — or an offset-less string — names a wall clock in
    some zone the value does not state, so writing it would have to GUESS
    that zone. The ORM refuses instead of guessing: the writer passes an aware
    value (``pendulum.now("UTC")``, ``datetime.now(UTC)``, a parsed ISO-8601
    string with ``Z``/``+00:00``). A wall clock the product owns is converted
    with its zone named at the edge (``pendulum.datetime(..., tz=zone)``).

    ``attribute`` is ``"<Model>.<column>"`` once a model write door has named
    it; a bare cast call leaves it ``None``.
    """

    def __init__(self, value: object, attribute: str | None = None):
        self.value = value
        self.attribute = attribute
        target = f" to {attribute}" if attribute else ""
        super().__init__(
            f"naive date-time {value!r} written{target}: a write must carry its "
            "zone (an aware UTC instant); the ORM never guesses one"
        )

    def on(self, attribute: str) -> NaiveDateTimeException:
        """The same refusal, naming the attribute the write was for."""
        return NaiveDateTimeException(self.value, attribute)
