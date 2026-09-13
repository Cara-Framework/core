"""Canonical definition of ``InvalidHeaderComponent``."""

from __future__ import annotations

from cara.exceptions import CaraException


class InvalidHeaderComponent(CaraException, RuntimeError):
    """A response-header declaration that cannot be published as written.

    Either a ``MiddlewareHeaders`` names a header it does not define, or two
    declarations guarding one operation define the same header differently — a
    document that kept either one would describe a header the other middleware
    does not send. In the taxonomy (§9) so ``except CaraException`` around spec
    generation catches it; ``RuntimeError`` stays as a SECOND base for the craft
    command that already treats a RuntimeError as "fail this build".
    """
