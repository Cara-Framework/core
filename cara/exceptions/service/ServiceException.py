"""Root of the service-layer domain exception family."""

from __future__ import annotations

from cara.exceptions.types.AppException import AppException


class ServiceException(AppException):
    """Base for every domain / service-layer exception a product raises.

    Services raise these instead of touching HTTP: each subclass declares a
    ``status_code`` and a ``to_dict`` envelope with a stable ``type`` token,
    and ``DefaultExceptionHandler`` renders it. The base itself carries the
    500 fallback and no ``to_dict`` on purpose (``validate_exception_envelopes``
    exempts it), so an untyped raise lands on the handler's redacted generic
    path.

    ``do_not_retry`` is what ``JobProcessor`` reads off a raised exception:
    the 4xx subclasses set it, because a refusal the input causes is repeated
    identically by every retry and belongs in the dead-letter queue at once.

    Products import this family through their own ``app.exceptions`` barrel
    and subclass it there for product-specific types.
    """

    status_code: int = 500
    do_not_retry: bool = False

    def __init__(self, message: str = "Service error", **extra) -> None:
        super().__init__(message)
        self.message = message
        self.extra = extra


__all__ = ["ServiceException"]
