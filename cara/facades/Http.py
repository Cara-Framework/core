from __future__ import annotations


class _HttpFacadeMeta(type):
    """Resolve the HTTP client only when a facade operation is used.

    The facade barrel is part of early bootstrap; importing the HTTP layer
    while that barrel is still binding re-enters ``cara.facades`` through
    request validation and leaves both packages partially initialized.
    """

    def __getattr__(cls, name: str):
        from cara.http.client.HttpFacade import (  # local: cycle with cara.http.client.HttpFacade
            HttpFacade,
        )

        return getattr(HttpFacade, name)


class Http(metaclass=_HttpFacadeMeta):
    """Static HTTP-client facade."""


__all__ = ["Http"]
