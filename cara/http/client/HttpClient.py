"""Cara HTTP Client — Laravel-style facade for external HTTP requests.

Provides a fluent interface for making HTTP requests with built-in retry,
exponential backoff, timeout management, and Retry-After header parsing.

Usage::

    from cara.facades import Http

    # Simple GET
    response = await Http.get("https://api.example.com/data")

    # With retry and timeout
    response = await Http.timeout(10).retry(3, backoff=2.0).get(url)

    # With headers
    response = await Http.with_headers({"Authorization": "Bearer ..."}).post(
        url, json=payload
    )

    # With base URL (for API clients)
    client = Http.base_url("https://api.example.com").with_headers({"X-API-Key": key})
    response = await client.get("/users")
"""

from __future__ import annotations


# ``Log`` is imported lazily inside the one method that uses it. A module-top
# ``from cara.facades import Log`` re-enters ``cara.facades`` while it is still
# initialising (this module is pulled in by ``cara/facades/Http.py`` during
# ``cara.facades.__init__``), leaving later facades resolving to their submodules
# instead of the facade classes and breaking early boot. Keep it local.
