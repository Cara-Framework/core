"""Cache-header helpers — the exact ``Cache-Control``/``Vary`` contract.

These helpers are the framework's whole opt-in HTTP-caching surface, so the
tests pin the emitted header STRINGS, not just "some header was set":

* ``apply_private_cache`` must key the cache on ``Authorization`` — losing
  that ``Vary`` member silently lets a shared proxy serve one user's payload
  to another (the worst possible cache bug, invisible in dev where no shared
  cache sits in front).
* ``apply_public_swr_cache`` must emit ``s-maxage`` (CDN tier) alongside
  ``max-age`` (browser tier); when the caller doesn't split them the CDN
  value follows ``max_age``.
* ``apply_no_cache`` must emit ``no-store`` — ``no-cache`` alone would still
  let intermediaries retain a revalidatable copy.
"""

from __future__ import annotations

from cara.http import (
    apply_no_cache,
    apply_private_cache,
    apply_public_swr_cache,
)


class FakeResponse:
    """Just the get/set ``header`` duality the helpers rely on."""

    def __init__(self):
        self._headers: dict = {}

    def header(self, name, value=None):
        if value is None:
            return self._headers.get(name)
        self._headers[name] = value
        return self


class TestApplyPrivateCache:
    def test_sets_private_max_age_and_auth_keyed_vary(self):
        response = FakeResponse()

        result = apply_private_cache(response, 300)

        assert result is response  # chainable, mutates in place
        assert response.header("Cache-Control") == "private, max-age=300"
        assert response.header("Vary") == "Authorization, Accept-Encoding"

    def test_max_age_is_coerced_to_int(self):
        # Callers pass config-sourced values; a float/str TTL must not
        # leak a non-integer token into the header grammar.
        response = FakeResponse()

        apply_private_cache(response, 60.9)

        assert response.header("Cache-Control") == "private, max-age=60"


class TestApplyPublicSwrCache:
    def test_s_maxage_defaults_to_max_age(self):
        response = FakeResponse()

        result = apply_public_swr_cache(response, 60, 300)

        assert result is response
        assert response.header("Cache-Control") == (
            "public, max-age=60, s-maxage=60, stale-while-revalidate=300"
        )
        assert response.header("Vary") == "Accept-Encoding"

    def test_explicit_s_max_age_splits_browser_and_cdn_ttls(self):
        response = FakeResponse()

        apply_public_swr_cache(response, 60, 300, s_max_age=600)

        assert response.header("Cache-Control") == (
            "public, max-age=60, s-maxage=600, stale-while-revalidate=300"
        )

    def test_zero_s_max_age_is_respected_not_defaulted(self):
        # ``s_max_age=0`` ("browser may cache, CDN must not") is a valid
        # split — an ``or``-style default would wrongly promote it to 60.
        response = FakeResponse()

        apply_public_swr_cache(response, 60, 300, s_max_age=0)

        assert response.header("Cache-Control") == (
            "public, max-age=60, s-maxage=0, stale-while-revalidate=300"
        )


class TestApplyNoCache:
    def test_emits_no_store(self):
        response = FakeResponse()

        result = apply_no_cache(response)

        assert result is response
        assert response.header("Cache-Control") == "no-store, must-revalidate"

    def test_does_not_touch_vary(self):
        # no-store responses are never cached, so keying them is
        # meaningless — the helper must not clobber a Vary the endpoint
        # set for content negotiation.
        response = FakeResponse()

        apply_no_cache(response)

        assert response.header("Vary") is None
