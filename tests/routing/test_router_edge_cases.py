"""
Router edge-case pins covering the audit fixes:

  1. Route parameter binding: percent-decoded values reach the handler
     (no more raw ``caf%C3%A9`` leaking through).
  2. 405 Method Not Allowed: ``MethodNotAllowedException`` carries the
     allow-list AND the default exception handler emits the RFC 9110
     §15.5.6 ``Allow`` header.
  3. OPTIONS preflight: the advertised method list ALWAYS includes
     ``OPTIONS`` itself, even when no OPTIONS route was registered.
  4. Non-standard HTTP verbs: PROPFIND / MKCOL / LINK / etc. land in
     ``routes_by_method`` and are reachable via ``find`` (pre-fix they
     silently dropped, 404 vs the registered route).
  5. Route group prefix nesting: ``/api`` + ``/admin`` + ``/users``
     composes to ``/api/admin/users`` (no double-slash regression).
  6. Middleware order in nested groups: outer → inner → route.
"""

import re

import pytest

from cara.exceptions import (
    MethodNotAllowedException,
    RouteNotFoundException,
)
from cara.exceptions.handlers.DefaultExceptionHandler import (
    DefaultExceptionHandler,
)
from cara.routing.Route import Route
from cara.routing.RouteCompiler import RouteCompiler
from cara.routing.RouteGroup import RouteGroup
from cara.routing.Router import Router


# ── Helpers ──────────────────────────────────────────────────────────


def _controller(_request=None, _response=None):
    """Tiny callable stand-in for a route controller."""
    return {"ok": True}


def _make_router(*routes):
    # Positional ``application`` then variadic routes — keyword form
    # clashes with ``*routes`` ("multiple values for application").
    return Router(None, *routes)


# ── 1. Parameter binding: percent-decoding ──────────────────────────


class TestRouteParameterDecoding:
    """``RouteCompiler.extract_parameters`` must percent-decode bound
    values. Pre-fix the raw ``%C3%A9`` reached the handler; this pin
    catches a future regression where someone reverts the ``unquote``
    in ``extract_parameters``.
    """

    def _compile(self, url: str) -> RouteCompiler:
        # Mirror the default compiler set Route uses so the tests
        # exercise the same surface real routes do.
        return RouteCompiler(url, Route.compilers)

    def test_utf8_percent_encoded_slug_is_decoded(self):
        # ``café`` percent-encoded as UTF-8 = ``caf%C3%A9``. A
        # storefront request to /products/caf%C3%A9 used to leak
        # the raw value into a ``Product.where('slug', ...)`` lookup
        # and silently 404. Pinned: handler sees the decoded value.
        compiler = self._compile("/products/@slug")
        params = compiler.extract_parameters("/products/caf%C3%A9")
        assert params == {"slug": "café"}

    def test_already_decoded_path_is_idempotent(self):
        # Compliant ASGI servers (uvicorn, hypercorn) pre-decode
        # ``scope['path']`` — the decode pass here is a no-op for
        # already-decoded input. Pin that fact so a future "I think
        # this is double-decoding" revert proposal is rebuffed.
        compiler = self._compile("/products/@slug")
        params = compiler.extract_parameters("/products/café")
        assert params == {"slug": "café"}

    def test_turkish_characters_round_trip(self):
        # Cheapa.io is a Turkish-locale storefront; pin both raw
        # Turkish chars and the percent-encoded form. ``ç`` is %C3%A7.
        compiler = self._compile("/products/@slug")
        assert compiler.extract_parameters("/products/çay-bardağı") == {
            "slug": "çay-bardağı"
        }
        assert compiler.extract_parameters("/products/%C3%A7ay-barda%C4%9F%C4%B1") == {
            "slug": "çay-bardağı"
        }

    def test_space_encoded_as_percent20_decodes(self):
        # ``%20`` is the canonical space encoding. The default
        # ``[^/]+`` matches ``%20`` (no literal slash) and unquote
        # converts it back to a space.
        compiler = self._compile("/q/@term")
        assert compiler.extract_parameters("/q/hello%20world") == {
            "term": "hello world"
        }

    def test_plus_sign_is_preserved_as_literal(self):
        # ``+`` is space ONLY in application/x-www-form-urlencoded
        # bodies, NOT in URL paths (RFC 3986). ``unquote`` (not
        # ``unquote_plus``) preserves it. Pin so a future "let's
        # treat + as space everywhere" refactor doesn't slip in.
        compiler = self._compile("/q/@term")
        assert compiler.extract_parameters("/q/a+b") == {"term": "a+b"}

    def test_malformed_percent_triplet_falls_back_to_raw(self):
        # ``%ZZ`` isn't a valid percent triplet. ``urllib.parse.unquote``
        # leaves it alone (returns the literal ``%ZZ``) rather than
        # raising — pin that no crash reaches the router, the bound
        # value just stays as-is.
        compiler = self._compile("/products/@slug")
        params = compiler.extract_parameters("/products/bad%ZZthing")
        # Either fully preserved or partially decoded — both are
        # acceptable; what matters is no crash + a string value.
        assert isinstance(params["slug"], str)
        assert "thing" in params["slug"]

    def test_optional_param_absent_remains_none(self):
        # Pre-fix the "optional param missing" branch returned None,
        # which the decoder must preserve (not turn into "None"
        # string or empty). Pin that decoded-vs-absent stays
        # distinguishable.
        compiler = self._compile("/products/@slug?")
        params = compiler.extract_parameters("/products")
        assert params == {"slug": None}

    def test_encoded_slash_in_segment_is_decoded_after_match(self):
        # ``%2F`` (encoded slash) within a slug stays inside the
        # captured segment because the regex ``[^/]+`` only refuses
        # *literal* ``/``. After matching, unquote expands it — the
        # handler sees the literal slash inside the slug, which is
        # the documented behaviour. Pin so a refactor that adds a
        # post-decode "validate no slash" doesn't silently flip
        # the contract.
        compiler = self._compile("/q/@term")
        params = compiler.extract_parameters("/q/a%2Fb")
        assert params == {"term": "a/b"}


# ── 2. 405 Method Not Allowed: exception carries allow-list ─────────


class TestMethodNotAllowedAllowList:
    """``Router.find`` must raise ``MethodNotAllowedException`` with
    the ``allowed`` kwarg populated so the default exception handler
    can emit the RFC 9110 §15.5.6 ``Allow`` header.
    """

    def test_method_mismatch_raises_with_allowed_kwarg(self):
        # Register POST + DELETE for /x, send GET. The exception
        # MUST carry the allow-list as an attribute (not just in the
        # message string) so downstream handlers can read it
        # structurally.
        post = Route.post("/x", _controller)
        delete = Route.delete("/x", _controller)
        router = _make_router(post, delete)

        with pytest.raises(MethodNotAllowedException) as exc_info:
            router.find("/x", "GET")
        exc = exc_info.value
        assert hasattr(exc, "allowed"), (
            "MethodNotAllowedException must carry an 'allowed' attribute "
            "so the exception handler can emit the Allow header."
        )
        # Order-agnostic: just verify the set.
        assert set(exc.allowed) == {"POST", "DELETE"}

    def test_no_routes_at_all_raises_route_not_found_not_405(self):
        # When NO routes match the path (not even with another method),
        # the response is 404 — pre-fix and post-fix. Pin so a future
        # refactor that conflates the two doesn't accidentally start
        # returning 405 for unknown paths.
        router = _make_router(Route.get("/known", _controller))
        with pytest.raises(RouteNotFoundException):
            router.find("/unknown", "GET")

    def test_get_route_auto_routes_head_request(self):
        # ``Route.get`` registers ``["get", "head"]`` so HEAD
        # auto-routes to the GET handler. Pin so a refactor that
        # tries to separate them must opt in explicitly.
        route = Route.get("/x", _controller)
        router = _make_router(route)
        assert router.find("/x", "HEAD") is route


# ── 3. Default exception handler: emits Allow header ────────────────


class TestExceptionHandlerAllowHeader:
    """End-to-end: a 405 response from the default handler carries
    the ``Allow`` header populated from the exception's ``allowed``
    attribute, per RFC 9110 §15.5.6.
    """

    def test_format_response_propagates_allowed_into_data(self):
        # ``format_response`` is the same pipe that lifts
        # ``retry_after`` from the exception attribute onto the JSON
        # body so the header-emit stage can read it. Pin that
        # ``allowed`` follows the same pipe.
        handler = DefaultExceptionHandler()
        exc = MethodNotAllowedException(
            "Method GET not allowed", allowed=["POST", "DELETE"]
        )
        data = handler.format_response(exc, 405)
        assert data.get("allowed") == ["POST", "DELETE"]

    def test_allow_header_for_emits_uppercased_token_list(self):
        # The header value is "POST, DELETE" — comma-space separator
        # per RFC 9110 §5.6.1 (#-list rule). Pin both the bytes
        # casing of the name and the value formatting.
        handler = DefaultExceptionHandler()
        headers = handler._allow_header_for({"allowed": ["post", "delete"]})
        assert headers == [[b"allow", b"POST, DELETE"]]

    def test_allow_header_for_empty_or_missing_returns_no_header(self):
        # Non-405 responses (which never carry ``allowed``) must not
        # accidentally pick up an Allow header. Pin no-data → no
        # header, empty-list → no header.
        handler = DefaultExceptionHandler()
        assert handler._allow_header_for({}) == []
        assert handler._allow_header_for({"allowed": []}) == []
        assert handler._allow_header_for({"allowed": None}) == []

    def test_allow_header_for_filters_malformed_tokens(self):
        # Defense in depth: never let a non-token string land in the
        # Allow header (would break the HTTP line). Pin that
        # entries that don't match the HTTP-token grammar are
        # filtered, not allowed to corrupt the header.
        handler = DefaultExceptionHandler()
        # Mixed: "GET" valid, the rest are not.
        headers = handler._allow_header_for(
            {"allowed": ["GET", "bad header", "POST\nInjected: yes", ""]}
        )
        assert headers == [[b"allow", b"GET"]]

    def test_allow_header_for_handles_non_iterable_gracefully(self):
        # ``getattr(exception, 'allowed', None)`` could in theory
        # return a non-iterable if user code attached something
        # unexpected. Pin: header builder swallows the TypeError
        # and returns no header, doesn't crash the error path.
        handler = DefaultExceptionHandler()
        assert handler._allow_header_for({"allowed": 42}) == []

    def test_404_response_does_not_carry_allow_header(self):
        # Sanity: format_response on a RouteNotFoundException (no
        # ``allowed`` attr) must not invent a header. Pin: data has
        # no ``allowed`` key, header builder yields nothing.
        handler = DefaultExceptionHandler()
        exc = RouteNotFoundException("nope")
        data = handler.format_response(exc, 404)
        assert "allowed" not in data
        assert handler._allow_header_for(data) == []


# ── 4. OPTIONS preflight includes OPTIONS in advertised methods ─────


class TestOptionsPreflight:
    """The auto-synthesised preflight route's ``Allow`` /
    ``Access-Control-Allow-Methods`` headers MUST advertise OPTIONS
    itself, regardless of whether an OPTIONS route was registered.
    """

    def _exercise_preflight(self, router, path, allowed):
        # Synthesise the preflight route and invoke its underlying
        # handler directly. The Route wraps the controller through
        # RouteResolver (which expects a context tuple + a real
        # request), but the preflight controller is a tiny closure
        # we can pull straight off ``RouteResolver._route_handler``
        # to test in isolation.
        synth = router._create_preflight_route(path, allowed)
        handler = synth.controller._route_handler

        captured: dict = {}

        class _FakeResponse:
            def with_headers(self, headers):
                captured.update(headers)
                return self

            def status(self, code):
                captured["__status"] = code
                return self

        # The closure binding gives us a bound method on a transient
        # instance; safe to call directly with (request=None, response).
        handler(None, _FakeResponse())
        return captured

    def test_preflight_advertises_options(self):
        # Register only GET + POST. A preflight (OPTIONS) request
        # auto-synthesises a preflight controller that returns 204.
        # Pin the headers it sets include OPTIONS in the allow list.
        router = _make_router(
            Route.get("/x", _controller),
            Route.post("/x", _controller),
        )
        captured = self._exercise_preflight(router, "/x", ["GET", "POST"])

        allow_parts = captured.get("Allow", "").split(", ")
        cors_parts = captured.get("Access-Control-Allow-Methods", "").split(", ")
        assert "OPTIONS" in allow_parts
        assert "OPTIONS" in cors_parts
        assert "GET" in allow_parts
        assert "POST" in allow_parts
        # 204 No Content per CORS preflight convention.
        assert captured.get("__status") == 204

    def test_preflight_dedups_explicit_options_route(self):
        # If the user explicitly registered an OPTIONS route too,
        # the synthesiser's append-OPTIONS step must not double-list
        # it. ``dict.fromkeys``-based dedup preserves order.
        router = _make_router(Route.get("/x", _controller))
        captured = self._exercise_preflight(router, "/y", ["GET", "OPTIONS"])
        # Order preserved (GET first, OPTIONS once) — not "GET, OPTIONS, OPTIONS".
        assert captured["Allow"] == "GET, OPTIONS"


async def _maybe_await(value):
    """Await ``value`` if it's a coroutine; otherwise return it.

    ``Router._create_preflight_route``'s controller is a plain
    callable returning ``response`` (not async). ``Route.controller``
    is wrapped by ``RouteResolver`` whose ``handle`` returns the
    handler's return value — sometimes a coroutine, sometimes not.
    Keep the test compatible with both shapes.
    """
    import inspect

    if inspect.isawaitable(value):
        return await value
    return value


# ── 5. Non-standard HTTP verbs reach the lookup table ───────────────


class TestNonStandardHttpVerbs:
    """Routes registered with non-RFC-9110 verbs (PROPFIND, MKCOL,
    REPORT — WebDAV; LINK / UNLINK — RFC 5988; custom extension
    verbs) must be findable via ``Router.find``. Pre-fix the
    ``routes_by_method`` bucketing used an ``if key in self.
    routes_by_method`` allow-list against ``HTTP_METHODS``, so
    non-standard verbs landed in ``self.routes`` but never in any
    bucket and ``find`` silently returned 404.
    """

    def test_propfind_route_is_findable(self):
        # WebDAV's PROPFIND — used by collaborative-editing tooling
        # and some sync clients. If the framework can't route it, a
        # client that depends on it sees a 404 for a registered URL.
        route = Route("/dav/@path", _controller, ["propfind"])
        router = _make_router(route)
        assert router.find("/dav/file.txt", "PROPFIND") is route

    def test_custom_verb_added_after_construction_is_findable(self):
        # The router's ``.add`` path uses the same bucketing logic;
        # pin that a verb registered post-construction is also
        # reachable (separate code path).
        router = _make_router(Route.get("/known", _controller))
        custom = Route("/sync", _controller, ["sync"])
        router.add(custom)
        assert router.find("/sync", "SYNC") is custom

    def test_standard_verbs_still_routed_unchanged(self):
        # Regression guard: the bucketing change MUST NOT break the
        # standard-verb case. Pin GET still routes to its handler.
        route = Route.get("/x", _controller)
        router = _make_router(route)
        assert router.find("/x", "GET") is route


# ── 6. Route group prefix nesting ───────────────────────────────────


class TestRouteGroupPrefixNesting:
    """RouteGroup composition: nested prefixes merge into a single
    normalised path with no double slashes.
    """

    def test_two_level_group_composes_prefix(self):
        # /api → /admin → /users gives /api/admin/users.
        inner_route = Route.get("/users", _controller)
        inner_group = RouteGroup(prefix="/admin").routes([inner_route])
        outer_group = RouteGroup(prefix="/api").routes(list(inner_group))
        urls = [r.url for r in outer_group]
        assert urls == ["/api/admin/users"]

    def test_three_level_group_composes_prefix(self):
        # Stress: add a third level on top.
        inner_route = Route.get("/me", _controller)
        l1 = RouteGroup(prefix="/users").routes([inner_route])
        l2 = RouteGroup(prefix="/admin").routes(list(l1))
        l3 = RouteGroup(prefix="/api").routes(list(l2))
        urls = [r.url for r in l3]
        assert urls == ["/api/admin/users/me"]

    def test_trailing_and_leading_slashes_dont_double(self):
        # /api/ + admin/ + /foo must collapse to /api/admin/foo
        # (no /api//admin or /api/admin//foo).
        route = Route.get("/foo", _controller)
        g1 = RouteGroup(prefix="admin/").routes([route])
        g2 = RouteGroup(prefix="/api/").routes(list(g1))
        urls = [r.url for r in g2]
        assert urls == ["/api/admin/foo"]
        # Belt-and-braces: no consecutive slashes anywhere.
        assert not any("//" in u for u in urls)

    def test_empty_prefix_at_any_level_is_a_no_op(self):
        # An empty-prefix group must pass URLs through unchanged
        # rather than turning them into "/" or breaking the chain.
        route = Route.get("/foo", _controller)
        g_empty = RouteGroup(prefix="").routes([route])
        urls = [r.url for r in g_empty]
        assert urls == ["/foo"]


# ── 7. Middleware order in nested groups (outer → inner → route) ────


class TestMiddlewareOrderNested:
    """RouteGroup composition: outer middleware runs before inner
    runs before route-declared. Matches Laravel's semantics.
    """

    def test_route_group_prepends_middleware(self):
        # Route declares Throttle. Inner group adds AdminGate.
        # Outer group adds Auth. Final order: Auth → AdminGate →
        # Throttle. Each group's middleware lands BEFORE the
        # previously-accumulated chain (most recent = outermost).
        route = Route.get("/users", _controller).middleware("throttle")
        inner = RouteGroup(prefix="/admin", middleware=["admin_gate"]).routes(
            [route]
        )
        outer = RouteGroup(prefix="/api", middleware=["auth"]).routes(list(inner))
        # Single route in the group — pull its middleware list out.
        result = list(outer)[0].get_middleware()
        assert result == ["auth", "admin_gate", "throttle"]

    def test_middleware_chain_is_preserved_across_passes(self):
        # Pin that running through TWO group passes doesn't drop
        # the chain or duplicate it — verifies the additive
        # ``list(self._middleware) + existing`` semantics in
        # ``RouteGroup.routes``.
        route = Route.get("/x", _controller).middleware("mw_a")
        g1 = RouteGroup(middleware=["mw_b"]).routes([route])
        g2 = RouteGroup(middleware=["mw_c"]).routes(list(g1))
        assert list(g2)[0].get_middleware() == ["mw_c", "mw_b", "mw_a"]

    def test_route_group_via_classmethod_also_composes(self):
        # ``Route.group`` (the classmethod variant) goes through a
        # separate code path that uses ``prepend_middleware``. Same
        # final order is required so both spellings agree.
        route = Route.get("/x", _controller).middleware("inner")
        grouped = Route.group(route, middleware="outer")
        assert grouped[0].get_middleware() == ["outer", "inner"]


# ── 8. Sanity: HTTP method tokens emit cleanly ──────────────────────


class TestAllowHeaderTokenSafety:
    """Belt-and-braces: the Allow-header builder filters anything
    that doesn't look like an HTTP token so a malicious upstream
    can't slip a header-splitting CRLF sequence through.
    """

    def test_token_regex_filters_crlf_injection(self):
        # CR/LF in a method name would split the HTTP message —
        # critical that the filter drops anything containing them.
        handler = DefaultExceptionHandler()
        headers = handler._allow_header_for(
            {"allowed": ["GET", "POST\r\nX-Smuggled: yes"]}
        )
        # Only GET survives the token filter.
        assert headers == [[b"allow", b"GET"]]
        # The dangerous sequence MUST NOT appear in the header bytes.
        assert b"X-Smuggled" not in headers[0][1]
        assert b"\r\n" not in headers[0][1]

    def test_token_regex_matches_standard_methods(self):
        # Pin that the standard verb set all pass the filter (no
        # over-restriction regression).
        handler = DefaultExceptionHandler()
        standard = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
        headers = handler._allow_header_for({"allowed": standard})
        assert headers, "Standard methods must pass the token filter."
        value = headers[0][1].decode()
        for m in standard:
            assert m in value
        # Comma-space separator per RFC 9110 §5.6.1.
        assert re.fullmatch(r"[A-Z]+(, [A-Z]+)*", value)
