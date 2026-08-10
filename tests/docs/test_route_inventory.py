"""Gate: a sharded route tree may never render as an empty page.

``routes:generate`` emits an aggregator that splices bounded shards in with a
starred argument — ``Route.prefix("/api").routes(*groups)``. A starred argument
is invisible to a static walker, so a shard-blind extractor finds nothing and
writes "_Total: 0 routes._".

That is the worst failure this engine has: writes are idempotent, so a page
that produces nothing reports "(unchanged)" on every run forever. One product
shipped an empty route reference for weeks while its generator claimed success
on each invocation — and the fix existed the whole time, in the other product's
copy of the same file. Parser and emitter now version together in the
framework, and this test is what keeps the shard walk from being dropped again.
"""

from __future__ import annotations

from cara.docs.Inventory import collect_routes, gen_routes, parse_routes

from ._fixtures import make_checkout, manifest_for, write

AGGREGATOR = """
from cara.routing import Route

from .generated.api import route_groups as groups


def register_api_routes():
    return Route.prefix("/api").middleware(["throttle:api"]).routes(*groups())
"""

SHARD = """
from cara.routing import Route


def route_groups():
    return (
        Route.prefix("/widgets").routes(
            Route.get("/", "WidgetController@index", name="widgets.index"),
            Route.post("/", "WidgetController@store", name="widgets.store"),
        ),
    )
"""

SINGLE_FILE = """
from cara.routing import Route


def register_web_routes():
    return Route.get("/health", "HealthController@show", name="health")
"""


def _sharded_checkout(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(root / "api" / "routes" / "api.py", AGGREGATOR)
    write(root / "api" / "routes" / "generated" / "api" / "group_000.py", SHARD)
    return root


def test_routes_behind_a_starred_argument_are_still_found(tmp_path):
    root = _sharded_checkout(tmp_path)

    routes = collect_routes(root)["api.py"]

    assert [(verb, path) for verb, path, _h, _n, _mw in routes] == [
        ("GET", "/api/widgets/"),
        ("POST", "/api/widgets/"),
    ]


def test_the_aggregators_prefix_is_applied_to_every_shard_route(tmp_path):
    """The wrapper carries the mount point; the shards carry only their own.

    Losing the outer prefix would publish a reference page full of paths that
    no client can call — accurate-looking and wrong, the failure mode this
    whole subsystem exists to prevent.
    """
    root = _sharded_checkout(tmp_path)

    paths = [path for _v, path, _h, _n, _mw in collect_routes(root)["api.py"]]

    assert all(path.startswith("/api/") for path in paths)


def test_a_single_file_route_module_still_works(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(root / "api" / "routes" / "web.py", SINGLE_FILE)

    assert [
        (verb, path) for verb, path, _h, _n, _mw in collect_routes(root)["web.py"]
    ] == [("GET", "/health")]


def test_middleware_declared_as_a_keyword_reaches_the_page(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(
        root / "api" / "routes" / "web.py",
        "from cara.routing import Route\n\n\n"
        "def register_web_routes():\n"
        "    return Route.get('/x', 'C@x', name='x', middleware=['auth'])\n",
    )

    routes = parse_routes(root / "api" / "routes" / "web.py")

    assert routes[0][4] == ["auth"]


def test_the_generated_page_reports_the_real_total(tmp_path):
    root = _sharded_checkout(tmp_path)
    manifest = manifest_for(root, "alpha")

    gen_routes(manifest, "2026-01-01 00:00", lambda _line: None)
    page = (manifest.reference / "routes.md").read_text(encoding="utf-8")

    assert "_Total: 2 routes._" in page
    assert "_Total: 0 routes._" not in page
    assert "product: alpha" in page


def test_a_route_module_that_does_not_exist_contributes_nothing(tmp_path):
    root = make_checkout(tmp_path, "alpha")

    assert collect_routes(root) == {
        "api.py": [],
        "web.py": [],
        "websocket.py": [],
        "broadcasting.py": [],
    }
