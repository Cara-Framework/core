"""OpenAPI 3.0 spec generator for the Cara router.

Walks the registered route table and emits a minimal but valid
OpenAPI 3.0.3 JSON document on stdout (or to ``--out=path``). The
storefront consumes this via ``openapi-typescript`` to produce a
fully-typed fetch client — when the backend renames a field, the TS
compiler in ``storefront/`` fails the next build instead of the user
discovering it at runtime.

Scope
-----
The output is intentionally **shape-only**: every path lists its
method, URI parameters (with type derived from the Cara
``@param:type`` compiler hint), and middleware names as
``x-cara-middleware`` extension. Request / response bodies are not
introspected — controllers don't carry type annotations dense enough
to infer Pydantic models without false positives. The frontend codegen
treats unspecified bodies as ``unknown``; that's still a net win over
``any`` everywhere today.

Usage
-----
    craft routes:openapi                            # pretty JSON to stdout
    craft routes:openapi --out=openapi.json        # write to file
    craft routes:openapi --out=openapi.json --pretty=false   # one-liner

Round-trip safety
-----------------
This command never modifies routes / controllers. It only reads the
in-memory router table that :func:`register_routes` has already
populated during app boot. A missing controller or malformed route
yields a warning (printed to stderr) and the offending entry is
skipped — the rest of the spec still emits.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from cara.commands import CommandBase
from cara.decorators import command


# Cara compiler hints map to OpenAPI parameter schemas. ``str`` is the
# default fallback so an unknown / missing compiler doesn't drop the
# parameter.
_TYPE_MAP: dict[str, dict[str, Any]] = {
    "int": {"type": "integer", "format": "int64"},
    "uint": {"type": "integer", "format": "int64", "minimum": 0},
    "float": {"type": "number"},
    "bool": {"type": "boolean"},
    "uuid": {"type": "string", "format": "uuid"},
    "str": {"type": "string"},
}

# Cara path syntax: ``/foo/@id:int/bar/@slug``. OpenAPI wants
# ``/foo/{id}/bar/{slug}``. This regex extracts the name + optional
# ``:type`` so we can rebuild the canonical path and emit a parameter
# schema for each placeholder.
_PARAM_RE = re.compile(
    r"@(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<type>[A-Za-z_][A-Za-z0-9_]*))?"
)


@command(
    name="routes:openapi",
    help="Dump the route table as an OpenAPI 3.0 JSON document.",
    options={
        "--out=?": "Write the spec to this path instead of stdout.",
        "--pretty=?": "Indent the JSON output (default: true). Pass false for one-liner.",
        "--title=?": "Spec ``info.title``. Defaults to ``Cheapa API``.",
        "--version=?": "Spec ``info.version``. Defaults to ``0.0.0``.",
    },
)
class RouteOpenApiCommand(CommandBase):
    """Emit OpenAPI 3.0 from the in-memory route table."""

    def handle(
        self,
        out: str | None = None,
        pretty: str | None = None,
        title: str | None = None,
        version: str | None = None,
    ) -> None:
        # ── pull the live route table ────────────────────────────────
        try:
            router = self.application.make("router")
            routes = list(router.routes)
        except Exception as exc:  # noqa: BLE001
            self.error(f"❌ Could not access router: {exc}")
            return

        if not routes:
            self.warning("⚠️  No routes registered — emitting empty spec.")

        # ── build the spec ──────────────────────────────────────────
        spec: dict[str, Any] = {
            "openapi": "3.0.3",
            "info": {
                "title": title or "Cheapa API",
                "version": version or "0.0.0",
                "description": (
                    "Auto-generated from the Cara router. Run "
                    "`craft routes:openapi` to refresh."
                ),
            },
            "paths": {},
        }

        skipped = 0
        for route in routes:
            try:
                path, params = self._build_path(route.url)
                for method in route.request_method:
                    operation = self._build_operation(route, method, params)
                    spec["paths"].setdefault(path, {})[method] = operation
            except Exception as exc:  # noqa: BLE001
                skipped += 1
                print(
                    f"openapi: skipped route {getattr(route, 'url', '?')}: {exc}",
                    file=sys.stderr,
                )

        # ── serialise + write ───────────────────────────────────────
        pretty_flag = (pretty or "true").lower() != "false"
        body = json.dumps(spec, indent=2 if pretty_flag else None, sort_keys=True)

        if out:
            target = Path(out).resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body + "\n")
            self.info(
                f"✓ OpenAPI spec written to {target} "
                f"({len(spec['paths'])} paths, {skipped} skipped)"
            )
        else:
            print(body)

    # ── helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _build_path(cara_url: str) -> tuple[str, list[dict[str, Any]]]:
        """Convert a Cara URL into an OpenAPI path + parameter list.

        ``/products/@id:int`` → ``("/products/{id}",
        [{"name": "id", "in": "path", "required": True,
          "schema": {"type": "integer", "format": "int64"}}])``
        """
        params: list[dict[str, Any]] = []

        def _replace(m: re.Match[str]) -> str:
            name = m.group("name")
            type_hint = (m.group("type") or "str").lower()
            schema = _TYPE_MAP.get(type_hint, _TYPE_MAP["str"]).copy()
            params.append(
                {
                    "name": name,
                    "in": "path",
                    "required": True,
                    "schema": schema,
                }
            )
            return "{" + name + "}"

        path = _PARAM_RE.sub(_replace, cara_url)
        return path, params

    @staticmethod
    def _build_operation(
        route: Any, method: str, params: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Build the OpenAPI operation object for one ``(path, method)``."""
        # Operation ID — kebab-case for OpenAPI convention.
        op_id = route.get_name() or f"{method}-{route.url}"
        op_id = re.sub(r"[^A-Za-z0-9]+", "-", op_id).strip("-").lower()

        # Cara stores middleware on the route, but the public attribute
        # name varies across versions; probe both.
        middleware: list[str] = []
        for attr in ("_middleware", "middleware"):
            mw = getattr(route, attr, None)
            if isinstance(mw, list):
                middleware = [str(m) for m in mw]
                break
            if callable(mw):
                try:
                    middleware = [str(m) for m in (mw() or [])]
                    break
                except TypeError:
                    pass

        # Controller resolver — best-effort string repr; some Cara
        # versions store the controller as ``"FooController@bar"``,
        # others wrap it in a RouteResolver. Either way ``repr`` is
        # informative enough for tooling.
        controller_repr = ""
        resolver = getattr(route, "controller", None)
        if resolver is not None:
            controller_repr = getattr(resolver, "raw", None) or repr(resolver)

        operation: dict[str, Any] = {
            "operationId": op_id,
            "tags": [_first_segment(route.url)],
            "summary": op_id.replace("-", " "),
            "responses": {
                "200": {"description": "Success"},
            },
        }

        if params:
            operation["parameters"] = params

        if middleware:
            operation["x-cara-middleware"] = middleware
        if controller_repr:
            operation["x-cara-controller"] = controller_repr

        return operation


def _first_segment(url: str) -> str:
    """``/api/products/@id`` → ``products`` (used as the OpenAPI tag)."""
    parts = [p for p in url.split("/") if p and not p.startswith("@")]
    if not parts:
        return "root"
    return parts[1] if parts[0] in {"api", "v1", "v2"} and len(parts) > 1 else parts[0]
