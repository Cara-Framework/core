"""ReplayIdempotentRequests — the ``Idempotency-Key`` header on a state-changing door.

Stripe's contract, once for every route that declares the middleware: the
first request with a key runs; a retry with the same request answers the
stored response (``Idempotent-Replayed: true``) without running the door
again; the same key with a different request is a 422; a retry while the
first runs is a 409. No header: the request runs as before.

Scope and fingerprint
---------------------
* A key belongs to the CALLER (:meth:`scope`, by default the signed-in user;
  a product overrides it to add its tenant or API credential) and the ROUTE,
  so two integrations, or two doors, never collide on a key. A caller the
  scope cannot name (no identity) runs without a claim.
* The fingerprint is the method, the path (its ids), the query string and
  the body: canonical JSON for a JSON body (any JSON value, not only an
  object), the fields for a form, and each uploaded file by its SHA-256 —
  a retried upload fingerprints the same, another file under the same key
  is the 422 of a different request.

What the first run leaves behind decides what the key does next
-----------------------------------------------------------------
- an answer below 500 that the door reached (2xx, or any answer after the
  door said its effect landed) is stored and replayed for 24 h — status,
  body and the headers that describe it (:attr:`kept_header_prefixes`);
- a refusal before the effect (validation, permission, a 4xx the door
  raised) or a failure before it frees the key, so the retry runs;
- a failure AFTER the effect landed (``IdempotencyClaim.landed``), a
  streamed answer that cannot be stored, or a request cancelled mid-door
  keeps the key closed: 409 while it may still run, then "check, then send
  a new key" — never a second effect;
- a claim store that cannot be reached is a 503 before the door runs.

Usage
-----
Register an alias (``"idempotent": ReplayIdempotentRequests`` or a product
subclass) and declare it LAST on a route's middleware, after authentication
and tenancy (``middleware=["idempotent"]``): the scope reads what they
resolved. The door calls ``IdempotencyClaim.landed(request)`` right after
its use case returns.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

from cara.context import ExecutionContext
from cara.http import IdempotencyClaim, Request, Response
from cara.middleware.Middleware import Middleware

#: Methods that change state; a key on a read is ignored.
_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})

_REPLAYED_HEADER = "Idempotent-Replayed"


class ReplayIdempotentRequests(Middleware):
    """Claim, replay or free one request's ``Idempotency-Key``."""

    #: Response headers that describe the stored body and so travel with
    #: it (lower-case prefixes). Headers the outer middleware stamps on
    #: every answer (request id, CORS, rate limit) are stamped on the
    #: replay again and are not stored.
    kept_header_prefixes: tuple[str, ...] = (
        "content-type",
        "content-disposition",
        "location",
    )

    def scope(self, request: Request) -> str | None:
        """Who the key belongs to, or None to run without a claim. The
        default is the signed-in user; override to add a tenant or an API
        credential."""
        user_id = getattr(request.user(), "id", None)
        return None if user_id is None else f"user:{user_id}"

    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Response]]
    ) -> Response:
        if request.method not in _MUTATING:
            return await next_fn(request)
        raw = IdempotencyClaim.request_key(request)
        scope = self.scope(request) if raw is not None else None
        if raw is None or scope is None:
            return await next_fn(request)
        claim = await ExecutionContext.run_in_thread(
            IdempotencyClaim.claim,
            self._cache_key(request, scope, raw),
            IdempotencyClaim.digest(await self._fingerprint(request)),
        )
        if claim.answer is not None:
            return self._replay(claim.answer)
        claim.attach(request)
        try:
            response = await next_fn(request)
        except Exception:
            # Raised refusals and faults land here (CancelledError does not:
            # a cancelled door may still commit in its thread, so its key
            # stays closed). Before the effect nothing changed.
            if not claim.effect_landed:
                await ExecutionContext.run_in_thread(claim.release)
            raise
        await self._settle(claim, response)
        return response

    async def _settle(self, claim: IdempotencyClaim, response: Response) -> None:
        status = int(response.get_status_code())
        stored = status < 500 and (status < 400 or claim.effect_landed)
        answer = self._answer(response, status) if stored else None
        if answer is not None:
            await ExecutionContext.run_in_thread(claim.keep, answer)
        elif status >= 400 and not claim.effect_landed:
            await ExecutionContext.run_in_thread(claim.release)
        # Kept closed ("running", an unknown outcome): a 5xx after the
        # effect, and a 2xx whose body cannot be stored (a stream) — it ran,
        # so a retry must not run it again.

    @staticmethod
    def _cache_key(request: Request, scope: str, raw: str) -> str:
        route = request.get_route()
        name = route.get_name() if route is not None else None
        return ":".join(
            (
                "idem",
                scope,
                str(name or f"{request.method} {request.path}"),
                IdempotencyClaim.digest(raw),
            )
        )

    @staticmethod
    async def _fingerprint(request: Request) -> dict[str, Any]:
        """Method, path, query and body, as plain canonical data."""
        content_type = str(request.header("content-type") or "")
        media = content_type.split(";", 1)[0].strip().lower()
        if media == "application/json" or media.endswith("+json"):
            # Any JSON value: ``all()`` keeps only an object's keys, so a
            # list body fingerprinted as nothing.
            body: Any = await request.json()
        else:
            body = await request.all()
            if media == "multipart/form-data":
                # Uploads are not in the form fields: hash their bytes.
                files = await request.files()
                body = {
                    "fields": body,
                    "files": {
                        str(name): {
                            "name": upload.filename,
                            "sha256": hashlib.sha256(upload.content).hexdigest(),
                        }
                        for name, upload in files.items()
                    },
                }
        return {
            "method": request.method,
            "path": request.path,
            "query": request.query_params,
            "body": body,
        }

    def _answer(self, response: Response, status: int) -> dict | None:
        if response.is_streaming():
            # A streamed body cannot be stored; its ``content`` is empty.
            return None
        content = getattr(response, "content", None)
        if isinstance(content, str):
            content = content.encode("utf-8")
        if not isinstance(content, bytes):
            return None
        headers = {
            name: str(value)
            for name, value in response.headers.all().items()
            if name.lower().startswith(self.kept_header_prefixes)
        }
        return {
            "status": status,
            "body": base64.b64encode(content).decode("ascii"),
            "headers": headers,
        }

    def _replay(self, answer: dict) -> Response:
        replay = Response(self.application)
        replay.set_content(base64.b64decode(str(answer.get("body") or "")))
        replay.status(int(answer.get("status") or 200))
        for name, value in dict(answer.get("headers") or {}).items():
            replay.header(name, value)
        replay.header(_REPLAYED_HEADER, "true")
        return replay


__all__ = ["ReplayIdempotentRequests"]
