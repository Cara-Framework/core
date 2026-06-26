"""Cara AI client — one interface for OpenRouter / Ollama / OpenAI-compatible APIs.

Provider switch via ``config("ai.provider")``; per-call model override via
``chat(..., model=...)``. Consistent retry, fallback and JSON parsing. App code
should reach this through the ``AI`` facade or by binding the contract it needs.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any

import requests

from cara.ai.AIProvider import AIProvider
from cara.ai.AIResponse import AIResponse
from cara.ai.exceptions import AIResponseError
from cara.ai.Parsing import parse_json as _parse_json
from cara.configuration import config
from cara.context import ExecutionContext
from cara.facades import Log


def _cfg(key: str, default: Any = None) -> Any:
    """Read ``ai.<key>`` from application config (env-backed via config/ai.py)."""
    val = config(f"ai.{key}", None)
    if val is not None and val != "":
        return val
    return default


def _log(level: str, msg: str) -> None:
    # Logging must never break an AI call.
    with contextlib.suppress(Exception):
        getattr(Log, level)(msg, category="cara.ai")


class AIClient:
    """Single entry point for AI chat / JSON calls.

    Example::

        ai = AIClient()
        ai.chat("What is 2+2?").content
        ai.json('Return {"answer": 4}')
        AIClient(provider="ollama", model="mistral").chat("…")
    """

    def __init__(
        self,
        provider: str | AIProvider | None = None,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: int | None = None,
        max_retries: int | None = None,
        fallback_model: str | None = None,
        site_url: str | None = None,
        site_name: str | None = None,
    ) -> None:
        prov_raw = provider or _cfg("provider", "openrouter")
        try:
            self.provider = AIProvider(str(prov_raw).lower())
        except ValueError:
            _log("warning", f"Unknown AI provider '{prov_raw}', using openrouter")
            self.provider = AIProvider.OPENROUTER

        if self.provider == AIProvider.OPENROUTER:
            self.base_url = base_url or _cfg(
                "openrouter_base_url", "https://openrouter.ai/api/v1/chat/completions"
            )
            self.model = model or _cfg(
                "openrouter_model", "mistralai/mistral-small-3.1-24b-instruct"
            )
            self.api_key = api_key or _cfg("openrouter_api_key", "")
        elif self.provider == AIProvider.OLLAMA:
            self.base_url = base_url or _cfg("ollama_base_url", "http://localhost:11434")
            self.model = model or _cfg("ollama_model", "mistral")
            self.api_key = api_key or _cfg("ollama_api_key", "")
        else:
            self.base_url = base_url or _cfg(
                "openai_base_url", "https://api.openai.com/v1/chat/completions"
            )
            self.model = model or _cfg("openai_model", "gpt-4o-mini")
            self.api_key = api_key or _cfg("openai_api_key", "")

        self.timeout = int(timeout or _cfg("timeout", 60))
        self.max_retries = int(max_retries or _cfg("max_retries", 1))
        self.fallback_model = fallback_model or _cfg("fallback_model", None)
        self.site_url = site_url or _cfg("openrouter_site_url", "")
        self.site_name = site_name or _cfg("openrouter_site_name", "")

    # -- public API ------------------------------------------------------- #

    def chat(
        self,
        prompt: str | list[dict[str, str]],
        *,
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
        model: str | None = None,
        system: str | None = None,
    ) -> AIResponse:
        """Chat completion. ``prompt`` is a string or a messages array."""
        if temperature is None:
            temperature = float(config("ai.default_temperature", 0.3))
        if max_tokens is None:
            max_tokens = int(config("ai.default_max_tokens", 1000))
        if top_p is None:
            top_p = float(config("ai.default_top_p", 0.9))

        if isinstance(prompt, str):
            messages: list[dict[str, str]] = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
        else:
            messages = list(prompt)

        use_model = model or self.model
        models_to_try = [use_model]
        if self.fallback_model and self.fallback_model != use_model:
            models_to_try.append(self.fallback_model)

        last_error: Exception | None = None
        for attempt_model in models_to_try:
            for retry in range(self.max_retries + 1):
                try:
                    start = time.time()
                    content, usage = self._dispatch(
                        attempt_model, messages, temperature, max_tokens, top_p
                    )
                    dur = int((time.time() - start) * 1000)
                    _log(
                        "debug",
                        f"AI [{self.provider.value}/{attempt_model}] {dur}ms "
                        f"in={len(str(messages))}ch out={len(content)}ch",
                    )
                    return AIResponse(
                        content=content,
                        model=attempt_model,
                        provider=self.provider,
                        tokens_in=(usage or {}).get("prompt_tokens"),
                        tokens_out=(usage or {}).get("completion_tokens"),
                        duration_ms=dur,
                    )
                except requests.exceptions.Timeout:
                    last_error = Exception(f"AI timeout ({self.timeout}s)")
                    _log(
                        "warning",
                        f"AI timeout [{self.provider.value}/{attempt_model}] retry={retry}",
                    )
                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, "status_code", "?")
                    body = (getattr(e.response, "text", "") or "")[:200]
                    last_error = Exception(f"AI HTTP {status}: {body}")
                    _log(
                        "error",
                        f"AI HTTP {status} [{self.provider.value}/{attempt_model}]: {body}",
                    )
                    if status in (400, 401, 403, 404):
                        break
                    if status == 429:
                        retry_after_raw = (
                            (e.response.headers or {}).get("Retry-After")
                            if e.response
                            else None
                        )
                        try:
                            wait_s = float(retry_after_raw) if retry_after_raw else 0.0
                        except (TypeError, ValueError):
                            wait_s = 0.0
                        if wait_s <= 0:
                            wait_s = min(2**retry, 30)
                        _log(
                            "warning",
                            f"AI 429 [{self.provider.value}/{attempt_model}] "
                            f"backing off {wait_s:.1f}s",
                        )
                        time.sleep(wait_s)
                except Exception as e:  # noqa: BLE001 — record and retry/fallback
                    last_error = e
                    _log("error", f"AI fail [{self.provider.value}/{attempt_model}]: {e}")
            if attempt_model != models_to_try[-1]:
                _log("warning", f"AI falling back to {models_to_try[-1]}")

        raise last_error or RuntimeError("AI call failed with no specific error")

    def json(
        self,
        prompt: str | list[dict[str, str]],
        *,
        fallback: Any = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
        system: str | None = None,
    ) -> Any:
        """Chat + parse the response as JSON. Returns ``fallback`` on error."""
        if temperature is None:
            temperature = float(config("ai.json_temperature", 0.2))
        if max_tokens is None:
            max_tokens = int(config("ai.json_max_tokens", 1500))

        try:
            resp = self.chat(
                prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                model=model,
                system=system,
            )
            return self.parse_json(resp.content, fallback=fallback)
        except Exception as e:  # noqa: BLE001 — fall back when the caller allows
            if fallback is not None:
                _log("warning", f"AI.json fallback: {e}")
                return fallback
            raise

    async def achat(
        self,
        prompt: str | list[dict[str, str]],
        *,
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
        model: str | None = None,
        system: str | None = None,
    ) -> AIResponse:
        """Async wrapper that runs the sync chat call off the event loop."""
        return await ExecutionContext.run_in_thread(
            self.chat,
            prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            model=model,
            system=system,
        )

    async def ajson(
        self,
        prompt: str | list[dict[str, str]],
        *,
        fallback: Any = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
        system: str | None = None,
    ) -> Any:
        """Async wrapper that runs the sync json call off the event loop."""
        return await ExecutionContext.run_in_thread(
            self.json,
            prompt,
            fallback=fallback,
            temperature=temperature,
            max_tokens=max_tokens,
            model=model,
            system=system,
        )

    @staticmethod
    def parse_json(raw: str, *, fallback: Any = None) -> Any:
        """Parse JSON from an AI response (markdown-fence / truncation tolerant)."""
        return _parse_json(raw, fallback=fallback)

    def get_config(self) -> dict[str, Any]:
        return {
            "provider": self.provider.value,
            "model": self.model,
            "base_url": self.base_url,
            "timeout": self.timeout,
            "has_api_key": bool(self.api_key),
        }

    # -- provider dispatch ------------------------------------------------ #

    def _dispatch(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        top_p: float,
    ) -> tuple[str, dict[str, Any]]:
        if self.provider == AIProvider.OLLAMA:
            return self._call_ollama(model, messages, temperature, max_tokens, top_p)
        return self._call_openai_compatible(
            model, messages, temperature, max_tokens, top_p
        )

    def _call_openai_compatible(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        top_p: float,
    ) -> tuple[str, dict[str, Any]]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if self.provider == AIProvider.OPENROUTER:
            headers["HTTP-Referer"] = self.site_url
            headers["X-Title"] = self.site_name

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
        }

        resp = requests.post(
            self.base_url, json=payload, headers=headers, timeout=self.timeout
        )
        resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            raise AIResponseError(
                f"AI provider returned no choices "
                f"(model={data.get('model')!r}, error={data.get('error')!r})"
            )
        content = (choices[0].get("message") or {}).get("content", "").strip()
        usage = data.get("usage") or {}
        return content, usage

    def _call_ollama(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        top_p: float,
    ) -> tuple[str, dict[str, Any]]:
        url = f"{self.base_url.rstrip('/')}/api/chat"
        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "top_p": top_p,
                "num_predict": max_tokens,
            },
        }
        resp = requests.post(url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        content = (data.get("message") or {}).get("content", "").strip()
        usage = {
            "prompt_tokens": data.get("prompt_eval_count"),
            "completion_tokens": data.get("eval_count"),
        }
        return content, usage
