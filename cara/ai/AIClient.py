"""``AIClient`` — one interface for OpenRouter / Ollama / OpenAI-compatible APIs.

Provider switch via ``config("ai.provider")``; per-call model override via
``chat(..., model=...)``. Consistent retry, fallback and JSON parsing. App code
should reach this through the ``AI`` facade or by binding the contract it needs.
"""

from __future__ import annotations

import contextlib
import math
import time
from typing import Any

import requests

from cara.ai.AIConfigurationError import AIConfigurationError
from cara.ai.AIProvider import AIProvider
from cara.ai.AIResponse import AIResponse
from cara.ai.AIResponseError import AIResponseError
from cara.ai.Parsing import parse_json as _parse_json
from cara.context import ExecutionContext
from cara.facades import Log


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
        default_temperature: float = 0.3,
        default_max_tokens: int = 1000,
        default_top_p: float = 0.9,
        json_temperature: float = 0.2,
        json_max_tokens: int = 1500,
    ) -> None:
        prov_raw = AIProvider.OPENROUTER if provider is None else provider
        try:
            self.provider = (
                prov_raw
                if isinstance(prov_raw, AIProvider)
                else AIProvider(str(prov_raw).lower())
            )
        except ValueError as exc:
            raise AIConfigurationError(f"Unknown AI provider: {prov_raw!r}") from exc

        if self.provider == AIProvider.OPENROUTER:
            default_base_url = "https://openrouter.ai/api/v1/chat/completions"
            default_model = ""
        elif self.provider == AIProvider.OLLAMA:
            default_base_url = "http://localhost:11434"
            default_model = "mistral"
        else:
            default_base_url = "https://api.openai.com/v1/chat/completions"
            default_model = "gpt-4o-mini"

        self.base_url = self._text(
            "base_url", default_base_url if base_url is None else base_url, required=True
        )
        self.model = self._text(
            "model", default_model if model is None else model, required=False
        )
        self.api_key = self._text(
            "api_key", "" if api_key is None else api_key, required=False
        )
        self.timeout = self._positive_int("timeout", 60 if timeout is None else timeout)
        self.max_retries = self._nonnegative_int(
            "max_retries", 1 if max_retries is None else max_retries
        )
        self.fallback_model = self._optional_text("fallback_model", fallback_model)
        self.site_url = self._text(
            "site_url", "" if site_url is None else site_url, required=False
        )
        self.site_name = self._text(
            "site_name", "" if site_name is None else site_name, required=False
        )
        self.default_temperature = self._temperature(
            "default_temperature", default_temperature
        )
        self.default_max_tokens = self._positive_int(
            "default_max_tokens", default_max_tokens
        )
        self.default_top_p = self._top_p("default_top_p", default_top_p)
        self.json_temperature = self._temperature("json_temperature", json_temperature)
        self.json_max_tokens = self._positive_int("json_max_tokens", json_max_tokens)

    @staticmethod
    def _text(name: str, value: object, *, required: bool) -> str:
        if not isinstance(value, str) or (required and not value.strip()):
            qualifier = "non-empty " if required else ""
            raise AIConfigurationError(f"AI {name} must be a {qualifier}string")
        return value.strip()

    @classmethod
    def _optional_text(cls, name: str, value: object) -> str | None:
        if value is None:
            return None
        parsed = cls._text(name, value, required=False)
        return parsed or None

    @staticmethod
    def _positive_int(name: str, value: object) -> int:
        if type(value) is not int or value <= 0:
            raise AIConfigurationError(f"AI {name} must be a positive integer")
        return value

    @staticmethod
    def _nonnegative_int(name: str, value: object) -> int:
        if type(value) is not int or value < 0:
            raise AIConfigurationError(f"AI {name} must be a non-negative integer")
        return value

    @staticmethod
    def _temperature(name: str, value: object) -> float:
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise AIConfigurationError(f"AI {name} must be a finite number from 0 to 2")
        parsed = float(value)
        if not math.isfinite(parsed) or not 0 <= parsed <= 2:
            raise AIConfigurationError(f"AI {name} must be a finite number from 0 to 2")
        return parsed

    @staticmethod
    def _top_p(name: str, value: object) -> float:
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise AIConfigurationError(
                f"AI {name} must be a finite number above 0 and at most 1"
            )
        parsed = float(value)
        if not math.isfinite(parsed) or not 0 < parsed <= 1:
            raise AIConfigurationError(
                f"AI {name} must be a finite number above 0 and at most 1"
            )
        return parsed

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
        response_format: dict[str, Any] | None = None,
        json_mode: bool | None = None,
    ) -> AIResponse:
        """Chat completion. ``prompt`` is a string or a messages array.

        ``json_mode=True`` is sugar for ``response_format={"type":
        "json_object"}`` (OpenAI-compatible providers; mapped to
        ``format: "json"`` on Ollama). An explicit ``response_format``
        wins over ``json_mode``.
        """
        if temperature is None:
            temperature = self.default_temperature
        else:
            temperature = self._temperature("temperature", temperature)
        if max_tokens is None:
            max_tokens = self.default_max_tokens
        else:
            max_tokens = self._positive_int("max_tokens", max_tokens)
        if top_p is None:
            top_p = self.default_top_p
        else:
            top_p = self._top_p("top_p", top_p)
        if response_format is None and json_mode:
            response_format = {"type": "json_object"}

        if isinstance(prompt, str):
            messages: list[dict[str, str]] = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
        else:
            messages = list(prompt)

        requested = model or self.model
        if not requested:
            raise AIConfigurationError(
                f"No model configured for AI provider '{self.provider.value}'. "
                f"Set ai.{self.provider.value}_model in this deployable's "
                f"config/ai.py, or pass model=... to the call."
            )
        models_to_try = self._models_to_try(requested)

        last_error: Exception | None = None
        for attempt_model in models_to_try:
            for retry in range(self.max_retries + 1):
                try:
                    start = time.time()
                    content, usage = self._dispatch(
                        attempt_model,
                        messages,
                        temperature,
                        max_tokens,
                        top_p,
                        response_format=response_format,
                    )
                    dur = int((time.time() - start) * 1000)
                    _log(
                        "debug",
                        f"AI [{self.provider.value}/{attempt_model}] {dur}ms "
                        f"in={len(str(messages))}ch out={len(content)}ch",
                    )
                    response = AIResponse(
                        content=content,
                        model=attempt_model,
                        provider=self.provider,
                        tokens_in=(usage or {}).get("prompt_tokens"),
                        tokens_out=(usage or {}).get("completion_tokens"),
                        duration_ms=dur,
                        finish_reason=(usage or {}).get("finish_reason"),
                    )
                    self._on_attempt_success(attempt_model, response)
                    return response
                except requests.exceptions.Timeout as e:
                    last_error = Exception(f"AI timeout ({self.timeout}s)")
                    _log(
                        "warning",
                        f"AI timeout [{self.provider.value}/{attempt_model}] retry={retry}",
                    )
                    self._on_attempt_error(attempt_model, e, None, retry)
                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, "status_code", "?")
                    body = (getattr(e.response, "text", "") or "")[:200]
                    last_error = Exception(f"AI HTTP {status}: {body}")
                    _log(
                        "error",
                        f"AI HTTP {status} [{self.provider.value}/{attempt_model}]: {body}",
                    )
                    self._on_attempt_error(attempt_model, e, status, retry)
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
                        except TypeError, ValueError:
                            wait_s = 0.0
                        if wait_s <= 0:
                            wait_s = self._backoff_seconds(retry, status=429)
                        _log(
                            "warning",
                            f"AI 429 [{self.provider.value}/{attempt_model}] "
                            f"backing off {wait_s:.1f}s",
                        )
                        time.sleep(wait_s)
                except Exception as e:  # noqa: BLE001 — record and retry/fallback
                    last_error = e
                    _log("error", f"AI fail [{self.provider.value}/{attempt_model}]: {e}")
                    self._on_attempt_error(attempt_model, e, None, retry)
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
        response_format: dict[str, Any] | None = None,
        json_mode: bool | None = None,
    ) -> Any:
        """Chat + parse the response as JSON. Returns ``fallback`` on error."""
        if temperature is None:
            temperature = self.json_temperature
        else:
            temperature = self._temperature("temperature", temperature)
        if max_tokens is None:
            max_tokens = self.json_max_tokens
        else:
            max_tokens = self._positive_int("max_tokens", max_tokens)

        try:
            resp = self.chat(
                prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                model=model,
                system=system,
                response_format=response_format,
                json_mode=json_mode,
            )
            return self.parse_json(resp.content, fallback=fallback)
        except AIConfigurationError:
            # A missing model is a deployment defect, not a flaky response —
            # ``fallback`` must not turn it into a permanently silent no-op.
            raise
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
        response_format: dict[str, Any] | None = None,
        json_mode: bool | None = None,
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
            response_format=response_format,
            json_mode=json_mode,
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
        response_format: dict[str, Any] | None = None,
        json_mode: bool | None = None,
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
            response_format=response_format,
            json_mode=json_mode,
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

    # -- retry-loop seams (override points for app policy layers) ---------- #

    def _models_to_try(self, requested: str) -> list[str]:
        """The ordered model chain one ``chat`` call walks. Override to
        inject an app-level fallback chain."""
        models = [requested]
        if self.fallback_model and self.fallback_model != requested:
            models.append(self.fallback_model)
        return models

    def _on_attempt_success(self, model: str, response: AIResponse) -> None:
        """Observation hook — called once per successful attempt, before the
        response is returned. Default: no-op. Override for metrics/audit;
        must never raise into the caller (wrap your own errors)."""

    def _on_attempt_error(
        self, model: str, error: Exception, status: int | None, retry: int
    ) -> None:
        """Observation hook — called once per failed attempt (``status`` is
        the HTTP status when the failure was an HTTPError, else ``None``).
        Default: no-op. Override for metrics / breakers / cooldowns; must
        never raise into the caller."""

    def _backoff_seconds(self, retry: int, *, status: int | None = None) -> float:
        """Wait before the next retry when the provider gave no explicit
        ``Retry-After``. Default: capped exponential."""
        return float(min(2**retry, 30))

    # -- provider dispatch ------------------------------------------------ #

    def _dispatch(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        top_p: float,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        if self.provider == AIProvider.OLLAMA:
            return self._call_ollama(
                model,
                messages,
                temperature,
                max_tokens,
                top_p,
                response_format=response_format,
            )
        return self._call_openai_compatible(
            model,
            messages,
            temperature,
            max_tokens,
            top_p,
            response_format=response_format,
        )

    def _call_openai_compatible(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        top_p: float,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if self.provider == AIProvider.OPENROUTER:
            headers["HTTP-Referer"] = self.site_url
            headers["X-Title"] = self.site_name

        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
        }
        if response_format is not None:
            payload["response_format"] = response_format

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
        usage = dict(data.get("usage") or {})
        finish_reason = choices[0].get("finish_reason")
        if finish_reason:
            # Ride along in the usage dict so the transport contract stays a
            # 2-tuple; ``chat()`` lifts it onto ``AIResponse.finish_reason``.
            usage["finish_reason"] = finish_reason
        return content, usage

    def _call_ollama(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        top_p: float,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        url = f"{self.base_url.rstrip('/')}/api/chat"
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "top_p": top_p,
                "num_predict": max_tokens,
            },
        }
        # Ollama speaks ``format: "json"`` instead of the OpenAI-style
        # ``response_format`` object.
        if response_format and response_format.get("type") == "json_object":
            payload["format"] = "json"

        resp = requests.post(url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            raise AIResponseError(
                f"Ollama response is not valid JSON: {resp.text[:200]}"
            ) from e
        if "error" in data:
            raise AIResponseError(f"Ollama error: {data['error']}")
        content = (data.get("message") or {}).get("content", "").strip()
        usage: dict[str, Any] = {
            "prompt_tokens": data.get("prompt_eval_count"),
            "completion_tokens": data.get("eval_count"),
        }
        done_reason = data.get("done_reason") or (
            "length" if data.get("truncated") else None
        )
        if done_reason:
            usage["finish_reason"] = done_reason
        return content, usage
