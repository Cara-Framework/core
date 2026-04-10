from __future__ import annotations
"""OpenRouter API wrapper — for planner/reviewer when using openrouter provider."""

import json
import os
import time
import subprocess
from dataclasses import dataclass, field
from typing import Optional, Callable

from .config import Cfg


@dataclass
class OpenRouterUsage:
    """Tracks OpenRouter API usage."""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    duration_ms: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def format(self) -> str:
        inp = _fmt_tokens(self.input_tokens)
        out = _fmt_tokens(self.output_tokens)
        return f"{inp} in / {out} out, ${self.cost_usd:.4f}, {self.duration_ms/1000:.1f}s"


@dataclass
class OpenRouterResult:
    text: str
    ok: bool
    usage: OpenRouterUsage = field(default_factory=OpenRouterUsage)
    error: Optional[str] = None
    session_id: Optional[str] = None  # Not used for OpenRouter, compat with ClaudeResult


_session_usage = OpenRouterUsage()

def get_session_usage() -> OpenRouterUsage:
    return _session_usage

def reset_session_usage():
    global _session_usage
    _session_usage = OpenRouterUsage()


OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"


def ask_openrouter(
    prompt: str,
    cfg: Cfg,
    model: Optional[str] = None,
    system_prompt: Optional[str] = None,
    timeout: int = 600,
    on_event: Optional[Callable[[str, dict], None]] = None,
) -> OpenRouterResult:
    """Call OpenRouter API for planning/reviewing (non-local models).

    Uses curl for simplicity — no extra Python deps needed.
    """
    model = model or cfg.planner_cfg.get("model", "qwen/qwen3-coder")
    api_key = cfg.openrouter_api_key
    cb = on_event or (lambda *a: None)

    if not api_key:
        return OpenRouterResult("", False, error="OPENROUTER_API_KEY not set")

    # FIX 10: prompt-cache hints. Structured system content with
    # cache_control: ephemeral lets caching-capable providers (Anthropic,
    # DeepInfra's Qwen, …) reuse the system prompt across calls.
    # Providers that don't support the marker ignore it.
    messages = []
    if system_prompt:
        messages.append({
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
        })
    messages.append({"role": "user", "content": prompt})

    # max_tokens / temperature are env-tunable so we can bump them when a
    # task hits the ceiling without editing code.
    try:
        _max_tok = max(1024, int(os.environ.get("USTA_QWEN_MAX_TOKENS", "16384")))
    except ValueError:
        _max_tok = 16384
    try:
        _temp = float(os.environ.get("USTA_QWEN_TEMPERATURE", "0.3"))
    except ValueError:
        _temp = 0.3

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": _max_tok,
        "temperature": _temp,
        # Ask OpenRouter to surface detailed cache/cost usage. Unknown
        # fields are dropped silently per-provider.
        "usage": {"include": True},
    }

    t0 = time.time()
    cb("start", {})

    try:
        # Use curl to call OpenRouter API
        cmd = [
            "curl", "-s", "-X", "POST", OPENROUTER_API_URL,
            "-H", f"Authorization: Bearer {api_key}",
            "-H", "Content-Type: application/json",
            "-H", "HTTP-Referer: https://github.com/cfkarakulak/usta",
            "-d", json.dumps(payload),
        ]

        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )

        if proc.returncode != 0:
            return OpenRouterResult("", False, error=f"curl failed: {proc.stderr}")

        try:
            resp = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return OpenRouterResult("", False, error=f"Invalid JSON: {proc.stdout[:200]}")

        if "error" in resp:
            err_msg = resp["error"].get("message", str(resp["error"]))
            return OpenRouterResult("", False, error=f"OpenRouter error: {err_msg}")

        # Extract response text
        choices = resp.get("choices", [])
        if not choices:
            return OpenRouterResult("", False, error="No choices in response")

        text = choices[0].get("message", {}).get("content", "")
        elapsed = time.time() - t0
        cb("done", {"elapsed": elapsed})

        # Parse usage
        usage_data = resp.get("usage", {})
        usage = OpenRouterUsage(
            input_tokens=usage_data.get("prompt_tokens", 0),
            output_tokens=usage_data.get("completion_tokens", 0),
            cost_usd=0.0,  # OpenRouter doesn't always return cost inline
            duration_ms=int(elapsed * 1000),
        )

        # Accumulate into session tracker
        _session_usage.input_tokens += usage.input_tokens
        _session_usage.output_tokens += usage.output_tokens
        _session_usage.cost_usd += usage.cost_usd
        _session_usage.duration_ms += usage.duration_ms

        return OpenRouterResult(text, True, usage)

    except subprocess.TimeoutExpired:
        return OpenRouterResult("", False, error=f"Timed out ({timeout}s)")
    except Exception as e:
        return OpenRouterResult("", False, error=str(e))


def _fmt_tokens(n: int) -> str:
    if n >= 1000:
        return f"{n/1000:.1f}k"
    return str(n)
