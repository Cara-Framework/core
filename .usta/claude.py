from __future__ import annotations
"""Claude CLI wrapper — subscription, zero cost, with usage tracking and session reuse."""

import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional, Callable
from dataclasses import dataclass, field

from .config import Cfg


@dataclass
class ClaudeUsage:
    """Tracks Claude subscription usage."""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read: int = 0
    cache_create: int = 0
    cost_usd: float = 0.0
    duration_ms: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def format(self) -> str:
        inp = _fmt_tokens(self.input_tokens + self.cache_read + self.cache_create)
        out = _fmt_tokens(self.output_tokens)
        return f"{inp} in / {out} out, ${self.cost_usd:.4f}, {self.duration_ms/1000:.1f}s"


_session_usage = ClaudeUsage()

def get_session_usage() -> ClaudeUsage:
    return _session_usage

def reset_session_usage():
    global _session_usage
    _session_usage = ClaudeUsage()


@dataclass
class ClaudeResult:
    text: str
    ok: bool
    usage: ClaudeUsage = field(default_factory=ClaudeUsage)
    error: Optional[str] = None
    session_id: Optional[str] = None


def ask(
    prompt: str,
    cfg: Cfg,
    model: Optional[str] = None,
    system_prompt: Optional[str] = None,
    cwd: Optional[Path] = None,
    timeout: int = 600,
    max_turns: Optional[int] = None,
    on_event: Optional[Callable[[str, dict], None]] = None,
    session_id: Optional[str] = None,
    resume: bool = False,
    mcp_config: Optional[str] = None,
    allowed_tools: Optional[list[str]] = None,
) -> ClaudeResult:
    """Run claude -p with streaming JSON output.

    Args:
        session_id: UUID to use for this session (enables continuation).
        resume: If True, resume an existing session (uses --resume instead of --session-id).
        mcp_config: Path to an MCP server config JSON (enables extra tools
            such as Slack, SSH, gcloud, etc. for agent mode).
        allowed_tools: Optional whitelist of tool names to allow
            (passed via ``--allowedTools``). ``None`` means "let claude
            decide from the MCP config".
    """
    model = model or cfg.claude_model
    cwd = cwd or cfg.project_dir
    cb = on_event or (lambda *a: None)

    cmd = [
        cfg.claude_bin, "-p",
        "--model", model,
        "--permission-mode", "bypassPermissions",
        "--output-format", "stream-json",
        "--verbose",
    ]

    if mcp_config:
        cmd.extend(["--mcp-config", mcp_config])
    if allowed_tools:
        cmd.extend(["--allowedTools", ",".join(allowed_tools)])

    # Session handling: resume existing or start new with ID
    if resume and session_id:
        cmd.extend(["--resume", session_id])
    elif session_id:
        cmd.extend(["--session-id", session_id])

    if max_turns is not None:
        cmd.extend(["--max-turns", str(max_turns)])
    if system_prompt and not resume:
        # System prompt only on first call (resume inherits it)
        cmd.extend(["--system-prompt", system_prompt])
    cmd.append(prompt)

    t0 = time.time()
    full_text = ""
    result_session_id = session_id

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=str(cwd),
            stdin=subprocess.DEVNULL,
        )
        cb("start", {})

        # Watchdog: kill proc if no output for `timeout` seconds
        _last_activity = [time.time()]
        def _watchdog():
            while proc.poll() is None:
                if time.time() - _last_activity[0] > timeout:
                    try:
                        proc.kill()
                    except OSError:
                        pass
                    break
                time.sleep(5)
        _wd = threading.Thread(target=_watchdog, daemon=True)
        _wd.start()

        for line in proc.stdout:
            _last_activity[0] = time.time()
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = evt.get("type", "")

            if etype == "system" and evt.get("subtype") == "init":
                # Capture session_id from init event
                if evt.get("session_id"):
                    result_session_id = evt["session_id"]
                continue

            elif etype == "assistant":
                msg = evt.get("message", {})
                content_blocks = msg.get("content", [])
                for block in content_blocks:
                    btype = block.get("type", "")
                    if btype == "tool_use":
                        tool_name = block.get("name", "?")
                        tool_input = block.get("input", {})
                        cb("tool", {"tool": tool_name, "input": tool_input, "elapsed": time.time() - t0})
                    elif btype == "text":
                        text = block.get("text", "")
                        if text:
                            full_text = text
                            cb("text", {"text": text, "elapsed": time.time() - t0})

            elif etype == "result":
                result_text = evt.get("result", full_text)
                if result_text:
                    full_text = result_text
                elapsed = time.time() - t0
                cb("done", {"elapsed": elapsed})

                # Capture session_id from result
                if evt.get("session_id"):
                    result_session_id = evt["session_id"]

                if evt.get("is_error"):
                    return ClaudeResult("", False, error=full_text or "Unknown error",
                                       session_id=result_session_id)

                usage_data = evt.get("usage", {})
                usage = ClaudeUsage(
                    input_tokens=usage_data.get("input_tokens", 0),
                    output_tokens=usage_data.get("output_tokens", 0),
                    cache_read=usage_data.get("cache_read_input_tokens", 0),
                    cache_create=usage_data.get("cache_creation_input_tokens", 0),
                    cost_usd=evt.get("total_cost_usd", 0.0),
                    duration_ms=evt.get("duration_ms", 0),
                )

                _session_usage.input_tokens += usage.input_tokens
                _session_usage.output_tokens += usage.output_tokens
                _session_usage.cache_read += usage.cache_read
                _session_usage.cache_create += usage.cache_create
                _session_usage.cost_usd += usage.cost_usd
                _session_usage.duration_ms += usage.duration_ms

                return ClaudeResult(full_text, True, usage, session_id=result_session_id)

        proc.wait()
        stderr = proc.stderr.read().strip() if proc.stderr else ""
        elapsed = time.time() - t0

        if proc.returncode != 0:
            return ClaudeResult("", False, error=stderr or f"exit {proc.returncode}",
                               session_id=result_session_id)
        if full_text:
            return ClaudeResult(full_text, True, session_id=result_session_id)
        return ClaudeResult("", False, error=stderr or "Empty response",
                           session_id=result_session_id)

    except subprocess.TimeoutExpired:
        proc.kill()
        return ClaudeResult("", False, error=f"Timed out ({timeout}s)")
    except FileNotFoundError:
        return ClaudeResult("", False, error=f"claude not found: {cfg.claude_bin}")


def _fmt_tokens(n: int) -> str:
    if n >= 1000:
        return f"{n/1000:.1f}k"
    return str(n)
