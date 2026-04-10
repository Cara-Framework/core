from __future__ import annotations
"""Zero-config — auto-detects everything, just works."""

import json
import os
import shutil
import uuid
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Tuple

TASKS_DIR = ".usta/tasks"
SESSION_FILE = ".usta/session.json"



# Default model pair — planner:applier
DEFAULT_MODEL_PAIR = "local/opus:openrouter/qwen/qwen3-coder"

def parse_model_pair(pair: str) -> Tuple[dict, dict]:
    """Parse 'planner_spec:applier_spec' into two model dicts.

    Each spec is 'provider/model_path' where provider is:
      - local   → use local CLI (claude for opus, qwen for qwen models)
      - openrouter → use OpenRouter API

    Returns (planner_cfg, applier_cfg) each with keys:
      provider: "local" | "openrouter"
      model: the model name/path after provider
      full: the original spec string

    Examples:
      "local/opus:openrouter/qwen/qwen3-coder"
        → planner = {provider: "local", model: "opus"}
        → applier = {provider: "openrouter", model: "qwen/qwen3-coder"}

      "openrouter/qwen/qwen3-coder:openrouter/qwen/qwen3-coder"
        → both use OpenRouter Qwen
    """
    parts = pair.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"Model pair must be 'planner:applier', got: {pair}")

    def _parse_spec(spec: str) -> dict:
        spec = spec.strip()
        if "/" not in spec:
            raise ValueError(
                f"Model spec must be 'provider/model', got: {spec!r}. "
                f"Example: local/opus or openrouter/qwen/qwen3-coder"
            )
        slash_idx = spec.index("/")
        provider = spec[:slash_idx]
        model = spec[slash_idx + 1:]
        if provider not in ("local", "openrouter"):
            raise ValueError(f"Unknown provider '{provider}'. Use 'local' or 'openrouter'.")
        return {"provider": provider, "model": model, "full": spec}

    return _parse_spec(parts[0]), _parse_spec(parts[1])

@dataclass
class Cfg:
    project_dir: Path = field(default_factory=Path.cwd)

    # Auto-detected binaries
    claude_bin: str = ""
    aider_bin: str = ""

    # Model pair (planner:applier)
    model_pair: str = DEFAULT_MODEL_PAIR
    planner_cfg: dict = field(default_factory=dict)
    applier_cfg: dict = field(default_factory=dict)

    # Claude model shorthand (derived from planner_cfg)
    claude_model: str = "opus"

    # Executor selection: "qwen" (default, OpenRouter chat completions) or "aider"
    executor: str = "qwen"
    # Aider-specific model (only used when executor == "aider")
    aider_model: str = "openrouter/qwen/qwen3-coder"

    # OpenRouter
    openrouter_api_key: str = ""

    # Auto mode (--auto, overnight unattended)
    auto_mode: bool = False

    # Review
    auto_review: bool = True
    auto_fix: bool = True
    max_fix_rounds: int = 2

    @property
    def tasks_dir(self) -> Path:
        return self.project_dir / TASKS_DIR



    # ── Session management ────────────────────────────────

    @property
    def _session_path(self) -> Path:
        return self.project_dir / SESSION_FILE

    def get_session_id(self) -> Optional[str]:
        p = self._session_path
        if p.exists():
            try:
                data = json.loads(p.read_text())
                return data.get("session_id")
            except (json.JSONDecodeError, OSError):
                pass
        return None

    def save_session_id(self, session_id: str) -> None:
        p = self._session_path
        p.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        if p.exists():
            try:
                data = json.loads(p.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        data["session_id"] = session_id
        p.write_text(json.dumps(data, indent=2))

    def new_session_id(self) -> str:
        sid = str(uuid.uuid4())
        self.save_session_id(sid)
        return sid

    def clear_session(self) -> None:
        p = self._session_path
        if p.exists():
            try:
                data = json.loads(p.read_text())
                data.pop("session_id", None)
                p.write_text(json.dumps(data, indent=2))
            except (json.JSONDecodeError, OSError):
                pass

def detect(project_dir: Optional[Path] = None,
           model_pair: Optional[str] = None,
           auto_mode: bool = False,
           executor: Optional[str] = None,
           aider_model: Optional[str] = None) -> Cfg:
    """Auto-detect everything. No config file needed."""
    cfg = Cfg(project_dir=project_dir or Path.cwd())

    # Parse model pair
    pair = model_pair or os.environ.get("USTA_MODEL", DEFAULT_MODEL_PAIR)
    cfg.model_pair = pair
    cfg.planner_cfg, cfg.applier_cfg = parse_model_pair(pair)

    if cfg.planner_cfg["provider"] == "local":
        cfg.claude_model = cfg.planner_cfg["model"]  # e.g. "opus"

    cfg.auto_mode = auto_mode

    # OpenRouter API key — try env, then common rc files
    cfg.openrouter_api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not cfg.openrouter_api_key:
        for rc in [Path.home() / ".zshrc", Path.home() / ".bashrc", cfg.project_dir / ".env"]:
            if rc.exists():
                try:
                    for line in rc.read_text().splitlines():
                        line = line.strip()
                        if line.startswith("export OPENROUTER_API_KEY="):
                            val = line.split("=", 1)[1].strip().strip('"').strip("'")
                            if val:
                                cfg.openrouter_api_key = val
                                os.environ["OPENROUTER_API_KEY"] = val
                                break
                except OSError:
                    pass
            if cfg.openrouter_api_key:
                break

    cfg.claude_bin = shutil.which("claude") or "claude"
    cfg.aider_bin = shutil.which("aider") or ""

    # Executor: explicit arg > env > default "qwen"
    cfg.executor = (
        (executor or os.environ.get("USTA_EXECUTOR") or "qwen").lower().strip()
    )
    if cfg.executor not in ("qwen", "aider"):
        raise ValueError(
            f"Unknown executor {cfg.executor!r}. Use 'qwen' or 'aider'."
        )

    # Aider model: explicit arg > env > default
    cfg.aider_model = (
        aider_model
        or os.environ.get("USTA_AIDER_MODEL")
        or "openrouter/qwen/qwen3-coder"
    )

    return cfg