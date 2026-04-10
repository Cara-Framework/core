from __future__ import annotations
"""Static checks — cheap, language-aware lint on the files a task touched.

FIX 11: the reviewer is great at logic but terrible at catching the kind
of boring bugs a linter spots in milliseconds — unused imports, undefined
names, `print(x` missing a closing paren, TypeScript files that stopped
compiling, etc. This module runs the project's static checker (when one
is available) on each touched file and returns findings in the same
shape :mod:`usta.verifier` emits, so the CLI can merge them into the
review result with no extra plumbing.

Supported checkers
------------------
* Python  → ``pyflakes`` (preferred) or ``python3 -m compile_all`` fallback
* TS/JS   → ``tsc --noEmit`` when ``tsconfig.json`` is present,
            else ``node --check`` for plain JS
* Go      → ``go vet ./...`` scoped to touched packages

Static checks run on every review round and degrade quietly if the
binary isn't on ``PATH`` — an absent checker is NEVER an error, just
"skipped".
"""

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ── Data shapes ─────────────────────────────────────────────

@dataclass
class StaticFinding:
    """A single lint / compile finding."""
    checker: str            # "pyflakes" | "tsc" | "node" | "govet" | "compile"
    file: str               # project-relative path
    line: int               # 0 when unknown
    message: str            # trimmed detail


@dataclass
class StaticResult:
    """Aggregated output of a static-check run."""
    ok: bool = True
    ran: bool = False
    checker: str = ""
    duration_ms: int = 0
    findings: list[StaticFinding] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)   # checker crashes
    stdout_tail: str = ""

    def summary(self) -> str:
        if not self.ran:
            return "static: skipped"
        if self.errors:
            return f"static: {self.checker} crashed — {self.errors[0][:80]}"
        if self.findings:
            return (
                f"static: {self.checker} — {len(self.findings)} finding(s) "
                f"({self.duration_ms/1000:.1f}s)"
            )
        return f"static: {self.checker} — clean ({self.duration_ms/1000:.1f}s)"

    def to_review_findings(self) -> list[dict]:
        out = []
        for f in self.findings:
            out.append({
                "severity": "warning",
                "file": f.file,
                "line": f.line,
                "msg": f"[{f.checker}] {f.message}",
            })
        for e in self.errors:
            out.append({
                "severity": "warning",
                "file": "",
                "line": 0,
                "msg": f"[{self.checker or 'static'}] crash: {e}",
            })
        return out


# ── Bucketing ───────────────────────────────────────────────

_PY_EXT = {".py"}
_TS_EXT = {".ts", ".tsx", ".mts", ".cts"}
_JS_EXT = {".js", ".jsx", ".mjs", ".cjs"}
_GO_EXT = {".go"}


def _bucket(project_dir: Path, files: list[str]) -> dict[str, list[Path]]:
    """Group *files* by language bucket. Missing files are dropped."""
    groups: dict[str, list[Path]] = {}
    for rel in files:
        p = project_dir / rel
        if not p.exists() or not p.is_file():
            continue
        if p.suffix in _PY_EXT:
            groups.setdefault("py", []).append(p)
        elif p.suffix in _TS_EXT:
            groups.setdefault("ts", []).append(p)
        elif p.suffix in _JS_EXT:
            groups.setdefault("js", []).append(p)
        elif p.suffix in _GO_EXT:
            groups.setdefault("go", []).append(p)
    return groups


# ── Python ──────────────────────────────────────────────────

_PYFLAKES_RE = re.compile(r'^([^:]+):(\d+):(?:\d+:)?\s*(.+)$')


def _check_python(project_dir: Path, files: list[Path], timeout: int) -> StaticResult:
    # Prefer pyflakes; fall back to py_compile for a basic syntax check.
    use_pyflakes = bool(shutil.which("pyflakes") or _have_module("pyflakes"))
    if use_pyflakes:
        if shutil.which("pyflakes"):
            cmd = ["pyflakes", *[str(f) for f in files]]
        else:
            cmd = ["python3", "-m", "pyflakes", *[str(f) for f in files]]
        checker = "pyflakes"
    else:
        # Syntax-only fallback.
        cmd = ["python3", "-m", "py_compile", *[str(f) for f in files]]
        checker = "compile"

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return StaticResult(
            ok=False, ran=True, checker=checker,
            errors=[f"timed out after {timeout}s"],
            duration_ms=int(timeout * 1000),
        )
    dur = int((time.time() - t0) * 1000)

    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    res = StaticResult(
        ok=proc.returncode == 0, ran=True, checker=checker,
        duration_ms=dur, stdout_tail=_tail(out, 4000),
    )

    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        m = _PYFLAKES_RE.match(line)
        if m:
            file = m.group(1)
            try:
                rel = str(Path(file).resolve().relative_to(project_dir.resolve()))
            except ValueError:
                rel = file
            res.findings.append(StaticFinding(
                checker=checker, file=rel,
                line=int(m.group(2)), message=m.group(3)[:240],
            ))
    return res


def _have_module(name: str) -> bool:
    try:
        subprocess.run(
            ["python3", "-c", f"import {name}"],
            capture_output=True, timeout=5,
        ).check_returncode()
        return True
    except Exception:
        return False


# ── TypeScript / JavaScript ─────────────────────────────────

_TSC_RE = re.compile(r'^([^(]+)\((\d+),(\d+)\):\s*(error|warning)\s+TS\d+:\s*(.+)$')


def _check_ts(project_dir: Path, files: list[Path], timeout: int) -> StaticResult:
    """Run ``tsc --noEmit`` when tsconfig exists, else fall back to node --check.

    We don't scope tsc to individual files because tsc with a tsconfig
    project uses the whole program — passing stray files confuses it.
    Instead we run the project build once and filter findings to those
    that originate in the touched files.
    """
    tsconfig = _find_ancestor(project_dir, "tsconfig.json")
    if tsconfig and (shutil.which("tsc") or shutil.which("npx")):
        cmd = (
            ["tsc", "--noEmit", "--pretty", "false", "-p", str(tsconfig.parent)]
            if shutil.which("tsc")
            else ["npx", "--no-install", "tsc", "--noEmit", "--pretty", "false",
                  "-p", str(tsconfig.parent)]
        )
        checker = "tsc"
    elif shutil.which("node"):
        # No tsconfig → treat inputs as plain JS and run node --check per file.
        return _check_js_fast(project_dir, files, timeout)
    else:
        return StaticResult(ok=True, ran=False, checker="tsc",
                            errors=["no tsc / tsconfig available"])

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return StaticResult(
            ok=False, ran=True, checker=checker,
            errors=[f"timed out after {timeout}s"],
            duration_ms=int(timeout * 1000),
        )
    dur = int((time.time() - t0) * 1000)

    # Resolved absolute paths of the files we care about.
    target_paths = set()
    for f in files:
        try:
            target_paths.add(str(f.resolve()))
        except OSError:
            target_paths.add(str(f))

    res = StaticResult(
        ok=proc.returncode == 0, ran=True, checker=checker,
        duration_ms=dur,
        stdout_tail=_tail((proc.stdout or "") + (proc.stderr or ""), 4000),
    )

    for line in (proc.stdout or "").splitlines():
        m = _TSC_RE.match(line.strip())
        if not m:
            continue
        file = m.group(1).strip()
        try:
            abs_ = str((project_dir / file).resolve())
        except OSError:
            abs_ = file
        # Scope: only findings in the touched set.
        if target_paths and abs_ not in target_paths:
            continue
        res.findings.append(StaticFinding(
            checker=checker,
            file=file,
            line=int(m.group(2)),
            message=f"TS{m.group(4)}: {m.group(5)[:200]}",
        ))
    # tsc exits non-zero whenever there's an error; but we consider the
    # run "ok" if none of the errors fall in the touched files — the
    # other instance's in-flight work shouldn't fail our review.
    res.ok = not res.findings
    return res


def _check_js_fast(project_dir: Path, files: list[Path], timeout: int) -> StaticResult:
    """Per-file ``node --check`` when we have no tsconfig."""
    res = StaticResult(ok=True, ran=True, checker="node")
    t0 = time.time()
    for f in files:
        try:
            proc = subprocess.run(
                ["node", "--check", str(f)],
                cwd=str(project_dir), capture_output=True, text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            res.errors.append(f"timeout checking {f.name}")
            continue
        if proc.returncode != 0:
            res.ok = False
            first = (proc.stderr or "").splitlines()[:2]
            res.findings.append(StaticFinding(
                checker="node",
                file=str(f.relative_to(project_dir))
                    if f.is_absolute() else str(f),
                line=0,
                message=" ".join(first)[:240],
            ))
    res.duration_ms = int((time.time() - t0) * 1000)
    return res


# ── Go ──────────────────────────────────────────────────────

def _check_go(project_dir: Path, files: list[Path], timeout: int) -> StaticResult:
    if not shutil.which("go"):
        return StaticResult(ok=True, ran=False, checker="govet",
                            errors=["go not installed"])
    pkgs = sorted({
        "./" + str(p.parent.relative_to(project_dir))
        if p.is_absolute() else "./" + str(p.parent)
        for p in files
    })
    cmd = ["go", "vet", *pkgs]
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return StaticResult(
            ok=False, ran=True, checker="govet",
            errors=[f"timed out after {timeout}s"],
            duration_ms=int(timeout * 1000),
        )
    dur = int((time.time() - t0) * 1000)
    out = (proc.stderr or "") + (proc.stdout or "")
    res = StaticResult(
        ok=proc.returncode == 0, ran=True, checker="govet",
        duration_ms=dur, stdout_tail=_tail(out, 4000),
    )
    # go vet lines: "file.go:12:2: something bad"
    for line in out.splitlines():
        m = re.match(r'^([^:]+):(\d+):\d+:\s*(.+)$', line.strip())
        if m:
            res.findings.append(StaticFinding(
                checker="govet",
                file=m.group(1),
                line=int(m.group(2)),
                message=m.group(3)[:240],
            ))
    return res


# ── Public entry point ─────────────────────────────────────

def check_files(
    files: list[str],
    project_dir: Path,
    timeout: Optional[int] = None,
) -> StaticResult:
    """Run every applicable static checker for *files*.

    Always returns a :class:`StaticResult`. When multiple checkers run
    (mixed Python + TypeScript task) findings are merged and ``checker``
    becomes a ``"+"``-joined label.
    """
    timeout = timeout or _env_int("USTA_STATIC_TIMEOUT", 90)
    if not files:
        return StaticResult(ok=True, ran=False, checker="",
                            errors=["no files"])

    groups = _bucket(project_dir, files)
    if not groups:
        return StaticResult(ok=True, ran=False, checker="",
                            errors=["no supported source files"])

    merged = StaticResult(ok=True, ran=False, checker="")
    labels: list[str] = []

    for lang, paths in groups.items():
        if lang == "py":
            sub = _check_python(project_dir, paths, timeout)
        elif lang == "ts":
            sub = _check_ts(project_dir, paths, timeout)
        elif lang == "js":
            sub = _check_js_fast(project_dir, paths, timeout)
        elif lang == "go":
            sub = _check_go(project_dir, paths, timeout)
        else:
            continue
        if sub.ran:
            merged.ran = True
            labels.append(sub.checker)
            merged.findings.extend(sub.findings)
            merged.errors.extend(sub.errors)
            merged.duration_ms += sub.duration_ms
            if not sub.ok:
                merged.ok = False
            if sub.stdout_tail:
                merged.stdout_tail = sub.stdout_tail

    merged.checker = "+".join(labels) if labels else ""
    return merged


# ── Helpers ─────────────────────────────────────────────────

def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


def _tail(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return "…" + text[-max_chars:]


def _find_ancestor(start: Path, name: str) -> Optional[Path]:
    """Return the nearest ancestor of *start* that contains *name*."""
    cur = start.resolve() if start.exists() else start
    for p in [cur, *cur.parents]:
        candidate = p / name
        if candidate.exists():
            return candidate
    return None
