from __future__ import annotations
"""Test verifier — runs the real test runner on the files a task touched.

The reviewer can only reason about the diff. It cannot tell whether a
test actually passes when executed. FIX 1 closes that hole by running
the project's own test runner against the tests that correspond to the
touched files, then returning a structured :class:`VerifierResult` that
the CLI can surface back to the reviewer as grounded findings.

Design goals
------------
* **Generic, language-aware.**  Detects pytest, jest/vitest, go test,
  cargo test, and plain ``npm test`` from the touched file extensions
  plus whatever the project happens to ship (``pyproject.toml``,
  ``package.json``, ``go.mod``, ``Cargo.toml``).
* **Scoped by default.**  For each modified source file we try to find
  its companion test file (``tests/test_foo.py``, ``foo_test.py``,
  ``Foo.test.ts`` …) and only run those — full-suite runs are FIX 2's
  job. Files without a companion test are skipped, not failed.
* **Always on.**  Runs unconditionally on every review round. Missing
  test runners degrade to "skipped", never to an error.
* **Never mutates files.**  Read-only subprocess execution inside the
  project directory.
* **Short, quotable failures.**  Captures stdout/stderr and trims to a
  handful of lines so the reviewer prompt stays small.

This module has no runtime dependencies beyond the stdlib and the
standard test runners on ``PATH``.
"""

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ── Data shapes ─────────────────────────────────────────────

@dataclass
class TestFailure:
    """A single failed/errored test case."""
    runner: str                # "pytest" | "jest" | "go" | "cargo" | "npm"
    file: str                  # path (project-relative) of the test file
    name: str                  # test function / describe block
    message: str               # trimmed failure excerpt


@dataclass
class VerifierResult:
    """Aggregated output of a verifier run."""
    ok: bool = True
    ran: bool = False
    runner: str = ""
    tests_executed: int = 0
    failures: list[TestFailure] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)    # runner crashes, not test fails
    duration_ms: int = 0
    stdout_tail: str = ""

    def summary(self) -> str:
        """One-line summary suitable for log output."""
        if not self.ran:
            return "verifier: skipped"
        if self.errors:
            return f"verifier: {self.runner} crashed — {self.errors[0][:80]}"
        if self.failures:
            return (
                f"verifier: {self.runner} — {len(self.failures)} failing "
                f"of {self.tests_executed} executed "
                f"({self.duration_ms/1000:.1f}s)"
            )
        return (
            f"verifier: {self.runner} — {self.tests_executed} passing "
            f"({self.duration_ms/1000:.1f}s)"
        )

    def to_review_findings(self) -> list[dict]:
        """Shape failures the way the reviewer's prompt expects.

        Mirrors the ``issues`` schema used by :mod:`usta.reviewer` so the
        CLI can splice these straight into the review result without
        another round trip.
        """
        findings: list[dict] = []
        for f in self.failures:
            findings.append({
                "severity": "error",
                "file": f.file,
                "line": 0,
                "msg": f"[{f.runner}] {f.name}: {f.message}",
            })
        for e in self.errors:
            findings.append({
                "severity": "error",
                "file": "",
                "line": 0,
                "msg": f"[{self.runner or 'verifier'}] crash: {e}",
            })
        return findings


# ── Env / config helpers ────────────────────────────────────

def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


# ── Companion-test discovery ────────────────────────────────

# Extensions we know how to verify. Anything else is skipped silently.
_SUPPORTED_SRC_EXT = {
    ".py", ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs",
    ".go", ".rs",
}


def _py_companion_tests(project_dir: Path, src: Path) -> list[Path]:
    """Return candidate pytest files for *src*.

    Tries, in order:
      - ``tests/test_<stem>.py`` under each ancestor that contains ``tests/``
      - ``test_<stem>.py`` in the same directory
      - ``<stem>_test.py`` in the same directory
      - ``tests/<stem>_test.py`` under each ancestor
    Only files that actually exist are returned, de-duped while preserving
    order. *src* itself is included if it is already a test file — the
    applier sometimes adds assertions directly in the file it touches.
    """
    stem = src.stem
    hits: list[Path] = []
    seen: set[Path] = set()

    def _add(p: Path) -> None:
        if p.exists() and p.is_file() and p not in seen:
            hits.append(p)
            seen.add(p)

    if stem.startswith("test_") or stem.endswith("_test"):
        _add(src)

    _add(src.with_name(f"test_{stem}.py"))
    _add(src.with_name(f"{stem}_test.py"))

    # Walk up looking for tests/ dirs — handles both flat projects and
    # pyproject-style ``src/<pkg>/foo.py`` layouts.
    for ancestor in [src.parent, *src.parents]:
        try:
            rel = ancestor.resolve().relative_to(project_dir.resolve())
        except ValueError:
            break
        for tdir_name in ("tests", "test"):
            tdir = project_dir / rel / tdir_name if str(rel) != "." else project_dir / tdir_name
            if not tdir.is_dir():
                continue
            _add(tdir / f"test_{stem}.py")
            _add(tdir / f"{stem}_test.py")
        if ancestor == project_dir:
            break
    return hits


def _js_companion_tests(project_dir: Path, src: Path) -> list[Path]:
    """Return candidate jest/vitest files for *src*.

    Tries the conventional variants: ``foo.test.ts``, ``foo.spec.ts``,
    ``__tests__/foo.ts``. Extension is matched to the source extension
    so a .ts source doesn't pull in .js spec by accident, but also
    falls back to the matching .js/.jsx when the .ts variant is absent.
    """
    stem = src.stem
    exts = [src.suffix]
    if src.suffix in (".ts", ".tsx"):
        exts += [".tsx" if src.suffix == ".ts" else ".ts"]
    if src.suffix in (".js", ".jsx", ".mjs", ".cjs"):
        exts += [".test.js", ".spec.js"]

    hits: list[Path] = []
    seen: set[Path] = set()

    def _add(p: Path) -> None:
        if p.exists() and p.is_file() and p not in seen:
            hits.append(p)
            seen.add(p)

    if ".test." in src.name or ".spec." in src.name:
        _add(src)

    for ext in exts:
        _add(src.with_name(f"{stem}.test{ext}"))
        _add(src.with_name(f"{stem}.spec{ext}"))
        _add(src.parent / "__tests__" / f"{stem}{ext}")
        _add(src.parent / "__tests__" / f"{stem}.test{ext}")
        _add(src.parent / "__tests__" / f"{stem}.spec{ext}")
    return hits


def _group_by_runner(
    project_dir: Path, files: list[str]
) -> dict[str, list[Path]]:
    """Bucket touched files into the runners that should handle them.

    The returned dict maps a runner name to a sorted list of absolute
    test paths to execute. Empty runners are omitted.
    """
    runners: dict[str, list[Path]] = {}
    seen: set[Path] = set()

    for rel in files:
        src = (project_dir / rel).resolve() if (project_dir / rel).exists() else (project_dir / rel)
        if src.suffix not in _SUPPORTED_SRC_EXT:
            continue
        if src.suffix == ".py":
            runner = "pytest"
            tests = _py_companion_tests(project_dir, src)
        elif src.suffix in (".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs"):
            runner = _detect_js_runner(project_dir)
            tests = _js_companion_tests(project_dir, src)
        elif src.suffix == ".go":
            runner = "go"
            tests = _go_companion_tests(project_dir, src)
        elif src.suffix == ".rs":
            runner = "cargo"
            tests = []  # cargo test runs the whole package; see _run_cargo
        else:
            continue

        bucket = runners.setdefault(runner, [])
        for t in tests:
            if t not in seen:
                bucket.append(t)
                seen.add(t)
        # Rust: we still need to record that *something* needs running
        # even without a concrete test file.
        if runner == "cargo" and not bucket:
            bucket.append(src)

    for key in list(runners.keys()):
        runners[key] = sorted(set(runners[key]))
        if not runners[key]:
            runners.pop(key)
    return runners


def _go_companion_tests(project_dir: Path, src: Path) -> list[Path]:
    """Return ``*_test.go`` files next to *src*."""
    hits: list[Path] = []
    stem = src.stem
    candidate = src.with_name(f"{stem}_test.go")
    if candidate.exists():
        hits.append(candidate)
    # Also include *any* _test.go in the same package; go test is
    # package-scoped so running a single file is awkward anyway.
    for sibling in src.parent.glob("*_test.go"):
        if sibling not in hits:
            hits.append(sibling)
    return hits


def _detect_js_runner(project_dir: Path) -> str:
    """Prefer vitest if the project lists it, else jest, else npm test."""
    pkg = project_dir / "package.json"
    if not pkg.exists():
        return "jest"
    try:
        data = json.loads(pkg.read_text(errors="replace"))
    except (json.JSONDecodeError, OSError):
        return "jest"
    deps = {}
    deps.update(data.get("dependencies") or {})
    deps.update(data.get("devDependencies") or {})
    if "vitest" in deps:
        return "vitest"
    if "jest" in deps:
        return "jest"
    scripts = (data.get("scripts") or {})
    if "test" in scripts:
        return "npm"
    return "jest"


# ── Runners ─────────────────────────────────────────────────

def _run_pytest(
    project_dir: Path, tests: list[Path], timeout: int
) -> VerifierResult:
    if not shutil.which("pytest") and not shutil.which("python3"):
        return VerifierResult(ok=True, ran=False, runner="pytest",
                              errors=["pytest not installed"])
    rel = [str(p.relative_to(project_dir)) if p.is_absolute() else str(p)
           for p in tests]
    cmd = ["pytest", "-q", "--tb=line",
           "--no-header", "--color=no", *rel]
    if not shutil.which("pytest"):
        cmd = ["python3", "-m", "pytest", *cmd[1:]]

    import time as _t
    t0 = _t.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return VerifierResult(
            ok=False, ran=True, runner="pytest",
            errors=[f"timed out after {timeout}s"],
            duration_ms=int((timeout) * 1000),
        )
    dur = int((_t.time() - t0) * 1000)
    out = (proc.stdout or "") + (proc.stderr or "")

    res = VerifierResult(
        ok=proc.returncode == 0, ran=True, runner="pytest",
        duration_ms=dur, stdout_tail=_tail(out, 4000),
    )
    _parse_pytest(out, rel, res)
    return res


def _parse_pytest(out: str, rel: list[str], res: VerifierResult) -> None:
    """Populate ``res.failures`` / ``res.tests_executed`` from pytest output."""
    # Count summary line. pytest prints several formats depending on the
    # verbosity and whether the run had failures:
    #   "=================== 3 failed, 12 passed in 2.1s ==================="
    #   "1 failed, 2 passed in 0.00s"
    #   "============================== 5 passed in 0.12s =============================="
    # The common denominator is a token "\d+ (failed|passed|…)" followed
    # eventually by " in N.Ns". Match any line containing that pattern.
    sum_re = re.compile(
        r'(\d+)\s+(failed|passed|errors?|skipped|xfailed|xpassed|deselected)'
    )
    counts: dict[str, int] = {}
    for line in out.splitlines():
        stripped = line.strip("= ").strip()
        if " in " not in stripped:
            continue
        if not sum_re.search(stripped):
            continue
        for m in sum_re.finditer(stripped):
            counts[m.group(2)] = int(m.group(1))
        break
    res.tests_executed = (
        counts.get("passed", 0) + counts.get("failed", 0)
        + counts.get("errors", 0) + counts.get("error", 0)
    )

    # FAILED lines look like: "FAILED tests/test_foo.py::test_bar - AssertionError: ..."
    fail_re = re.compile(
        r'^FAILED\s+([^\s:]+)::([^\s]+)\s*(?:-\s*(.+))?$'
    )
    err_re = re.compile(
        r'^ERROR\s+([^\s:]+)::([^\s]+)\s*(?:-\s*(.+))?$'
    )
    for line in out.splitlines():
        m = fail_re.match(line.strip()) or err_re.match(line.strip())
        if not m:
            continue
        res.failures.append(TestFailure(
            runner="pytest",
            file=m.group(1),
            name=m.group(2),
            message=(m.group(3) or "").strip()[:240] or "see test output",
        ))

    # Some pytest configs don't emit the "FAILED" summary — fall back to
    # the short tb "file:line: error" lines.
    if counts.get("failed", 0) and not res.failures:
        tb_re = re.compile(r'^([^\s:]+\.py):(\d+):\s*(.+)$')
        for line in out.splitlines():
            m = tb_re.match(line.strip())
            if not m:
                continue
            res.failures.append(TestFailure(
                runner="pytest",
                file=m.group(1),
                name=f"line {m.group(2)}",
                message=m.group(3)[:240],
            ))
            if len(res.failures) >= counts["failed"]:
                break


def _run_jest_like(
    project_dir: Path, tests: list[Path], timeout: int, runner: str
) -> VerifierResult:
    """Run jest/vitest/npm-test against a handful of test files."""
    rel = [str(p.relative_to(project_dir)) if p.is_absolute() else str(p)
           for p in tests]

    if runner == "jest":
        if shutil.which("jest"):
            cmd = ["jest", "--json", "--colors=false", *rel]
        elif shutil.which("npx"):
            cmd = ["npx", "--no-install", "jest", "--json", "--colors=false", *rel]
        else:
            return VerifierResult(ok=True, ran=False, runner="jest",
                                  errors=["jest not installed"])
    elif runner == "vitest":
        if shutil.which("vitest"):
            cmd = ["vitest", "run", "--reporter=json", *rel]
        elif shutil.which("npx"):
            cmd = ["npx", "--no-install", "vitest", "run", "--reporter=json", *rel]
        else:
            return VerifierResult(ok=True, ran=False, runner="vitest",
                                  errors=["vitest not installed"])
    else:  # npm test — no per-file scoping, one-shot
        if not shutil.which("npm"):
            return VerifierResult(ok=True, ran=False, runner="npm",
                                  errors=["npm not installed"])
        cmd = ["npm", "test", "--silent"]

    import time as _t
    t0 = _t.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return VerifierResult(
            ok=False, ran=True, runner=runner,
            errors=[f"timed out after {timeout}s"],
            duration_ms=int(timeout * 1000),
        )
    dur = int((_t.time() - t0) * 1000)
    out = proc.stdout or ""
    err = proc.stderr or ""

    res = VerifierResult(
        ok=proc.returncode == 0, ran=True, runner=runner,
        duration_ms=dur, stdout_tail=_tail(out + err, 4000),
    )

    # jest/vitest JSON report parse
    if runner in ("jest", "vitest"):
        data = _try_json(out) or _try_json(err)
        if isinstance(data, dict):
            num_total = data.get("numTotalTests")
            if num_total is not None:
                res.tests_executed = num_total
            else:
                results = data.get("testResults") or []
                res.tests_executed = sum(
                    len((r or {}).get("assertionResults", []))
                    for r in results
                )
            for tr in data.get("testResults") or []:
                tfile = tr.get("name") or tr.get("testFilePath") or ""
                for a in tr.get("assertionResults") or []:
                    if a.get("status") == "failed":
                        res.failures.append(TestFailure(
                            runner=runner,
                            file=tfile,
                            name=a.get("fullName") or a.get("title") or "?",
                            message=_tail(
                                "\n".join(a.get("failureMessages") or []),
                                240,
                            ),
                        ))
            return res

    # Fallback: scrape "✕ test name" / "FAIL file" lines
    fail_re = re.compile(r'^(?:\s*[✕×]|FAIL)\s+(.+)$')
    for line in (out + "\n" + err).splitlines():
        m = fail_re.match(line)
        if m:
            res.failures.append(TestFailure(
                runner=runner, file="", name=m.group(1).strip()[:200],
                message="see test output",
            ))
    return res


def _run_go(
    project_dir: Path, tests: list[Path], timeout: int
) -> VerifierResult:
    if not shutil.which("go"):
        return VerifierResult(ok=True, ran=False, runner="go",
                              errors=["go not installed"])
    # Scope to unique parent dirs (go test is package-scoped).
    pkgs = sorted({
        "./" + str(p.parent.relative_to(project_dir))
        if p.is_absolute() else "./" + str(p.parent)
        for p in tests
    }) or ["./..."]
    cmd = ["go", "test", "-json", "-count=1", *pkgs]

    import time as _t
    t0 = _t.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return VerifierResult(
            ok=False, ran=True, runner="go",
            errors=[f"timed out after {timeout}s"],
            duration_ms=int(timeout * 1000),
        )
    dur = int((_t.time() - t0) * 1000)
    res = VerifierResult(
        ok=proc.returncode == 0, ran=True, runner="go",
        duration_ms=dur, stdout_tail=_tail(proc.stdout or "", 4000),
    )
    # Stream of JSON lines: {"Action":"fail"|"pass","Package":...,"Test":...}
    for line in (proc.stdout or "").splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("Action") == "run" and ev.get("Test"):
            res.tests_executed += 1
        if ev.get("Action") == "fail" and ev.get("Test"):
            res.failures.append(TestFailure(
                runner="go",
                file=ev.get("Package", ""),
                name=ev.get("Test", "?"),
                message=_tail(ev.get("Output", "") or "", 240),
            ))
    return res


def _run_cargo(
    project_dir: Path, tests: list[Path], timeout: int
) -> VerifierResult:
    if not shutil.which("cargo"):
        return VerifierResult(ok=True, ran=False, runner="cargo",
                              errors=["cargo not installed"])
    cmd = ["cargo", "test", "--quiet", "--no-fail-fast"]
    import time as _t
    t0 = _t.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(project_dir), capture_output=True, text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return VerifierResult(
            ok=False, ran=True, runner="cargo",
            errors=[f"timed out after {timeout}s"],
            duration_ms=int(timeout * 1000),
        )
    dur = int((_t.time() - t0) * 1000)
    out = (proc.stdout or "") + (proc.stderr or "")
    res = VerifierResult(
        ok=proc.returncode == 0, ran=True, runner="cargo",
        duration_ms=dur, stdout_tail=_tail(out, 4000),
    )
    # "test result: FAILED. 3 passed; 2 failed; ..."
    m = re.search(r'test result:.*?(\d+)\s+passed;\s*(\d+)\s+failed', out)
    if m:
        res.tests_executed = int(m.group(1)) + int(m.group(2))
    for line in out.splitlines():
        m2 = re.match(r'^test\s+([^\s]+)\s+\.\.\.\s+FAILED\s*$', line)
        if m2:
            res.failures.append(TestFailure(
                runner="cargo", file="", name=m2.group(1),
                message="see cargo output",
            ))
    return res


# ── Public entry point ─────────────────────────────────────

def verify_files(
    files: list[str],
    project_dir: Path,
    timeout: Optional[int] = None,
) -> VerifierResult:
    """Run the appropriate test runners for *files*.

    Returns an aggregated :class:`VerifierResult`. When multiple runners
    apply (e.g. a task touched both Python and TypeScript), failures
    from every runner are merged and ``runner`` is set to a
    ``"+"``-separated label.
    """
    timeout = timeout or _env_int("USTA_VERIFIER_TIMEOUT", 180)

    if not files:
        return VerifierResult(ok=True, ran=False, runner="",
                              errors=["no files to verify"])

    groups = _group_by_runner(project_dir, files)
    if not groups:
        return VerifierResult(ok=True, ran=False, runner="",
                              errors=["no supported source files"])

    merged = VerifierResult(ok=True, ran=False, runner="")
    labels: list[str] = []
    for runner, tests in groups.items():
        # Skip runners with zero companion tests (other than cargo which
        # runs the whole package).
        if not tests and runner != "cargo":
            continue
        if runner == "pytest":
            sub = _run_pytest(project_dir, tests, timeout)
        elif runner in ("jest", "vitest", "npm"):
            sub = _run_jest_like(project_dir, tests, timeout, runner)
        elif runner == "go":
            sub = _run_go(project_dir, tests, timeout)
        elif runner == "cargo":
            sub = _run_cargo(project_dir, tests, timeout)
        else:
            continue

        if sub.ran:
            merged.ran = True
            labels.append(sub.runner)
            merged.tests_executed += sub.tests_executed
            merged.failures.extend(sub.failures)
            merged.errors.extend(sub.errors)
            merged.duration_ms += sub.duration_ms
            if not sub.ok:
                merged.ok = False
            # Keep the last runner's tail; good enough for a single
            # quoted block in the review prompt.
            if sub.stdout_tail:
                merged.stdout_tail = sub.stdout_tail

    merged.runner = "+".join(labels) if labels else ""
    return merged


# ── Helpers ─────────────────────────────────────────────────

def _tail(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return "…" + text[-max_chars:]


def _try_json(text: str):
    text = text.strip()
    if not text:
        return None
    # jest --json can emit multi-line progress before the JSON blob. Find
    # the first '{' after the last newline-separated progress marker.
    idx = text.find("{")
    if idx < 0:
        return None
    try:
        return json.loads(text[idx:])
    except json.JSONDecodeError:
        return None
