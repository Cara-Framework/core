from __future__ import annotations
"""CLI — zero config, just run."""

import os
import re
import time
import threading
from pathlib import Path
from typing import Optional

import click
from rich.text import Text

from .config import detect, Cfg
from .tasks import (
    Task, load_tasks, load_pending_tasks, save_task, today_dir,
    mark_done, is_finished, get_finished_ids,
)
from .claude import reset_session_usage
from .tui import (
    Dashboard, print_banner, print_summary, print_usage_bar,
    select_tasks, print_review, console,
)
from .verifier import verify_files, VerifierResult
from .static_checks import check_files as static_check_files, StaticResult
from .lessons import harvest_from_review, record_lessons


def _cfg(ctx_obj: dict) -> Cfg:
    return detect(
        project_dir=Path(ctx_obj["dir"]) if ctx_obj.get("dir") else Path.cwd(),
        model_pair=ctx_obj.get("model"),
        auto_mode=ctx_obj.get("auto", False),
        executor=ctx_obj.get("executor"),
        aider_model=ctx_obj.get("aider_model"),
    )


@click.group()
@click.option("-d", "--dir", "project_dir", default=None, help="Project dir (default: cwd)")
@click.option("-m", "--model", "model_pair", default=None,
              help="Model pair planner:applier (e.g. local/opus:openrouter/qwen/qwen3-coder)")
@click.option("--executor", type=click.Choice(["qwen", "aider"]), default=None,
              help="Applier executor. Default: qwen (direct OpenRouter chat completions).")
@click.option("--aider-model", "aider_model", default=None,
              help="Model used when --executor aider (default: openrouter/qwen/qwen3-coder)")
@click.option("--auto", is_flag=True, default=False,
              help="Unattended overnight mode — auto-approve everything")
@click.version_option("1.0.0", prog_name="usta")
@click.pass_context
def main(ctx, project_dir, model_pair, executor, aider_model, auto):
    """usta — AI Development Orchestrator

    \b
    Opus plans · Qwen applies · Opus reviews.
    Uses Claude subscription — zero extra cost for planning.

    \b
    Quick start:
      cd your-project
      usta plan "Add user authentication"
      usta go

    \b
    Model pair:
      usta --model local/opus:openrouter/qwen/qwen3-coder go
    """
    ctx.ensure_object(dict)
    ctx.obj["dir"] = project_dir
    ctx.obj["model"] = model_pair
    ctx.obj["auto"] = auto
    ctx.obj["executor"] = executor
    ctx.obj["aider_model"] = aider_model


# ── plan ──────────────────────────────────────────────────

@main.command()
@click.argument("objective")
@click.option("-f", "--file", "files", multiple=True, help="Extra context files")
@click.pass_context
def plan(ctx, objective, files):
    """Generate tasks from an objective (Opus, $0)."""
    print_banner()
    reset_session_usage()
    cfg = _cfg(ctx.obj)
    from .planner import plan as do_plan

    console.print(f"[bold blue]Objective:[/bold blue] {objective}")
    console.print(f"[dim]Project:[/dim]   {cfg.project_dir}")
    # Show session status
    existing = cfg.get_session_id()
    if existing:
        console.print(f"[dim]Session:[/dim]   [green]♻ Continuing[/green] [dim](project familiar, no re-exploration)[/dim]")
    else:
        console.print(f"[dim]Session:[/dim]   [yellow]★ New[/yellow] [dim](first time — will explore codebase)[/dim]")
    console.print()

    t0 = time.time()
    text_chunks = 0

    # Track tool groups for clean output
    _tool_group = {"name": "", "count": 0, "last_detail": "", "last_ts": ""}

    def _flush_group():
        """Print accumulated tool group as one clean line."""
        g = _tool_group
        if g["name"] and g["count"] > 0:
            count_str = f" ×{g['count']}" if g["count"] > 1 else ""
            console.print(f"  {g['last_ts']}  [cyan]⠸[/cyan] {g['name']}{count_str}")
        g["name"] = ""
        g["count"] = 0
        g["last_detail"] = ""

    def _humanize_tool(tool: str, inp: dict) -> str:
        """Turn tool call into a human-readable description."""
        if tool in ("Read", "read_file"):
            fp = inp.get("file_path", inp.get("path", ""))
            if fp:
                name = fp.split("/")[-1]
                return f"Reading {name}"
            return "Reading file"
        elif tool in ("Grep", "grep"):
            pat = inp.get("pattern", "")[:30]
            return f"Searching: {pat}" if pat else "Searching codebase"
        elif tool in ("Bash", "bash"):
            cmd = inp.get("command", "")
            if cmd.startswith("find"):
                return "Scanning directory structure"
            elif cmd.startswith("ls"):
                return "Listing files"
            elif cmd.startswith("wc"):
                return "Counting lines"
            elif cmd.startswith("grep"):
                return "Searching patterns"
            return f"Running command"
        elif tool in ("Glob", "glob"):
            return f"Finding files: {inp.get('pattern', '')}"
        elif tool == "Agent":
            return inp.get("description", "Delegating subtask")[:50]
        return tool

    def _on_event(etype, data):
        nonlocal text_chunks
        elapsed = time.time() - t0
        ts = f"[dim]{elapsed:.0f}s[/dim]"

        if etype == "start":
            if existing:
                console.print(f"  {ts}  [cyan]⠋[/cyan] Continuing session...")
            else:
                console.print(f"  {ts}  [cyan]⠋[/cyan] Exploring codebase...")

        elif etype == "tool":
            tool = data.get("tool", "?")
            inp = data.get("input", {})
            g = _tool_group

            # Group consecutive same-type tools
            base_tool = tool.split("_")[0] if "_" in tool else tool
            if base_tool == g["name"]:
                g["count"] += 1
                g["last_ts"] = ts
            else:
                _flush_group()
                g["name"] = base_tool
                g["count"] = 1
                g["last_ts"] = ts

            # For non-Read tools or first of a group, show humanized detail
            if base_tool != "Read" or g["count"] == 1:
                desc = _humanize_tool(tool, inp)
                if desc != g.get("last_detail"):
                    g["last_detail"] = desc

        elif etype == "text":
            _flush_group()
            text_chunks += 1
            text = data.get("text", "")
            if text_chunks == 1:
                console.print(f"  {ts}  [cyan]⠼[/cyan] Generating tasks...")
            elif text_chunks % 20 == 0:
                console.print(f"  {ts}  [cyan]⠧[/cyan] Still writing... [dim]({text_chunks} chunks)[/dim]")

        elif etype == "done":
            _flush_group()
            console.print(f"  {ts}  [green]✓[/green] Done")

    console.print("[bold]Planning with Opus...[/bold]\n")
    ctx_files = [Path(f) for f in files] if files else None
    tasks, result, is_new = do_plan(objective, cfg, ctx_files, on_event=_on_event)
    console.print()

    if not tasks:
        if result and result.error:
            console.print(f"[red bold]Error:[/red bold] [red]{result.error}[/red]")
        elif result and result.text:
            console.print(f"[yellow]Opus responded but no tasks were parsed.[/yellow]")
            console.print(f"[dim]Response preview:[/dim]\n{result.text[:500]}")
        else:
            console.print("[red]No tasks generated — empty response.[/red]")
        print_usage_bar("Opus usage")
        return

    console.print(f"[green bold]{len(tasks)} tasks generated:[/green bold]\n")
    for t in tasks:
        console.print(f"  [cyan]{t.id}[/cyan]. {t.title} [dim]({len(t.files)} files)[/dim]")
    print_usage_bar("Opus usage (planning)")

    d = today_dir(cfg)
    d.mkdir(parents=True, exist_ok=True)
    for t in tasks:
        save_task(t, cfg)
    console.print(f"\n[green]Saved {len(tasks)} tasks to {d}[/green]")
    console.print("[dim]Run 'usta go' to execute.[/dim]\n")


# ── tasks ─────────────────────────────────────────────────

@main.command()
@click.option("--all", "show_all", is_flag=True, help="Include finished tasks")
@click.pass_context
def tasks(ctx, show_all):
    """List tasks (pending only by default, --all for everything)."""
    cfg = _cfg(ctx.obj)
    if show_all:
        tl = load_tasks(cfg)
    else:
        tl = load_pending_tasks(cfg)
    if not tl:
        console.print("[yellow]No pending tasks. Run 'usta plan' first.[/yellow]")
        return

    finished = get_finished_ids(cfg)
    label = "all" if show_all else "pending"
    console.print(f"\n[bold]{len(tl)} {label} tasks:[/bold]\n")
    for t in tl:
        if t.id in finished:
            console.print(f"  [dim]{t.id}. {t.title} ✓ done[/dim]")
        else:
            console.print(f"  [cyan]{t.id}[/cyan]. {t.title} [dim]({len(t.files)} files)[/dim]")


# ── go ────────────────────────────────────────────────────

@main.command()
@click.option("-s", "--select", "task_ids", multiple=True, help="Task IDs")
@click.option("--no-review", is_flag=True, help="Skip review")
@click.option("--no-fix", is_flag=True, help="Skip auto-fix")
@click.option("--stop-on-fail", is_flag=True, help="Stop on first failure")
@click.pass_context
def go(ctx, task_ids, no_review, no_fix, stop_on_fail):
    """Run everything: select → execute → review → fix.

    \b
    Examples:
      usta go                     # all pending tasks
      usta go -s 04 -s 05 -s 06  # specific tasks
      usta go --no-review         # skip Opus review
    """
    # Ignore SIGPIPE so broken stdout (piped process) doesn't crash us
    import signal as _sig2
    try:
        _sig2.signal(_sig2.SIGPIPE, _sig2.SIG_DFL)
    except (OSError, ValueError):
        pass

    print_banner()
    reset_session_usage()
    cfg = _cfg(ctx.obj)

    # Only show pending (unfinished) tasks
    all_tasks = load_pending_tasks(cfg)
    if not all_tasks:
        console.print("[yellow]No pending tasks. Run 'usta plan' first.[/yellow]")
        return

    if task_ids:
        selected = [t for t in all_tasks if t.id in set(task_ids)]
    elif cfg.auto_mode:
        selected = all_tasks
        console.print(f"[dim]Auto mode: running all {len(selected)} tasks.[/dim]")
    else:
        selected = select_tasks(all_tasks)
    if not selected:
        return

    # Warn about large tasks — DeepSeek struggles with 5+ files
    large = [t for t in selected if len(t.files) > 4]
    if large:
        console.print(f"\n[yellow bold]⚠ {len(large)} task(s) have 5+ files — higher fail risk:[/yellow bold]")
        for t in large:
            console.print(f"  [yellow]{t.id}. {t.title} ({len(t.files)} files)[/yellow]")
        console.print(f"[dim]  Tip: re-plan with smaller tasks, or let watchdog handle failures.[/dim]\n")

    console.print(f"\n[bold]Running {len(selected)} tasks...[/bold]\n")

    from .runner import run_task, kill_active
    import signal as _sig

    exec_fn = run_task
    if cfg.executor == "aider":
        if not cfg.aider_bin:
            console.print(
                "[red]✗ aider binary not found. Install with `pip install aider-chat` "
                "or drop --executor aider.[/red]"
            )
            return
        exec_label = f"aider ({cfg.aider_model.split('/')[-1]})"
    else:
        exec_label = f"Qwen-direct ({cfg.applier_cfg.get('model', '').split('/')[-1]})"
    console.print(f"[dim]Executor:[/dim] {exec_label}")
    if cfg.auto_mode:
        console.print(f"[yellow bold]⚡ AUTO MODE — unattended, all tasks will run without prompts[/yellow bold]")

    # Catch Ctrl+C — flag stop; urllib request completes naturally
    _interrupted = False
    _orig_sigint = _sig.getsignal(_sig.SIGINT)
    def _on_sigint(sig, frame):
        nonlocal _interrupted
        _interrupted = True
        kill_active()
    _sig.signal(_sig.SIGINT, _on_sigint)

    # ── Dependency-aware parallel execution ──────────────────
    # Group tasks into waves: wave 0 = no deps, wave 1 = depends only on
    # wave 0 tasks, etc. Tasks within the same wave run concurrently.
    def _build_waves(tasks: list[Task]) -> list[list[Task]]:
        """Group tasks into dependency waves for parallel execution."""
        task_by_id = {t.id: t for t in tasks}
        done_ids: set[str] = set()
        remaining = list(tasks)
        waves: list[list[Task]] = []
        max_iters = len(tasks) + 1  # prevent infinite loop
        for _ in range(max_iters):
            if not remaining:
                break
            # Find tasks whose dependencies are all done.
            ready = [
                t for t in remaining
                if all(d in done_ids for d in (t.depends_on or []))
            ]
            if not ready:
                # Remaining tasks have unresolvable deps — run them anyway.
                ready = list(remaining)
            waves.append(ready)
            for t in ready:
                done_ids.add(t.id)
            remaining = [t for t in remaining if t.id not in done_ids]
        return waves

    waves = _build_waves(selected)

    dash = Dashboard(selected, title=f"Running {len(selected)} Tasks")
    dash.start()
    try:
        for wave in waves:
            if _interrupted:
                console.print("\n[yellow]Interrupted — stopping.[/yellow]")
                break
            # Run all tasks in this wave concurrently.
            events: list[tuple[Task, threading.Event]] = []
            for task in wave:
                done_ev = threading.Event()
                def _run(t=task, ev=done_ev):
                    exec_fn(t, cfg)
                    ev.set()
                thr = threading.Thread(target=_run, daemon=True)
                thr.start()
                events.append((task, done_ev))

            # Wait for all tasks in this wave to finish.
            while not all(ev.is_set() for _, ev in events):
                dash.tick()
                time.sleep(0.12)
                if _interrupted:
                    break
            # Join all threads in this wave.
            for task, done_ev in events:
                done_ev.wait(timeout=1)
                dash.tick()

            # Mark results.
            for task, _ in events:
                if task.status == "done":
                    mark_done(task.id, cfg, "pending-review" if not no_review else "no-review",
                              cost=task.cost, tokens=task.tokens, duration=task.duration,
                              messages=task.messages)
                elif task.status == "fail":
                    console.print(f"  [red]✗ Task {task.id} failed: {task.error}[/red]")
                    mark_done(task.id, cfg, "fail",
                              cost=task.cost, tokens=task.tokens, duration=task.duration,
                              error=task.error, messages=task.messages)

            if stop_on_fail and any(t.status == "fail" for t, _ in events):
                break
    finally:
        dash.stop()
        _sig.signal(_sig.SIGINT, _orig_sigint)

    try:
        print_summary(selected)
    except BrokenPipeError:
        pass

    # ── Review + auto-fix outer loop ───────────────────────
    # FIX 11: wrap review + auto-fix in an iterative loop so usta
    # behaves like Claude: review -> fix -> re-review -> fix again
    # until everything passes OR we run out of rounds OR we stop
    # making progress. Controlled by USTA_REVIEW_LOOP_MAX env var.
    succeeded = [t for t in selected if t.status == "done"]
    if succeeded and not no_review:
        _auto_commit_if_dirty(cfg)

        from .reviewer import review_batch

        def _has_real_issue(review: dict) -> bool:
            """Trigger auto-fix on BOTH warn and fail, but only when
            at least one issue is severity=warning or error. INFO-level
            nits should not round-trip through Qwen.
            """
            issues = review.get("issues") or []
            for it in issues:
                sev = str(it.get("severity", "")).lower()
                if sev in ("error", "warning"):
                    return True
            if not issues and review.get("verdict") == "fail":
                return True
            return False

        def _compute_fixable(review_map: dict) -> dict:
            return {
                tid: r for tid, r in review_map.items()
                if r.get("verdict") in ("fail", "warn")
                and r.get("fix_instructions")
                and _has_real_issue(r)
            }

        review_loop_max = max(1, int(os.environ.get("USTA_REVIEW_LOOP_MAX", "3")))
        review_round = 0
        reviews: dict = {}
        # First pass reviews everything; subsequent passes only re-review
        # the tasks that were just auto-fixed.
        tasks_to_review: list = list(succeeded)
        prev_fixable_ids: set[str] = set()

        while tasks_to_review and review_round < review_loop_max:
            review_round += 1
            round_tag = (
                f" (round {review_round}/{review_loop_max})"
                if review_round > 1 else ""
            )
            console.print(
                f"\n[bold yellow]Reviewing {len(tasks_to_review)} task(s) "
                f"with Opus{round_tag}...[/bold yellow]\n"
            )
            # FIX 1: run the real test runner on each task's touched
            # files before asking Opus to review the diff. Results get
            # folded into the review after the Opus pass so auto-fix
            # picks them up.
            verifier_results: dict[str, VerifierResult] = {}
            for t in tasks_to_review:
                try:
                    vr = verify_files(t.files, cfg.project_dir)
                except Exception as e:
                    console.print(
                        f"  [yellow]verifier crashed for {t.id}: "
                        f"{e}[/yellow]"
                    )
                    continue
                verifier_results[t.id] = vr
                if vr.ran:
                    colour = "red" if not vr.ok else "dim"
                    console.print(
                        f"  [{colour}]  {t.id} {vr.summary()}[/{colour}]"
                    )

            # FIX 11: run cheap static checks (pyflakes/tsc/govet) on
            # each task's files. Unused imports, undefined names, syntax
            # errors etc. get promoted to auto-fix findings so Qwen
            # never ships obvious bugs.
            static_results: dict[str, StaticResult] = {}
            for t in tasks_to_review:
                try:
                    sr = static_check_files(t.files, cfg.project_dir)
                except Exception as e:
                    console.print(
                        f"  [yellow]static check crashed for {t.id}: "
                        f"{e}[/yellow]"
                    )
                    continue
                static_results[t.id] = sr
                if sr.ran and (sr.findings or sr.errors):
                    console.print(
                        f"  [yellow]  {t.id} {sr.summary()}[/yellow]"
                    )
                elif sr.ran:
                    console.print(
                        f"  [dim]  {t.id} {sr.summary()}[/dim]"
                    )

            round_reviews: dict = {}
            try:
                t0_r = time.time()
                console.print(
                    f"  [dim]0s[/dim]  [cyan]⠋[/cyan] Reviewing "
                    f"{len(tasks_to_review)} task(s)...",
                    end="",
                )
                round_reviews, resp = review_batch(tasks_to_review, cfg)
                # FIX 11: merge static-check findings. Lint issues are
                # severity=warning; they trigger auto-fix but don't
                # force verdict=fail on their own — a passing task with
                # an unused import stays "warn".
                for tid, sr in static_results.items():
                    if not sr.ran or (not sr.findings and not sr.errors):
                        continue
                    rev = round_reviews.setdefault(tid, {
                        "task_id": tid, "verdict": "pass",
                        "issues": [], "summary": "", "fix_instructions": None,
                    })
                    rev.setdefault("issues", []).extend(sr.to_review_findings())
                    if rev.get("verdict") == "pass":
                        rev["verdict"] = "warn"
                    existing_fix = rev.get("fix_instructions") or ""
                    bullets = "\n".join(
                        f"- {f.file}:{f.line} {f.message}"
                        for f in sr.findings
                    )
                    static_preamble = (
                        f"Static checker [{sr.checker}] flagged "
                        f"{len(sr.findings)} issue(s) on the files this "
                        f"task touched. Resolve them without breaking "
                        f"other behaviour:\n{bullets}"
                    )
                    rev["fix_instructions"] = (
                        static_preamble
                        + (f"\n\n{existing_fix}" if existing_fix else "")
                    )
                    if not rev.get("summary"):
                        rev["summary"] = (
                            f"{sr.checker}: {len(sr.findings)} lint issue(s)"
                        )

                # FIX 1: merge verifier findings into the review result.
                # Real test failures are severity=error and always force
                # verdict=fail with concrete fix_instructions.
                for tid, vr in verifier_results.items():
                    if not vr.ran or vr.ok:
                        continue
                    rev = round_reviews.setdefault(tid, {
                        "task_id": tid, "verdict": "pass",
                        "issues": [], "summary": "", "fix_instructions": None,
                    })
                    rev.setdefault("issues", []).extend(vr.to_review_findings())
                    rev["verdict"] = "fail"
                    existing_fix = rev.get("fix_instructions") or ""
                    # Prepend a structured block so Qwen sees it first.
                    tail = vr.stdout_tail or ""
                    fix_preamble = (
                        f"Test runner [{vr.runner}] reported "
                        f"{len(vr.failures)} failing of {vr.tests_executed} "
                        f"executed tests on the files this task touched. "
                        f"Fix the code so every test passes. Failures:\n"
                        + "\n".join(
                            f"- {f.file}::{f.name}: {f.message}"
                            for f in vr.failures
                        )
                    )
                    if tail:
                        fix_preamble += f"\n\nLast runner output:\n{tail}"
                    rev["fix_instructions"] = (
                        fix_preamble
                        + (f"\n\n{existing_fix}" if existing_fix else "")
                    )
                    rev["summary"] = (
                        f"{vr.runner}: {len(vr.failures)} failing tests"
                        + (f"; {rev.get('summary','')}"
                           if rev.get("summary") else "")
                    )
                elapsed_r = time.time() - t0_r
                console.print(
                    f"\r  [dim]{elapsed_r:.0f}s[/dim]  [green]✓[/green] "
                    f"Reviewed {len(tasks_to_review)} task(s)   "
                )

                usage = resp.usage if resp else None
                for t in tasks_to_review:
                    rev = round_reviews.get(
                        t.id, {"verdict": "pass", "summary": "Not reviewed"}
                    )
                    print_review(
                        rev, t, usage if t == tasks_to_review[0] else None
                    )
                    verdict = rev.get("verdict", "unknown")
                    mark_done(t.id, cfg, verdict)
            except BrokenPipeError:
                for t in tasks_to_review:
                    if t.id not in round_reviews:
                        mark_done(t.id, cfg, "review-error")
            except Exception as e:
                try:
                    console.print(f"\n  [red]✗ Review error: {e}[/red]")
                except BrokenPipeError:
                    pass
                for t in tasks_to_review:
                    if t.id not in round_reviews:
                        mark_done(t.id, cfg, "review-error")

            # Merge this round's reviews into the accumulated map
            reviews.update(round_reviews)

            # FIX 5: harvest lessons from real findings so the next
            # planner run can reference them. Only warn/fail verdicts
            # with error/warning-severity issues produce lessons —
            # pass/info nits are ignored.
            try:
                for t in tasks_to_review:
                    rev = round_reviews.get(t.id)
                    if not rev:
                        continue
                    msgs = harvest_from_review(rev)
                    if msgs:
                        record_lessons(
                            cfg.project_dir, msgs,
                            task_hint=t.title, source="reviewer",
                        )
            except Exception:
                pass

            print_usage_bar(f"Opus usage (review round {review_round})")

            fixable = _compute_fixable(round_reviews)

            if not fixable:
                if review_round > 1:
                    console.print(
                        f"\n[bold green]  ✓ All issues resolved after "
                        f"review round {review_round}.[/bold green]"
                    )
                break

            # Skip info-only findings message (first round only)
            if review_round == 1:
                skipped = [
                    tid for tid, r in round_reviews.items()
                    if r.get("verdict") in ("fail", "warn")
                    and r.get("fix_instructions")
                    and not _has_real_issue(r)
                ]
                if skipped:
                    console.print(
                        f"[dim]  Skipped auto-fix for {len(skipped)} "
                        f"task(s) with info-only findings.[/dim]"
                    )

            if no_fix or not cfg.auto_fix:
                break

            # Progress check: if the same tasks are failing with the
            # same issue count, give up instead of spinning forever.
            # We compare (task_ids, total_issue_count) — if an auto-fix
            # reduced issue count but didn't fully resolve, that's still
            # progress and we should keep going.
            current_ids = set(fixable.keys())
            current_issue_count = sum(
                len(r.get("issues") or []) for r in fixable.values()
            )
            prev_issue_count = getattr(
                _compute_fixable, "_prev_issue_count", -1
            )
            if (
                review_round > 1
                and current_ids == prev_fixable_ids
                and current_issue_count >= prev_issue_count
            ):
                console.print(
                    f"[yellow]  No progress in round {review_round} "
                    f"({len(current_ids)} task(s), "
                    f"{current_issue_count} issue(s) — same as last round) "
                    f"— stopping loop.[/yellow]"
                )
                break
            prev_fixable_ids = current_ids
            _compute_fixable._prev_issue_count = current_issue_count

            if review_round >= review_loop_max:
                console.print(
                    f"[yellow]  Review loop hit max rounds "
                    f"({review_loop_max}); {len(fixable)} task(s) "
                    f"still failing.[/yellow]"
                )
                break

            console.print(
                f"\n[bold yellow]{len(fixable)} task(s) have fixable "
                f"issues. Running auto-fix (round {review_round})..."
                f"[/bold yellow]"
            )
            _auto_fix(fixable, selected, cfg)

            # Next iteration: only re-review the originals we just fixed
            tasks_to_review = [t for t in succeeded if t.id in fixable]

    try:
        print_usage_bar("Total Opus usage this session")
        console.print("\n[bold green]Done![/bold green]\n")
    except BrokenPipeError:
        pass  # stdout disconnected (e.g. piped process died)


# ── review (standalone) ──────────────────────────────────

@main.command()
@click.option("-s", "--select", "task_ids", multiple=True)
@click.pass_context
def review(ctx, task_ids):
    """Review changes with Opus ($0)."""
    reset_session_usage()
    cfg = _cfg(ctx.obj)
    tl = load_pending_tasks(cfg)
    if task_ids:
        tl = [t for t in tl if t.id in set(task_ids)]
    if not tl:
        console.print("[yellow]Nothing to review.[/yellow]")
        return

    from .reviewer import review_batch
    console.print(f"  [cyan]⠋[/cyan] Reviewing {len(tl)} tasks (single call)...")
    reviews, resp = review_batch(tl, cfg)
    usage = resp.usage if resp else None
    for t in tl:
        rev = reviews.get(t.id, {"verdict": "pass", "summary": "Not reviewed"})
        print_review(rev, t, usage if t == tl[0] else None)
        mark_done(t.id, cfg, rev.get("verdict", "unknown"))

    print_usage_bar("Opus usage (review)")


# ── status ────────────────────────────────────────────────

@main.command()
@click.pass_context
def status(ctx):
    """Live progress — shows what's running, done, and pending."""
    import shutil
    import subprocess as sp
    print_banner()
    cfg = _cfg(ctx.obj)

    claude_ok = shutil.which(cfg.claude_bin) is not None
    openrouter_ok = bool(cfg.openrouter_api_key)
    aider_ok = bool(cfg.aider_bin)

    planner_str = cfg.planner_cfg.get("full", "?")
    applier_str = cfg.applier_cfg.get("full", "?")
    console.print(f"  Planner {'[green]✓[/green]' if claude_ok else '[red]✗[/red]'}  [dim]{planner_str}[/dim]")
    if cfg.executor == "aider":
        console.print(f"  Applier {'[green]✓[/green]' if aider_ok else '[red]✗[/red]'}  [dim]{cfg.aider_model}[/dim] [cyan](aider @ {cfg.aider_bin or 'not found'})[/cyan]")
    else:
        console.print(f"  Applier {'[green]✓[/green]' if openrouter_ok else '[red]✗[/red]'}  [dim]{applier_str}[/dim] [cyan](qwen-direct)[/cyan]")
    console.print(f"  Project [dim]{cfg.project_dir}[/dim]")

    # Session info
    sid = cfg.get_session_id()
    if sid:
        console.print(f"  Session [green]♻ Active[/green] [dim]({sid[:8]}…)[/dim]")
    else:
        console.print(f"  Session [yellow]★ None[/yellow]")

    # ── Detect running usta go / aider processes ──────────
    running_task = None
    running_elapsed = ""
    try:
        r = sp.run(["ps", "aux"], capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if "usta" in line and "go" in line and "grep" not in line:
                console.print(f"\n  [bold cyan]⚡ usta go is running[/bold cyan]")
                break
        import re as _re
        for line in r.stdout.splitlines():
            if ("aider --read" in line or ("qwen" in line and "--yolo" in line)) and "grep" not in line:
                m = _re.search(r'/(\d+_[^/]+\.md)', line)
                if m:
                    running_task = m.group(1)
                break

        # Get elapsed time from ps for the executor process
        if running_task:
            r2 = sp.run(["ps", "-o", "etime=", "-p",
                         *[line.split()[1] for line in r.stdout.splitlines()
                           if ("aider --read" in line or ("qwen" in line and "--yolo" in line)) and "grep" not in line][:1]],
                        capture_output=True, text=True, timeout=5)
            running_elapsed = r2.stdout.strip() if r2.returncode == 0 else ""
            elapsed_str = f" [dim]({running_elapsed})[/dim]" if running_elapsed else ""
            console.print(f"  [cyan]⠸ Active:[/cyan] {running_task}{elapsed_str}")
    except Exception:
        pass

    # ── Task progress ─────────────────────────────────────
    all_t = load_tasks(cfg)
    finished_ids = get_finished_ids(cfg)
    pending = [t for t in all_t if t.id not in finished_ids]

    if all_t:
        from .tasks import _load_state
        state = _load_state(cfg)
        fin_data = state.get("finished", {})

        # Count by status
        n_total = len(all_t)
        task_ids_in_files = {t.id for t in all_t}
        done_ids = set()
        fail_ids = set()
        for tid, entry in fin_data.items():
            if tid not in task_ids_in_files:
                continue
            if entry.get("status") == "fail" or entry.get("review") == "fail":
                fail_ids.add(tid)
            else:
                done_ids.add(tid)
        n_done = len(done_ids)
        n_fail = len(fail_ids)
        is_running = running_task is not None
        n_pending = n_total - n_done - n_fail - (1 if is_running else 0)
        if n_pending < 0:
            n_pending = 0

        # Progress bar
        if n_total > 0:
            pct = (n_done + n_fail) / n_total
            bar_len = 20
            filled = int(bar_len * pct)
            bar = "█" * filled + "░" * (bar_len - filled)
            parts = [f"[green]{n_done} done[/green]"]
            if n_fail:
                parts.append(f"[red]{n_fail} fail[/red]")
            if is_running:
                parts.append(f"[cyan]1 running[/cyan]")
            if n_pending > 0:
                parts.append(f"[dim]{n_pending} pending[/dim]")
            console.print(f"\n  Tasks   [green]{bar}[/green] {n_done + n_fail}/{n_total}  {', '.join(parts)}")

        # Cost + time summary from state (only count tasks that have files)
        costs = [v.get("cost", 0) or 0 for tid, v in fin_data.items() if tid in task_ids_in_files]
        durations = [v.get("duration", 0) or 0 for tid, v in fin_data.items() if tid in task_ids_in_files]
        tokens_list = [v.get("tokens", 0) or 0 for tid, v in fin_data.items() if tid in task_ids_in_files]
        total_cost = sum(costs)
        total_tokens = sum(tokens_list)
        total_time = sum(durations)

        if total_cost > 0 or total_tokens > 0:
            tok_str = f"{total_tokens/1000:.0f}k" if total_tokens >= 1000 else str(total_tokens)
            console.print(f"  Cost    [yellow]${total_cost:.4f}[/yellow] [dim]({tok_str} tokens, {total_time:.0f}s)[/dim]")

        # ETA based on average duration of completed tasks
        done_durations = [v.get("duration", 0) or 0 for tid, v in fin_data.items()
                         if tid in task_ids_in_files and tid in done_ids and v.get("duration")]
        if done_durations and (n_pending > 0 or is_running):
            avg_dur = sum(done_durations) / len(done_durations)
            remaining = n_pending + (1 if is_running else 0)
            eta_secs = avg_dur * remaining
            if eta_secs > 3600:
                eta_str = f"{eta_secs/3600:.1f}h"
            elif eta_secs > 60:
                eta_str = f"{eta_secs/60:.0f}m"
            else:
                eta_str = f"{eta_secs:.0f}s"
            console.print(f"  ETA     [cyan]~{eta_str}[/cyan] [dim](avg {avg_dur:.0f}s/task, {remaining} left)[/dim]")

        # Per-task breakdown — collapse old done tasks without stats
        def _flush_collapsed(ids):
            if len(ids) == 1:
                console.print(f"  ✅ [dim]{ids[0]}.[/dim] [dim](no stats)[/dim]")
            else:
                console.print(f"  ✅ [green]{ids[0]}–{ids[-1]}[/green] [dim]({len(ids)} tasks, no stats)[/dim]")

        console.print()
        collapsed = []
        for t in all_t:
            if t.id in finished_ids:
                entry = fin_data.get(t.id, {})
                verdict = entry.get("review", "done")
                is_fail = entry.get("status") == "fail" or verdict == "fail"
                has_stats = entry.get("cost") or entry.get("tokens") or entry.get("duration")

                if is_fail:
                    if collapsed:
                        _flush_collapsed(collapsed)
                        collapsed = []
                    err = entry.get("error", "unknown")
                    console.print(f"  ❌ [dim]{t.id}.[/dim] {t.title} [red]{err}[/red]")
                elif has_stats:
                    if collapsed:
                        _flush_collapsed(collapsed)
                        collapsed = []
                    cost = entry.get("cost")
                    tokens = entry.get("tokens", 0)
                    msgs = entry.get("messages", 0)
                    dur = entry.get("duration", 0)
                    stats = []
                    if cost:
                        stats.append(f"[yellow]${cost:.3f}[/yellow]")
                    if tokens:
                        tok_s = f"{tokens/1000:.0f}k" if tokens >= 1000 else str(tokens)
                        msg_s = f"{msgs} msg, " if msgs else ""
                        stats.append(f"[dim]{msg_s}{tok_s} tok[/dim]")
                    if dur:
                        stats.append(f"[dim]{dur:.0f}s[/dim]")
                    stat_str = f"  {'  '.join(stats)}" if stats else ""
                    console.print(f"  ✅ [dim]{t.id}.[/dim] {t.title}{stat_str}")
                else:
                    collapsed.append(t.id)
            else:
                if collapsed:
                    _flush_collapsed(collapsed)
                    collapsed = []
                if running_task and t.filename == running_task:
                    el = f" [dim]({running_elapsed})[/dim]" if running_elapsed else ""
                    console.print(f"  [cyan]⠸[/cyan]  [bold]{t.id}.[/bold] {t.title} [cyan]running...{el}[/cyan]")
                else:
                    console.print(f"  ⏳ [dim]{t.id}. {t.title}[/dim]")
        if collapsed:
            _flush_collapsed(collapsed)

    console.print()


# ── peek ──────────────────────────────────────────────────

@main.command()
@click.option("-n", "--lines", "n_lines", default=30, help="Lines to show before following")
@click.option("--no-follow", is_flag=True, help="Show last N lines and exit (no tail -f)")
@click.pass_context
def peek(ctx, n_lines, no_follow):
    """Live-stream the running task's aider output (tail -f). Ctrl+C to stop."""
    import subprocess as sp
    cfg = _cfg(ctx.obj)

    # Find which task is running
    r = sp.run(["ps", "aux"], capture_output=True, text=True, timeout=5)
    import re as _re
    running_file = None
    for line in r.stdout.splitlines():
        if ("aider --read" in line or "qwen" in line) and "grep" not in line:
            m = _re.search(r'/(\d+_[^/]+\.md)', line)
            if m:
                running_file = m.group(1)
            break

    if not running_file:
        console.print("[yellow]No executor process running.[/yellow]")
        return

    # Find log file
    log_dir = cfg.project_dir / ".usta" / "logs"
    log_name = running_file.replace(".md", ".log")
    tid = running_file.split("_")[0]
    log_path = log_dir / f"{tid}_{log_name}"

    if not log_path.exists():
        console.print(f"[yellow]No live log yet for {running_file}[/yellow]")
        console.print(f"[dim]Expected: {log_path}[/dim]")
        return

    console.print(f"[bold]Peeking: {running_file}[/bold]")
    console.print(f"[dim]Ctrl+C to stop.[/dim]\n")

    try:
        # Use tail -f for live streaming, tail -n for snapshot
        tail_args = ["tail", f"-n{n_lines}"]
        if not no_follow:
            tail_args.append("-f")
        tail_args.append(str(log_path))

        proc = sp.Popen(tail_args, stdout=sp.PIPE, stderr=sp.DEVNULL, text=True)
        for line in proc.stdout:
            line = line.rstrip()
            # Colorize key patterns
            if line.startswith("Tokens:") or line.startswith("Cost:"):
                console.print(f"[yellow]{line}[/yellow]")
            elif "Applied edit" in line or "Commit" in line:
                console.print(f"[green]{line}[/green]")
            elif "did not conform" in line or "error" in line.lower():
                console.print(f"[red]{line}[/red]")
            else:
                print(line)
        proc.wait()
    except KeyboardInterrupt:
        proc.kill()
        console.print(f"\n[dim]Stopped. Log: {log_path}[/dim]")


# ── logs ──────────────────────────────────────────────────

@main.command()
@click.argument("task_id", required=False)
@click.pass_context
def logs(ctx, task_id):
    """View aider output for a task. Shows last task if no ID given."""
    cfg = _cfg(ctx.obj)
    log_dir = cfg.project_dir / ".usta" / "logs"
    if not log_dir.exists():
        console.print("[yellow]No logs yet. Run 'usta go' first.[/yellow]")
        return

    log_files = sorted(log_dir.glob("*.log"))
    if not log_files:
        console.print("[yellow]No logs yet.[/yellow]")
        return

    if task_id:
        matches = [f for f in log_files if f.name.startswith(f"{task_id}_")]
        if not matches:
            console.print(f"[yellow]No log for task {task_id}.[/yellow]")
            console.print(f"[dim]Available: {', '.join(f.stem.split('_')[0] for f in log_files)}[/dim]")
            return
        target = matches[0]
    else:
        target = log_files[-1]

    console.print(f"[bold]Log: {target.name}[/bold]\n")
    content = target.read_text(errors="replace")
    # Show last 80 lines (most relevant)
    lines = content.splitlines()
    if len(lines) > 80:
        console.print(f"[dim]... ({len(lines) - 80} lines hidden, showing last 80)[/dim]\n")
        lines = lines[-80:]
    for line in lines:
        console.print(line)


# ── diff ──────────────────────────────────────────────────

@main.command()
@click.pass_context
def diff(ctx):
    """Show all changes made by usta on this branch."""
    import subprocess as sp
    cfg = _cfg(ctx.obj)

    # Find merge-base with master/main
    for base in ["master", "main"]:
        try:
            r = sp.run(["git", "merge-base", base, "HEAD"],
                      capture_output=True, text=True, cwd=str(cfg.project_dir))
            if r.returncode == 0:
                merge_base = r.stdout.strip()
                # Stat summary
                r2 = sp.run(["git", "diff", "--stat", merge_base, "HEAD"],
                           capture_output=True, text=True, cwd=str(cfg.project_dir))
                console.print(f"[bold]Changes since {base}:[/bold]\n")
                console.print(r2.stdout)
                # Commit list
                r3 = sp.run(["git", "log", "--oneline", f"{merge_base}..HEAD"],
                           capture_output=True, text=True, cwd=str(cfg.project_dir))
                n_commits = len(r3.stdout.strip().splitlines())
                console.print(f"[dim]{n_commits} commits[/dim]")
                return
        except Exception:
            continue
    console.print("[yellow]Could not determine base branch.[/yellow]")


# ── retry ─────────────────────────────────────────────────

@main.command()
@click.option("-s", "--select", "task_ids", multiple=True, help="Task IDs to retry")
@click.pass_context
def retry(ctx, task_ids):
    """Re-run failed or specific tasks."""
    from .tasks import _load_state, _save_state
    cfg = _cfg(ctx.obj)

    state = _load_state(cfg)
    finished = state.get("finished", {})

    if task_ids:
        # Remove specific tasks from finished state
        for tid in task_ids:
            finished.pop(tid, None)
        state["finished"] = finished
        _save_state(cfg, state)
        console.print(f"[green]Reset {len(task_ids)} tasks. Run 'usta go' to re-execute.[/green]")
    else:
        # Reset all failed tasks
        failed = [tid for tid, v in finished.items() if v.get("review") == "fail" or v.get("status") == "fail"]
        if not failed:
            console.print("[yellow]No failed tasks to retry.[/yellow]")
            return
        for tid in failed:
            finished.pop(tid)
        state["finished"] = finished
        _save_state(cfg, state)
        console.print(f"[green]Reset {len(failed)} failed tasks: {', '.join(failed)}[/green]")
        console.print("[dim]Run 'usta go' to re-execute.[/dim]")


# ── forget ────────────────────────────────────────────────

@main.command()
@click.pass_context
def forget(ctx):
    """Clear Claude's memory of this project. Next plan will re-explore."""
    cfg = _cfg(ctx.obj)
    cfg.clear_session()
    console.print("[green]Session cleared.[/green] Next [bold]usta plan[/bold] will explore the codebase fresh.")


# ── agent ─────────────────────────────────────────────────

@main.command()
@click.argument("instruction")
@click.option("--mcp-config", "mcp_config", default=None, type=click.Path(),
              help="Path to an MCP server config JSON. Defaults to "
                   ".usta/mcp.json then ~/.config/usta/mcp.json")
@click.option("--allow", "allow", multiple=True,
              help="Whitelist an MCP/claude tool name. Repeat for multiple. "
                   "Default: all tools from the MCP config + claude built-ins.")
@click.option("--max-turns", "max_turns", default=40, show_default=True,
              help="Upper bound on agent turns.")
@click.pass_context
def agent(ctx, instruction, mcp_config, allow, max_turns):
    """Run Opus as an agent with MCP tools (Slack/SSH/gcloud/Figma/…).

    Does not touch task files — this is a one-shot agentic turn for
    things like "ping the team that the build is green", "grab the
    latest Figma frame and drop it into docs/", or "tail prod logs until
    the next error and summarize what happened".
    """
    from .agent import run_agent

    print_banner()
    reset_session_usage()
    cfg = _cfg(ctx.obj)

    explicit = Path(mcp_config).expanduser().resolve() if mcp_config else None
    allowed = list(allow) if allow else None

    console.print(f"[bold]Instruction:[/bold] {instruction}")
    console.print(f"[bold]Project:[/bold]   {cfg.project_dir}")

    # Resolve and show which MCP config we're about to use.
    from .agent import _resolve_mcp_config, _list_mcp_servers
    resolved = _resolve_mcp_config(cfg.project_dir, explicit)
    if resolved:
        servers = _list_mcp_servers(resolved)
        srv_text = ", ".join(servers) if servers else "(no servers parsed)"
        console.print(f"[bold]MCP:[/bold]       {resolved}  →  {srv_text}")
    else:
        console.print(
            "[dim]MCP:       none — running with claude built-ins only. "
            "Drop an mcp.json in .usta/ or ~/.config/usta/ to add tools.[/dim]"
        )
    console.print()

    # Stream tool calls and assistant text as they arrive.
    t_start = time.time()
    last_text = {"s": ""}

    def _on_event(kind: str, payload: dict):
        if kind == "tool":
            name = payload.get("tool", "?")
            elapsed = payload.get("elapsed", 0.0)
            console.print(f"  [cyan]· {elapsed:5.1f}s  tool[/cyan] {name}")
        elif kind == "text":
            text = (payload.get("text") or "").strip()
            if text and text != last_text["s"]:
                last_text["s"] = text
                preview = text if len(text) <= 200 else text[:200] + "…"
                console.print(f"  [dim]{preview}[/dim]")

    run = run_agent(
        instruction, cfg,
        mcp_config=explicit,
        allowed_tools=allowed,
        max_turns=max_turns,
        on_event=_on_event,
    )
    elapsed = time.time() - t_start

    console.print()
    if run.ok:
        console.print(f"[bold green]✓ Agent done[/bold green]  {elapsed:.1f}s  ${run.cost_usd:.4f}")
        if run.text:
            console.print()
            console.print(run.text)
    else:
        console.print(f"[bold red]✗ Agent failed[/bold red]  {elapsed:.1f}s")
        console.print(f"[red]{run.error or 'unknown error'}[/red]")


# ── helpers ───────────────────────────────────────────────

def _auto_commit_if_dirty(cfg: Cfg):
    """If aider left uncommitted changes (modified or new files), commit them before review."""
    import subprocess as sp
    try:
        r = sp.run(["git", "status", "--porcelain"], capture_output=True, text=True,
                   cwd=str(cfg.project_dir), timeout=10)
        if not r.stdout.strip():
            return
        n_changes = len([l for l in r.stdout.strip().splitlines() if l.strip()])
        console.print(f"[dim]  Auto-committing {n_changes} aider changes before review...[/dim]")
        # Use git add -A to catch all changes (modified, new, deleted) — safe
        # because we're in the project dir and aider only touches relevant files
        sp.run(["git", "add", "-A"], cwd=str(cfg.project_dir), timeout=10)
        sp.run(["git", "commit", "-m", "chore: usta auto-commit aider changes for review"],
               cwd=str(cfg.project_dir), timeout=10, capture_output=True)
    except Exception:
        pass  # Non-critical — review will still try unstaged diff


_FILE_MENTION_RE = re.compile(
    r"(?<![\w/.-])"
    r"((?:[\w.-]+/)+[\w.-]+\.[A-Za-z0-9]{1,6})"
    r"(?![\w/.-])"
)

# Dotted module references like `cara.support.Str` or `cara.validation.rules.BaseRule`.
# Require at least 2 segments so we don't match single words like "validate".
_MODULE_REF_RE = re.compile(
    r"(?<![\w.])"
    r"([a-z_][a-zA-Z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,6})"
    r"(?![\w.])"
)

# Bare filenames inside backticks: `__init__.py`, `Str.py`, `Time.py`
_BACKTICK_FILE_RE = re.compile(r"`([A-Za-z_][\w\-]*\.[A-Za-z0-9]{1,6})`")


def _resolve_module_to_path(project_dir: Path, dotted: str) -> Optional[str]:
    """Resolve a dotted module ref (cara.support.Str) to a relative .py path.

    Tries both ``<parts>.py`` and ``<parts>/__init__.py``. Returns the first
    hit that exists inside *project_dir*.
    """
    parts = dotted.split(".")
    base = Path(*parts)
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        full = project_dir / candidate
        if full.exists() and full.is_file():
            return str(candidate)
    return None


def _resolve_bare_filename(project_dir: Path, name: str) -> Optional[str]:
    """Resolve a bare filename (``__init__.py``) by searching the tree.

    Only returns a match if there is exactly one file with that name in
    the project — otherwise it's ambiguous and we refuse to guess.
    """
    if "/" in name or "\\" in name:
        return None
    matches: list[Path] = []
    # Keep the walk cheap — cap at ~2000 files.
    count = 0
    for p in project_dir.rglob(name):
        if p.is_file() and ".git" not in p.parts and "node_modules" not in p.parts:
            matches.append(p)
            if len(matches) > 1:
                return None  # ambiguous
        count += 1
        if count > 2000:
            break
    if len(matches) == 1:
        return str(matches[0].relative_to(project_dir))
    return None


def _extract_fix_files(text: str, project_dir: Path) -> list[str]:
    """Pull plausible file paths out of reviewer's fix_instructions.

    Three strategies, in order of specificity:

    1. Literal paths like ``cara/support/Str.py``.
    2. Dotted module references like ``cara.support.Str`` → resolve to
       ``cara/support/Str.py`` or ``cara/support/Str/__init__.py``.
    3. Bare filenames in backticks like ``` `__init__.py` ``` → resolve
       only if the name is unique in the project.

    Returns only files that actually exist on disk. Empty list means the
    reviewer's instructions were too vague to safely auto-fix; the caller
    should skip auto-fix rather than guess.
    """
    if not text:
        return []
    seen: set[str] = set()
    out: list[str] = []

    # 1. Literal paths (highest signal)
    for m in _FILE_MENTION_RE.findall(text):
        rel = m.strip().strip(".,;:)('\"[]")
        if not rel or rel in seen:
            continue
        seen.add(rel)
        p = project_dir / rel
        if p.exists() and p.is_file():
            out.append(rel)

    # 2. Dotted module refs
    for m in _MODULE_REF_RE.findall(text):
        if m in seen:
            continue
        resolved = _resolve_module_to_path(project_dir, m)
        if resolved and resolved not in seen:
            seen.add(resolved)
            seen.add(m)
            out.append(resolved)

    # 3. Bare filenames in backticks (lowest signal, only if unique)
    for m in _BACKTICK_FILE_RE.findall(text):
        if m in seen:
            continue
        seen.add(m)
        resolved = _resolve_bare_filename(project_dir, m)
        if resolved and resolved not in seen:
            seen.add(resolved)
            out.append(resolved)

    return out


def _auto_fix(fixable: dict, tasks: list[Task], cfg: Cfg):
    """Generate fix tasks from review feedback and run them through the qwen-direct executor."""
    from .runner import run_task as do_run

    fix_tasks = []
    skipped_vague: list[str] = []
    for tid, rev in fixable.items():
        orig = next((t for t in tasks if t.id == tid), None)
        if not orig or not rev.get("fix_instructions"):
            continue
        instr = rev["fix_instructions"]
        # Prefer files referenced by the fix_instructions (they point at the
        # ACTUAL code to touch — which for deletion tasks is downstream call
        # sites, not the already-removed modules).
        fix_files = _extract_fix_files(instr, cfg.project_dir)
        if not fix_files:
            # Intersect original task's files with files mentioned in the
            # fix instructions by name (loose match). This catches the
            # "reviewer talks about `__init__.py` generically" case while
            # refusing to blindly rewrite every file the original task
            # touched — which is what caused the app/helpers deletion bug.
            candidates = [f for f in orig.files if (cfg.project_dir / f).exists()]
            mentioned_candidates = [
                f for f in candidates
                if Path(f).name in instr or f in instr
            ]
            if mentioned_candidates:
                fix_files = mentioned_candidates
            else:
                # Reviewer's instructions don't reference any file we can
                # identify. Refuse to guess — skip this auto-fix entirely.
                skipped_vague.append(tid)
                continue
        # Build a rich fix prompt so Qwen has full context: what the
        # task was, what the reviewer found, and exactly what to fix.
        fix_body_parts = [f"# Fix: {orig.title}\n"]
        # Include the reviewer's summary for context.
        if rev.get("summary"):
            fix_body_parts.append(
                f"## Reviewer Summary\n{rev['summary']}\n"
            )
        # Include specific issues so Qwen can address each one.
        issues = rev.get("issues") or []
        if issues:
            issue_lines = []
            for iss in issues:
                sev = iss.get("severity", "?")
                f_name = iss.get("file", "")
                line_n = iss.get("line", 0)
                msg = iss.get("msg", "")
                loc = f"{f_name}:{line_n}" if f_name and line_n else (f_name or "?")
                issue_lines.append(f"- [{sev}] {loc} — {msg}")
            fix_body_parts.append(
                "## Issues Found\n" + "\n".join(issue_lines) + "\n"
            )
        fix_body_parts.append(f"## Fix Instructions\n{instr}")
        fix_content = "\n".join(fix_body_parts)

        ft = Task(
            id=f"{tid}f", title=f"Fix: {orig.title}",
            filename=f"{tid}_fix.md",
            content=fix_content,
            files=fix_files,
        )
        fix_tasks.append(ft)
        save_task(ft, cfg)

    if skipped_vague:
        console.print(
            f"[yellow]  Skipped {len(skipped_vague)} fix task(s) with "
            f"unresolvable file references: {', '.join(skipped_vague)}. "
            f"Review the fix_instructions manually and re-run with an "
            f"explicit file list if needed.[/yellow]"
        )

    if not fix_tasks:
        console.print("[dim]  No fix instructions from review — skipping auto-fix.[/dim]")
        return

    console.print(f"\n[bold]Running {len(fix_tasks)} fixes...[/bold]\n")
    FIX_TIMEOUT = 180  # 3 minutes max per fix task
    dash = Dashboard(fix_tasks, title="Fixing Issues")
    dash.start()
    try:
        for ft in fix_tasks:
            done_ev = threading.Event()
            def _run(t=ft):
                try:
                    do_run(t, cfg)
                except Exception as e:
                    t.status = "fail"
                    t.error = str(e)
                done_ev.set()
            thr = threading.Thread(target=_run, daemon=True)
            thr.start()
            deadline = time.time() + FIX_TIMEOUT
            while not done_ev.is_set() and time.time() < deadline:
                dash.tick()
                time.sleep(0.12)
            if not done_ev.is_set():
                ft.status = "fail"
                ft.error = f"timed out after {FIX_TIMEOUT}s"
                console.print(f"  [red]Fix {ft.id} timed out[/red]")
            else:
                thr.join(timeout=2)
            dash.tick()
    finally:
        dash.stop()

    # Commit fix changes
    _auto_commit_if_dirty(cfg)

    # Report fix results
    ok = [ft for ft in fix_tasks if ft.status == "done"]
    fail = [ft for ft in fix_tasks if ft.status == "fail"]
    if ok:
        console.print(f"  [green]✓ {len(ok)} fix(es) applied successfully[/green]")
    if fail:
        console.print(f"  [red]✗ {len(fail)} fix(es) failed[/red]")
        for ft in fail:
            console.print(f"    [red]{ft.id}. {ft.title}: {ft.error}[/red]")


# ── kill ──────────────────────────────────────────────────

@main.command()
@click.pass_context
def kill(ctx):
    """Kill orphan aider processes from a previous usta go."""
    import subprocess as sp
    cfg = _cfg(ctx.obj)
    project = str(cfg.project_dir)

    r = sp.run(["ps", "aux"], capture_output=True, text=True, timeout=5)
    pids = []
    for line in r.stdout.splitlines():
        if ("aider" in line or "qwen" in line) and project in line and "grep" not in line:
            pid = line.split()[1]
            pids.append(pid)

    if not pids:
        console.print("[green]No orphan executor processes found.[/green]")
        return

    for pid in pids:
        try:
            import os as _os
            _os.kill(int(pid), 15)  # SIGTERM
            console.print(f"  [red]Killed[/red] aider PID {pid}")
        except OSError as e:
            console.print(f"  [yellow]Could not kill PID {pid}: {e}[/yellow]")

    console.print(f"[green]Cleaned {len(pids)} aider process(es).[/green]")


# ── clean ─────────────────────────────────────────────────

@main.command()
@click.option("--yes", "confirmed", is_flag=True, help="Skip confirmation")
@click.pass_context
def clean(ctx, confirmed):
    """Remove all task files and state. Fresh start."""
    import shutil as _sh
    cfg = _cfg(ctx.obj)

    if not cfg.tasks_dir.exists():
        console.print("[yellow]Nothing to clean.[/yellow]")
        return

    from .tasks import _load_state
    state = _load_state(cfg)
    n_tasks = len(state.get("finished", {}))
    n_files = sum(1 for d in cfg.tasks_dir.iterdir() if d.is_dir()
                  for _ in d.glob("*.md"))

    if not confirmed:
        console.print(f"[bold red]This will delete:[/bold red]")
        console.print(f"  • {n_files} task files")
        console.print(f"  • {n_tasks} state entries")
        console.print(f"  • All logs in .usta/logs/")
        choice = console.input("\n[bold]Continue? [y/N][/bold] ").strip().lower()
        if choice != "y":
            console.print("[dim]Cancelled.[/dim]")
            return

    # Remove task dirs (keep .usta/ itself)
    for item in cfg.tasks_dir.iterdir():
        if item.is_dir():
            _sh.rmtree(item)
        elif item.name == "state.json":
            item.unlink()

    # Remove logs
    log_dir = cfg.project_dir / ".usta" / "logs"
    if log_dir.exists():
        _sh.rmtree(log_dir)

    console.print(f"[green]Cleaned {n_files} tasks + {n_tasks} state entries + logs.[/green]")
    console.print("[dim]Run 'usta plan' to start fresh.[/dim]")


# ── autopilot ─────────────────────────────────────────────

AUTOPILOT_META_PROMPT = """You are a relentless autonomous researcher/developer improving a codebase.
You are given a FOCUS AREA and the project's current state. Generate 1-3 small,
high-value tasks that advance the focus. Each task must be independent and completable
by a downstream AI coder in under 5 minutes.

RULES:
- Prioritize: bug fixes > missing tests > refactors > features.
- Never repeat a task that was already tried (see HISTORY below).
- Keep tasks SMALL: 1-3 files max. One concern per task.
- If you truly can't find anything useful to do for the focus area, output an
  empty JSON array []. This will end the autopilot run gracefully.
- Return ONLY a valid JSON array, no markdown fences.
[{{"id":"01","title":"...","filename":"01_xxx.md","content":"...","files":[...],"depends_on":[]}}]
"""


@main.command()
@click.argument("focus")
@click.option("--max-rounds", "max_rounds", default=0, type=int,
              help="Max experiment rounds (0 = infinite)")
@click.option("--test-cmd", "test_cmd", default=None,
              help="Test command to run after each round (e.g. 'pytest tests/ -q')")
@click.option("--max-cost", "max_cost", default=0.0, type=float,
              help="Budget cap in USD (0 = unlimited). Stops when exceeded.")
@click.pass_context
def autopilot(ctx, focus, max_rounds, test_cmd, max_cost):
    """Autonomous overnight mode — plan → apply → review → keep/discard, forever.

    \b
    Inspired by Karpathy's autoresearch: leave it running while you sleep.
    FOCUS is what to improve (e.g. "test coverage", "error handling", "performance").

    \b
    Examples:
      usta --auto autopilot "improve test coverage and edge cases"
      usta --auto autopilot "refactor for clarity and reduce complexity" --max-rounds 10
      usta --auto autopilot "add missing error handling" --test-cmd "pytest tests/ -q"
      usta --auto autopilot "improve performance" --max-cost 5.00
    """
    import signal as _sig
    import subprocess as sp

    print_banner()
    reset_session_usage()
    # Also reset OpenRouter usage tracker
    try:
        from .openrouter import reset_session_usage as reset_or_usage
        reset_or_usage()
    except Exception:
        pass
    cfg = _cfg(ctx.obj)
    cfg.auto_mode = True  # force auto mode

    console.print(f"[bold blue]AUTOPILOT MODE[/bold blue]")
    console.print(f"[bold blue]Focus:[/bold blue] {focus}")
    console.print(f"[dim]Project:[/dim]   {cfg.project_dir}")
    console.print(f"[dim]Rounds:[/dim]    {'∞ (until interrupted)' if max_rounds == 0 else max_rounds}")
    if test_cmd:
        console.print(f"[dim]Test cmd:[/dim]  {test_cmd}")
    if max_cost > 0:
        console.print(f"[dim]Budget:[/dim]   ${max_cost:.2f}")
    console.print()
    console.print(
        "[yellow bold]⚡ AUTONOMOUS MODE — will run indefinitely until you "
        "press Ctrl+C or it runs out of ideas.[/yellow bold]\n"
    )

    # ── Results log ───────────────────────────────────────
    log_path = cfg.project_dir / ".usta" / "autopilot.tsv"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.exists():
        log_path.write_text("round\tcommit\ttasks\tpassed\tfailed\tdiscarded\tstatus\tdescription\tcost_usd\n")

    # ── Cost tracking ─────────────────────────────────────
    _total_cost = [0.0]

    def _track_cost():
        """Pull cumulative cost from both Claude and OpenRouter usage trackers."""
        cost = 0.0
        try:
            from .claude import get_session_usage
            cost += get_session_usage().cost_usd
        except Exception:
            pass
        try:
            from .openrouter import get_session_usage as get_or_usage
            cost += get_or_usage().cost_usd
        except Exception:
            pass
        _total_cost[0] = cost
        return cost

    def _budget_exceeded():
        if max_cost <= 0:
            return False
        return _track_cost() > max_cost

    def _log_round(rnd, commit, n_tasks, n_pass, n_fail, n_discard, status, desc):
        cost = _track_cost()
        with open(log_path, "a") as f:
            f.write(f"{rnd}\t{commit}\t{n_tasks}\t{n_pass}\t{n_fail}\t{n_discard}\t{status}\t{desc}\t{cost:.4f}\n")

    def _git_head():
        r = sp.run(["git", "rev-parse", "--short", "HEAD"],
                   capture_output=True, text=True, cwd=str(cfg.project_dir), timeout=5)
        return r.stdout.strip() if r.returncode == 0 else "unknown"

    def _git_reset_to(commit):
        sp.run(["git", "reset", "--hard", commit],
               capture_output=True, text=True, cwd=str(cfg.project_dir), timeout=10)

    def _git_stash_save(name):
        """Stash current dirty state with a label. Returns True if something was stashed."""
        r = sp.run(["git", "stash", "push", "-m", name],
                   capture_output=True, text=True, cwd=str(cfg.project_dir), timeout=10)
        return "No local changes" not in (r.stdout + r.stderr)

    def _git_stash_pop():
        sp.run(["git", "stash", "pop"],
               capture_output=True, text=True, cwd=str(cfg.project_dir), timeout=10)

    def _run_tests():
        if not test_cmd:
            return True, ""
        try:
            r = sp.run(test_cmd, shell=True, capture_output=True, text=True,
                       cwd=str(cfg.project_dir), timeout=120)
            output = (r.stdout + r.stderr)[-500:]
            return r.returncode == 0, output
        except Exception as e:
            return False, str(e)

    # ── History + lessons tracking ─────────────────────────
    history: list[str] = []

    def _record_lessons(reviews: dict, tasks: list):
        """Write lessons from failed/warned reviews to lessons.jsonl."""
        try:
            from .lessons import harvest_from_review, record_lessons
            for t in tasks:
                rev = reviews.get(t.id, {})
                msgs = harvest_from_review(rev)
                if msgs:
                    record_lessons(cfg.project_dir, msgs, task_hint=t.title)
        except Exception:
            pass

    def _record_test_failure_lesson(test_output: str, task_titles: str):
        """Write a lesson when tests fail despite passing review."""
        try:
            from .lessons import record_lessons
            # Extract FAILED lines from pytest output
            failed_lines = [
                ln.strip() for ln in test_output.splitlines()
                if ln.strip().startswith("FAILED ") or "Error" in ln
            ]
            msgs = failed_lines[:5] if failed_lines else [f"Tests failed after review-pass for: {task_titles[:120]}"]
            record_lessons(cfg.project_dir, msgs, task_hint="autopilot-test-gate", source="verifier")
        except Exception:
            pass

    def _build_objective(rnd):
        hist_block = ""
        if history:
            hist_block = "\n\nHISTORY (do NOT repeat these):\n" + "\n".join(
                f"  - Round {i+1}: {h}" for i, h in enumerate(history[-20:])
            )
        return (
            f"Focus area: {focus}\n"
            f"This is autopilot round {rnd}. Find 1-3 small improvements "
            f"to make in this codebase. Be creative and diverse — mix bug "
            f"fixes, tests, refactors, and small features."
            f"{hist_block}"
        )

    # ── Interrupt handler ─────────────────────────────────
    _stop = False
    _orig = _sig.getsignal(_sig.SIGINT)
    def _on_int(sig, frame):
        nonlocal _stop
        _stop = True
        console.print("\n[yellow]Interrupt received — finishing current round...[/yellow]")
    _sig.signal(_sig.SIGINT, _on_int)

    # ── Main loop ─────────────────────────────────────────
    from .planner import plan as do_plan
    from .runner import run_task as do_run
    from .reviewer import review_batch

    round_num = 0
    total_kept = 0
    total_discarded = 0

    try:
        while not _stop:
            round_num += 1
            if max_rounds > 0 and round_num > max_rounds:
                console.print(f"\n[green]Reached max rounds ({max_rounds}). Stopping.[/green]")
                break

            # Budget check
            if _budget_exceeded():
                console.print(f"\n[yellow]Budget exceeded (${_total_cost[0]:.2f} / ${max_cost:.2f}). Stopping.[/yellow]")
                break

            console.print(f"\n{'='*60}")
            console.print(f"[bold cyan]AUTOPILOT ROUND {round_num}[/bold cyan]")
            if max_cost > 0:
                console.print(f"[dim]  Budget: ${_track_cost():.2f} / ${max_cost:.2f}[/dim]")
            console.print(f"{'='*60}\n")

            # Snapshot HEAD for potential rollback
            snapshot = _git_head()

            # ── Step 1: Plan ──────────────────────────────
            console.print("[bold]Step 1: Planning with Opus...[/bold]\n")
            objective = _build_objective(round_num)
            try:
                tasks, resp, is_new = do_plan(objective, cfg)
            except Exception as plan_err:
                console.print(f"[red]Planning crashed: {plan_err}[/red]")
                _log_round(round_num, snapshot, 0, 0, 0, 0, "error", f"plan crash: {plan_err}")
                history.append(f"ERROR (plan): {plan_err}")
                continue

            if not tasks:
                console.print("[yellow]Opus couldn't find anything to do. Stopping autopilot.[/yellow]")
                _log_round(round_num, snapshot, 0, 0, 0, 0, "empty", "no tasks generated")
                break

            task_titles = "; ".join(t.title for t in tasks)
            console.print(f"\n[green]{len(tasks)} tasks planned:[/green]")
            for t in tasks:
                console.print(f"  [cyan]{t.id}[/cyan]. {t.title} [dim]({len(t.files)} files)[/dim]")
            console.print()

            # Save tasks
            for t in tasks:
                save_task(t, cfg)

            if _stop:
                break

            # ── Step 2: Execute (parallel) ────────────────
            console.print("[bold]Step 2: Executing with Qwen...[/bold]\n")

            # Check for dependencies — tasks with depends_on run after their deps
            independent = [t for t in tasks if not t.depends_on]
            dependent = [t for t in tasks if t.depends_on]

            def _exec_batch(batch):
                """Execute a batch of tasks in parallel threads."""
                if not batch or _stop:
                    return
                threads = []
                timings = {}
                for t in batch:
                    def _worker(task=t):
                        t0w = time.time()
                        try:
                            do_run(task, cfg)
                        except Exception as e:
                            task.status = "fail"
                            task.error = str(e)
                        timings[task.id] = time.time() - t0w
                    thr = threading.Thread(target=_worker, daemon=True)
                    threads.append((thr, t))
                    thr.start()

                for thr, t in threads:
                    thr.join(timeout=300)  # 5 min max per task
                    if thr.is_alive():
                        t.status = "fail"
                        t.error = "timed out (300s)"

                for t in batch:
                    elapsed = timings.get(t.id, 0)
                    if t.status == "done":
                        console.print(f"  [green]✓[/green] {t.id}. {t.title} [dim]({elapsed:.0f}s)[/dim]")
                    else:
                        console.print(f"  [red]✗[/red] {t.id}. {t.title}: {t.error} [dim]({elapsed:.0f}s)[/dim]")

            # Run independent tasks in parallel
            if len(independent) > 1:
                console.print(f"  [dim]Running {len(independent)} independent tasks in parallel...[/dim]")
            _exec_batch(independent)

            # Run dependent tasks sequentially
            for t in dependent:
                if _stop:
                    break
                _exec_batch([t])

            succeeded = [t for t in tasks if t.status == "done"]
            failed_exec = [t for t in tasks if t.status == "fail"]

            if not succeeded:
                console.print("[red]All tasks failed execution. Discarding round.[/red]")
                _git_reset_to(snapshot)
                total_discarded += len(tasks)
                history.append(f"FAILED (exec): {task_titles}")
                _log_round(round_num, snapshot, len(tasks), 0, len(tasks), len(tasks),
                           "discard", f"all exec failed: {task_titles[:100]}")
                continue

            if _stop:
                break

            # ── Step 2.5: Pre-test gate ───────────────────
            # Run tests BEFORE review to catch runtime failures early.
            # This saves an expensive Opus review call on broken code.
            _auto_commit_if_dirty(cfg)
            if test_cmd and not _stop:
                console.print(f"\n[bold]Step 2.5: Pre-review test gate...[/bold]")
                pre_ok, pre_output = _run_tests()
                if pre_ok:
                    console.print(f"  [green]✓ Pre-test passed[/green]")
                else:
                    console.print(f"  [red]✗ Pre-test failed — skipping review, discarding round[/red]")
                    if pre_output:
                        console.print(f"  [dim]{pre_output[-200:]}[/dim]")
                    _record_test_failure_lesson(pre_output, task_titles)
                    _git_reset_to(snapshot)
                    total_discarded += len(tasks)
                    history.append(f"FAILED (pre-test): {task_titles}")
                    _log_round(round_num, snapshot, len(tasks), 0, len(tasks), len(tasks),
                               "discard", f"pre-test failed: {task_titles[:100]}")
                    continue

            # ── Step 3: Review ────────────────────────────
            console.print(f"\n[bold]Step 3: Reviewing {len(succeeded)} task(s) with Opus...[/bold]\n")

            try:
                round_reviews, resp = review_batch(succeeded, cfg)
            except Exception as rev_err:
                console.print(f"[red]Review crashed: {rev_err} — discarding round.[/red]")
                _git_reset_to(snapshot)
                total_discarded += len(tasks)
                history.append(f"ERROR (review): {rev_err}")
                _log_round(round_num, snapshot, len(tasks), 0, 0, len(tasks),
                           "error", f"review crash: {rev_err}")
                continue

            for t in succeeded:
                rev = round_reviews.get(t.id, {"verdict": "pass", "summary": "?"})
                v = rev.get("verdict", "unknown")
                icon = "✅" if v == "pass" else ("⚠️" if v == "warn" else "❌")
                console.print(f"  {icon} {t.id}. {t.title}: {rev.get('summary','')[:80]}")

            # Record lessons from reviews (pass or fail)
            _record_lessons(round_reviews, succeeded)

            passed = [t for t in succeeded
                      if round_reviews.get(t.id, {}).get("verdict") in ("pass", "warn")]
            review_failed = [t for t in succeeded
                            if round_reviews.get(t.id, {}).get("verdict") == "fail"]

            # ── Step 4: Auto-fix failed reviews (1 round) ─
            if review_failed and not _stop:
                fixable = {
                    t.id: round_reviews[t.id] for t in review_failed
                    if round_reviews.get(t.id, {}).get("fix_instructions")
                }
                if fixable:
                    console.print(f"\n[yellow]Auto-fixing {len(fixable)} task(s)...[/yellow]")
                    _auto_fix(fixable, tasks, cfg)
                    # Re-review fixed tasks
                    re_review_tasks = [t for t in review_failed if t.id in fixable]
                    if re_review_tasks:
                        _auto_commit_if_dirty(cfg)
                        try:
                            rr2, _ = review_batch(re_review_tasks, cfg)
                        except Exception:
                            rr2 = {}
                        for t in re_review_tasks:
                            rev2 = rr2.get(t.id, {})
                            if rev2.get("verdict") in ("pass", "warn"):
                                passed.append(t)
                                review_failed.remove(t)
                                console.print(f"  [green]✓ Fixed: {t.id}. {t.title}[/green]")
                            else:
                                console.print(f"  [red]✗ Still failing: {t.id}. {t.title}[/red]")

            # ── Step 5: Test gate (final) ─────────────────
            if passed and test_cmd and not _stop:
                _auto_commit_if_dirty(cfg)
                console.print(f"\n[bold]Step 5: Final test gate...[/bold]")
                test_ok, test_output = _run_tests()
                if test_ok:
                    console.print(f"  [green]✓ Tests passed[/green]")
                else:
                    console.print(f"  [red]✗ Tests failed — discarding entire round[/red]")
                    if test_output:
                        console.print(f"  [dim]{test_output[-200:]}[/dim]")
                    _record_test_failure_lesson(test_output, task_titles)
                    _git_reset_to(snapshot)
                    total_discarded += len(tasks)
                    history.append(f"FAILED (tests): {task_titles}")
                    _log_round(round_num, snapshot, len(tasks), 0, len(tasks), len(tasks),
                               "discard", f"tests failed: {task_titles[:100]}")
                    continue

            # ── Step 6: Keep/Discard decision ─────────────
            new_head = _git_head()
            n_pass = len(passed)
            n_fail = len(failed_exec) + len(review_failed)
            n_discard = 0

            if not passed:
                # Nothing passed — full rollback
                console.print("[red]No tasks passed review. Discarding round.[/red]")
                _git_reset_to(snapshot)
                n_discard = len(tasks)
                total_discarded += n_discard
                history.append(f"DISCARDED: {task_titles}")
                _log_round(round_num, new_head, len(tasks), 0, n_fail, n_discard,
                           "discard", task_titles[:100])
            else:
                # At least some passed — keep the round
                total_kept += n_pass
                total_discarded += n_fail
                _auto_commit_if_dirty(cfg)
                console.print(
                    f"\n[bold green]Round {round_num}: kept {n_pass} task(s), "
                    f"discarded {n_fail}[/bold green]"
                )
                history.append(f"KEPT({n_pass}): {task_titles}")
                _log_round(round_num, new_head, len(tasks), n_pass, n_fail, n_fail,
                           "keep", task_titles[:100])

            console.print(
                f"[dim]Running total: {total_kept} kept, "
                f"{total_discarded} discarded across {round_num} rounds"
                f" | cost: ${_track_cost():.2f}[/dim]"
            )

    finally:
        _sig.signal(_sig.SIGINT, _orig)

    # ── Final summary ─────────────────────────────────────
    final_cost = _track_cost()
    console.print(f"\n{'='*60}")
    console.print(f"[bold green]AUTOPILOT COMPLETE[/bold green]")
    console.print(f"{'='*60}")
    console.print(f"  Rounds:    {round_num}")
    console.print(f"  Kept:      {total_kept}")
    console.print(f"  Discarded: {total_discarded}")
    console.print(f"  Cost:      ${final_cost:.2f}")
    console.print(f"  Log:       {log_path}")
    console.print()



if __name__ == "__main__":
    main()
