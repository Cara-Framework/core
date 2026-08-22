"""Claim verification: does the prose still describe the checkout it ships in?

Freshness only compares DATES: a doc can name a file, a command or a port that
never existed and stay green forever. This pass reads the assertions instead —
every mechanically checkable claim in every markdown the checkout owns.

DESIGN RULE — zero false positives beats coverage. A checker that cries wolf
gets ignored, and then the gate is theatre. Every extraction that is even
slightly ambiguous lands in UNVERIFIABLE (informational) instead of BROKEN
(fails the build). Concretely: fenced code blocks are skipped entirely (they
hold illustrative snippets), only backticked spans are read for paths, a bare
``craft <word>`` is never failed (ordinary prose right after the word "craft"
would extract as a command name), and a port claim can never fail — ports are
reported, never judged.
"""

from __future__ import annotations

import re
from pathlib import Path

from cara.docs.ClaimSources import (
    _owned_markdowns,
    check_path_claim,
    declared_ports,
    sibling_roots,
    strip_fences,
)
from cara.docs.DocsManifest import DocsManifest
from cara.docs.Inventory import command_rows
from cara.docs.Support import Say, md_escape, read

IGNORE_FILE = "docs-check: ignore-file"
IGNORE_LINE = "docs-check: ignore"

INLINE_CODE = re.compile(r"`([^`\n]+)`")
TILDE_RE = re.compile(r"~/[A-Za-z0-9._\-/]*[A-Za-z0-9._\-]")
CRAFT_RE = re.compile(r"\bcraft\s+([a-z][a-z0-9_.\-]*(?::[a-z0-9_.:\-]+)*)")
PORT_RE = re.compile(r"(?::(\d{4})\b|\bport[ =](\d{4})\b)", re.I)

Finding = tuple[str, int, str, str, str]


def command_names(root: Path) -> set[str]:
    """Every craft command name registered in ``root``.

    The app-level ones (the same inventory the reference page publishes) plus
    the framework's built-ins, which declare their names in class bodies.
    """
    names = {row[3]["name"] for row in command_rows(root)[1]}
    for framework_dir in (
        root / "commons" / "cara" / "cara" / "commands",
        root / "services" / "cara" / "commands",
        root / "api" / "cara" / "commands",
    ):
        if framework_dir.is_dir():
            for f in framework_dir.rglob("*.py"):
                names |= set(
                    re.findall(r'name\s*=\s*["\']([a-z][a-z0-9_:.\-]*)["\']', read(f))
                )
            break
    return names


def owned_markdowns(manifest: DocsManifest) -> list[Path]:
    """Hand-maintained markdown this product is answerable for."""
    return _owned_markdowns(
        manifest.root, manifest.docs, manifest.reference, manifest.product
    )


# ---------------------------------------------------------------- forbidden practice
#
# A doc can be flawlessly ACCURATE and still be WRONG. Every path resolves,
# every command exists, every port is declared — and the page still tells you
# to run something the atlas bans. A banned command is a real command with a
# real name; the sin is prescribing it, and subordinate docs have done exactly
# that while the whole claim verifier stayed green.
#
# So this pass checks a different property: not "is the statement true?" but
# "does a subordinate doc contradict the rule the atlas laid down?".
#
# THE BAN LIST IS NOT A CONSTANT HERE. It is PARSED OUT OF THE ATLAS, the same
# sentence a human reads. A hard-coded list would be a second source of truth
# that silently drifts from the first — the very disease this whole system
# exists to cure. The rule lives in one place; the checker reads it. Add a ban
# by writing it in the atlas, nowhere else.

# `<command:with:colon>` followed, within one clause, by a prohibition word.
# The backticks + the mandatory colon keep this off ordinary prose: it only
# ever fires on a command-shaped token the atlas explicitly bans.
BAN_DECL_RE = re.compile(
    r"`([a-z][a-z0-9_.\-]*(?::[a-z0-9_.\-]+)+)`[^`\n]{0,40}?"
    r"\b(?:YASAK|YASAKTIR|FORBIDDEN|BANNED|PROHIBITED)\b",
    re.IGNORECASE,
)


# Prescriptive form: an actual invocation — `craft <cmd>`, `python craft
# <cmd>`, `./venv/bin/python craft <cmd>`. The runner word is the anchor, so
# merely NAMING the command ("`x:y` is FORBIDDEN") can never match. That
# asymmetry is what lets the rule state itself without self-incriminating.
def invocation_re(command: str) -> re.Pattern[str]:
    """Pattern matching a real invocation of ``command``, never a mention."""
    return re.compile(r"(?<![\w:.\-])craft\s+" + re.escape(command) + r"(?![\w:.\-])")


# Any PROHIBITION marker on the line exempts it: the line is TALKING about the
# ban, not issuing an order. Bilingual. The atlas sentence, "never run `craft
# x:y`", every "instead of" contrast and every "do NOT" land here.
#
# Strictly prohibition vocabulary — NOT consequence vocabulary. "destroys",
# "erases", "wipes" were in this list for one iteration and swallowed a live
# mutation ("Rebuild with `craft x:y`; it erases live credentials"):
# describing the damage is not forbidding it, and a doc that names the harm
# while still ordering the act is the most dangerous kind, not the safest.
NEGATION_RE = re.compile(
    r"\b(?:forbidden|forbids?|yasak\w*|banned|prohibited|never|asla|avoid|"
    r"do not|does not|don'?t|must not|cannot|can't|no longer|not\b|"
    r"instead|yerine|kullanma\w*|çalıştırma\w*|deprecated|unsupported)",
    re.IGNORECASE,
)


def atlas_bans(root: Path) -> dict[str, str]:
    """{forbidden command -> the atlas that bans it}.

    Read from the checkout's own atlas plus any shared neighbouring atlas,
    because an atlas may speak for more than one checkout — a ban declared in
    one is a ban in the other.
    """
    bans: dict[str, str] = {}
    atlases = [root / "CLAUDE.md"] + [s / "CLAUDE.md" for s in sibling_roots(root)]
    for atlas in atlases:
        if not atlas.exists():
            continue
        label = str(atlas) if root not in atlas.parents else str(atlas.relative_to(root))
        for command in BAN_DECL_RE.findall(read(atlas)):
            bans.setdefault(command, label)
    return bans


def check_forbidden(
    label: str,
    raw_lines: list[str],
    bans: dict[str, str],
) -> list[Finding]:
    """BROKEN rows for every line that PRESCRIBES a banned command.

    Fenced blocks are read here — unlike every other claim. A paste-ready
    snippet is the most dangerous place for a forbidden order, not the safest:
    a reader copies it without reading the prose around it.
    """
    out: list[Finding] = []
    for number, line in enumerate(raw_lines, 1):
        if IGNORE_LINE in line or line.lstrip().startswith("```"):
            continue
        if NEGATION_RE.search(line):
            continue
        for command, origin in bans.items():
            if invocation_re(command).search(line):
                out.append(
                    (
                        label,
                        number,
                        "forbidden",
                        f"craft {command}",
                        f"prescribes a practice the atlas forbids ({origin})",
                    )
                )
    return out


def verify_claims(manifest: DocsManifest, say: Say) -> tuple[list, list]:
    """Resolve every checkable claim in this product's markdown corpus."""
    root = manifest.root
    broken: list[Finding] = []
    unverifiable: list[Finding] = []
    siblings = sibling_roots(root)
    inventories = {root: command_names(root)}
    ports_here = declared_ports(root, manifest.port_source_dirs)
    bans = atlas_bans(root)
    if not bans:
        # Green-by-omission is the failure mode this whole system was built
        # against: delete the atlas sentence and the forbidden-practice check
        # quietly becomes a no-op that passes forever. It fails loudly instead.
        broken.append(
            (
                "CLAUDE.md",
                0,
                "forbidden",
                "<ban list>",
                "the atlas declares no forbidden practices — the check would be "
                "a no-op; state bans as `command:name` YASAK/FORBIDDEN",
            )
        )

    for doc in owned_markdowns(manifest):
        text = read(doc)
        if IGNORE_FILE in text:
            continue
        label = str(doc.relative_to(root)) if root in doc.parents else str(doc)
        broken += check_forbidden(label, text.splitlines(), bans)
        # A doc that names another checkout may legitimately cite it, so a
        # neighbour stays a resolution root — but only as a DOWNGRADE, never as
        # proof. Resolving a claim next door alone once turned an atlas
        # sentence about a guard THIS product does not carry into a green tick,
        # because a file of that name existed in the neighbour and the
        # neighbour was mentioned somewhere else in the document. Paths follow
        # the command rule below: absent here + present there = unverifiable,
        # not ok.
        roots = [root] + [
            s
            for s in siblings
            if s.parent.name in text or s.parent.name.split(".")[0] in text
        ]
        # A neighbour that is NOT checked out cannot answer the question
        # either way. Locally the shape above finds it and the claim reads
        # "resolves in <product>, not <this one>"; CI checks out ONE product,
        # so the same sentence flipped to BROKEN — a verdict about the
        # environment, not about the sentence. When the document names a
        # declared sibling and no sibling tree is present, say what is
        # actually true: unverifiable here.
        absent_sibling = next(
            (
                name
                for name in manifest.sibling_products
                if not siblings
                and (name in text or name.split(".")[0] in text)
            ),
            None,
        )
        for number, line in enumerate(strip_fences(text), 1):
            if IGNORE_LINE in line or not line.strip():
                continue
            for span in INLINE_CODE.findall(line):
                for token in re.split(r"[\s,;|]+", span):
                    verdict = check_path_claim(token, [root])
                    if verdict is None:
                        continue
                    kind, reason = verdict
                    if kind == "broken":
                        elsewhere = next(
                            (
                                sibling.parent.name
                                for sibling in roots[1:]
                                if check_path_claim(token, [sibling]) == ("ok", "")
                            ),
                            None,
                        )
                        if elsewhere:
                            unverifiable.append(
                                (
                                    label,
                                    number,
                                    "path",
                                    token,
                                    f"resolves in {elsewhere}, not {manifest.product}",
                                )
                            )
                        elif absent_sibling:
                            unverifiable.append(
                                (
                                    label,
                                    number,
                                    "path",
                                    token,
                                    f"names {absent_sibling}, not checked out here",
                                )
                            )
                        else:
                            broken.append((label, number, "path", token, reason))
                    elif kind == "unverifiable":
                        unverifiable.append((label, number, "path", token, reason))
            for match in TILDE_RE.finditer(line):
                pointer = match.group(0).rstrip(".,;:)")
                # The regex stops at a glob char, so a pointer ending in `*.md`
                # would otherwise be judged on its truncated prefix and
                # reported broken. Check what follows.
                if line[match.end() : match.end() + 1] in ("*", "?", "["):
                    unverifiable.append(
                        (label, number, "pointer", pointer + "…", "glob pointer")
                    )
                elif not Path(pointer).expanduser().exists():
                    # A workspace pointer into a neighbour product is the same
                    # situation as the path branch above: absent here is not
                    # the same fact as wrong.
                    into_sibling = next(
                        (
                            name
                            for name in manifest.sibling_products
                            if name in pointer
                        ),
                        None,
                    )
                    if into_sibling and not siblings:
                        unverifiable.append(
                            (
                                label,
                                number,
                                "pointer",
                                pointer,
                                f"points into {into_sibling}, not checked out here",
                            )
                        )
                    else:
                        broken.append(
                            (label, number, "pointer", pointer, "does not exist on disk")
                        )
            for command in CRAFT_RE.findall(line):
                if ":" not in command:
                    # Bare word after "craft": prose extracts as a command
                    # name far too easily. Never failed — only confirmed.
                    if command not in inventories[root]:
                        unverifiable.append(
                            (
                                label,
                                number,
                                "command",
                                f"craft {command}",
                                "bare word, not a known command",
                            )
                        )
                    continue
                if command in inventories[root]:
                    continue
                elsewhere = None
                for sibling in roots[1:]:
                    inventories.setdefault(sibling, command_names(sibling))
                    if command in inventories[sibling]:
                        elsewhere = sibling.parent.name
                if elsewhere is None and absent_sibling:
                    elsewhere = f"{absent_sibling} (not checked out here)"
                if elsewhere:
                    unverifiable.append(
                        (
                            label,
                            number,
                            "command",
                            f"craft {command}",
                            f"belongs to {elsewhere}, not {manifest.product}",
                        )
                    )
                else:
                    broken.append(
                        (
                            label,
                            number,
                            "command",
                            f"craft {command}",
                            f"no such command in {manifest.product}",
                        )
                    )
            for first, second in PORT_RE.findall(line):
                port = first or second
                if port not in ports_here:
                    # Never BROKEN: ports live in gitignored .env files,
                    # tunnels and third-party dashboards. A false alarm here
                    # would train people to ignore the whole report.
                    unverifiable.append(
                        (label, number, "port", port, "not declared by this repo")
                    )

    for label, number, kind, claim, reason in broken:
        say(f"  ❌ BROKEN {label}:{number} [{kind}] `{claim}` — {reason}")
    say(f"Claims: {len(broken)} broken, {len(unverifiable)} unverifiable")
    return broken, unverifiable


def claims_report(
    manifest: DocsManifest,
    broken: list,
    unverifiable: list,
    now: str,
) -> str:
    """Render the claim-verification page."""
    lines = [
        f"<!-- GENERATED by `craft {manifest.generator_command}` -->",
        "",
        "# Claim verification",
        "",
        f"_Checked: {now} · every repo-owned markdown's paths, `~/` pointers, "
        "craft commands and ports, resolved against this checkout — plus every "
        "line that PRESCRIBES a practice the atlas forbids._",
        "",
        "A `forbidden` finding is not about accuracy: the command exists and the "
        "line is spelled right, but the atlas bans running it. Bans are parsed "
        "out of the atlas itself (a ``` `command:name` YASAK/FORBIDDEN ``` sentence), "
        "never hard-coded here — declaring or warning about a ban is always legal, "
        "ordering it is not.",
        "",
        "Mark a deliberately-nonexistent example with `<!-- docs-check: ignore -->` at the end of the line, or `<!-- docs-check: ignore-file -->` anywhere in the file.",
        "",
        f"## Broken ({len(broken)})",
        "",
    ]
    if broken:
        lines += ["| Doc | Line | Kind | Claim | Reason |", "|---|---|---|---|---|"]
        lines += [
            f"| `{d}` | {n} | {k} | `{md_escape(c)}` | {r} |" for d, n, k, c, r in broken
        ]
    else:
        lines.append("_None — every checkable claim resolves._")
    lines += [
        "",
        f"## Unverifiable ({len(unverifiable)})",
        "",
        "_Informational only; these never fail the gate._",
        "",
    ]
    if unverifiable:
        lines += ["| Doc | Line | Kind | Claim | Reason |", "|---|---|---|---|---|"]
        lines += [
            f"| `{d}` | {n} | {k} | `{md_escape(c)}` | {r} |"
            for d, n, k, c, r in unverifiable
        ]
    return "\n".join(lines) + "\n"


__all__ = [
    "BAN_DECL_RE",
    "CRAFT_RE",
    "IGNORE_FILE",
    "IGNORE_LINE",
    "INLINE_CODE",
    "NEGATION_RE",
    "PORT_RE",
    "TILDE_RE",
    "atlas_bans",
    "check_forbidden",
    "claims_report",
    "command_names",
    "invocation_re",
    "owned_markdowns",
    "verify_claims",
]
