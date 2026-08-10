"""Gate: no doc may PRESCRIBE a practice the atlas forbids.

The claim verifier next door checks whether a doc's statements are TRUE —
paths resolve, commands exist, ports are declared. A doc can pass all of that
and still be wrong: a banned command is a real command with a real name, and
the sin is telling someone to run it. Subordinate atlases have taught the
literal opposite of the governing one for weeks while every date was fresh and
every claim resolved.

Two properties are pinned here, and they fail for different reasons:

1. THE BAN LIST IS DERIVED, NOT DECLARED. ``atlas_bans`` parses the ban out of
   the atlas sentence a human already writes. A hard-coded list in the checker
   would be a second source of truth, free to drift from the first — the exact
   disease this system exists to cure.

2. DECLARING A BAN IS NOT VIOLATING IT. A checker that cannot tell a
   prohibition from an order either floods the report with false alarms or
   forbids the rule from writing itself down.

The fixtures below use a synthetic atlas on purpose. Pointing framework tests
at a real product's atlas would make the framework's own suite fail whenever a
product edited its prose.
"""

from __future__ import annotations

from cara.docs.Claims import IGNORE_LINE, atlas_bans, check_forbidden

from ._fixtures import make_checkout, write

BAN_SENTENCE = "`schema:nuke` is FORBIDDEN. Use `craft schema:rebuild` instead.\n"

# Every one of these ORDERS the reader to run a banned command.
PRESCRIPTIONS = [
    "Run: `python craft schema:nuke && python craft migrate`",
    "cd services && python craft schema:nuke && python craft migrate",
    "To rebuild the schema run `craft schema:nuke` then `craft migrate`.",
    "    ./venv/bin/python craft schema:nuke",
    # Names the damage and still gives the order — the most dangerous form,
    # and the one an over-broad negation list quietly swallowed once.
    "Rebuild with `craft schema:nuke`; it erases live credentials.",
]

# Every one of these TALKS ABOUT the ban. All must stay legal, or the rule
# cannot be written down anywhere — including in the atlas that declares it.
DECLARATIONS = [
    "`schema:nuke` is FORBIDDEN. Use `craft schema:rebuild` instead.",
    "`schema:nuke` YASAK (it destroys encrypted credentials).",
    "Never run `craft schema:nuke` — it destroys encrypted credentials.",
    "Do NOT use `craft schema:nuke`; prefer `craft schema:rebuild`.",
    "python craft schema:rebuild    # rebuild the dev DB (schema:nuke is FORBIDDEN)",
    "`schema:nuke` is forbidden: it erases live credentials.",
    # Unrelated commands that merely share a prefix or a neighbourhood.
    "python craft migrate",
    "Run `craft schema:check` and `craft migrations:check` after regenerating.",
]


def _bans(tmp_path):
    root = make_checkout(tmp_path, "alpha", atlas=BAN_SENTENCE)
    return root, atlas_bans(root)


def test_the_ban_list_is_parsed_from_the_atlas(tmp_path):
    _root, bans = _bans(tmp_path)

    assert "schema:nuke" in bans
    assert bans["schema:nuke"] == "CLAUDE.md"


def test_an_atlas_that_declares_nothing_yields_no_bans(tmp_path):
    """Green-by-omission is the failure this whole system was built against.

    The parser reporting an empty map is not itself the guard — the verifier
    turns an empty map into a BROKEN finding, so deleting the atlas sentence
    fails loudly instead of silently disabling the check.
    """
    root = make_checkout(tmp_path, "alpha", atlas="Nothing is banned here.\n")

    assert atlas_bans(root) == {}


def test_a_ban_declared_by_a_neighbours_shared_atlas_still_binds(tmp_path):
    alpha = make_checkout(tmp_path, "alpha", atlas="Nothing local.\n")
    make_checkout(tmp_path, "beta", atlas=BAN_SENTENCE)

    bans = atlas_bans(alpha)

    assert "schema:nuke" in bans
    assert bans["schema:nuke"].endswith("beta.example/code/CLAUDE.md")


def test_prescriptions_are_flagged(tmp_path):
    _root, bans = _bans(tmp_path)
    for line in PRESCRIPTIONS:
        assert check_forbidden("probe.md", [line], bans), (
            f"forbidden-practice check missed a prescription: {line!r}"
        )


def test_declarations_are_never_flagged(tmp_path):
    _root, bans = _bans(tmp_path)
    for line in DECLARATIONS:
        assert not check_forbidden("probe.md", [line], bans), (
            f"false alarm — this line states or respects the ban, "
            f"it does not issue it: {line!r}"
        )


def test_an_ignore_marker_still_opts_a_line_out(tmp_path):
    _root, bans = _bans(tmp_path)
    line = f"Run `craft schema:nuke` <!-- {IGNORE_LINE} -->"

    assert not check_forbidden("probe.md", [line], bans)


def test_a_fenced_snippet_is_read_here_unlike_every_other_claim(tmp_path):
    """A paste-ready snippet is the most dangerous place for a forbidden order,
    not the safest: a reader copies it without reading the prose around it."""
    _root, bans = _bans(tmp_path)
    lines = ["```bash", "python craft schema:nuke", "```"]

    findings = check_forbidden("probe.md", lines, bans)

    assert [row[1] for row in findings] == [2]


def test_a_command_that_merely_shares_a_prefix_is_not_a_violation(tmp_path):
    _root, bans = _bans(tmp_path)

    assert not check_forbidden("probe.md", ["python craft schema:nuke-all"], bans)


def test_the_engine_reads_the_atlas_that_ships_with_the_checkout(tmp_path):
    """The ban list follows the checkout, never a path baked into the code."""
    root = make_checkout(tmp_path, "alpha", atlas="Nothing local.\n")
    write(root / "CLAUDE.md", "`other:command` is BANNED.\n")

    assert set(atlas_bans(root)) == {"other:command"}
