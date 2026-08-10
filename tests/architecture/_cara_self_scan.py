"""Point the Guard Pack at cara's OWN source (DOCTRINE §11).

Leading underscore: a test-support module, not a test file (the
``_fixtures.py`` convention next door).

cara ships every scanner in ``cara/architecture/scanners/`` and drives them
from ``tests/architecture/`` — against SYNTHETIC ``tmp_path`` trees, every
one of them. For as long as the pack has existed, nothing has ever pointed a
scanner at the framework's own code. The measured result of never asking:
582 untagged function-local imports, 182 import-form violations, 126
source-shape violations, 58 incomplete barrels — in the repository that
ships the law those rules are written in. A framework that cannot survive
its own guard is not entitled to fail a product's build.

This module builds the ``Manifest`` for that scan and pins the debt it
finds. Two facts make the manifest possible at all, and both are recent:
``ManifestRoots.app_namespace`` / ``app_path_prefix`` (``ImportForm`` and
``SourceShape`` used to compare against the literal ``"app"``, so a
cara-rooted scan matched nothing and returned a vacuously clean PASS), and
the single module-object predicate in ``_ast_utils`` that the barrel writer
and the barrel reader now share.

WHICH SCANNERS RUN, AND WHY NOT THE REST
----------------------------------------
``SELF_SCANNED`` and ``NOT_APPLICABLE`` together must cover every key of
``scanners.REGISTRY`` — a new scanner cannot be added to the framework
without someone deciding, in writing, whether the framework obeys it. A
silent omission is how this whole blind spot started.

Included, because the rule is about SOURCE and applies to any Python tree:

* ``import_tiers`` — the four-tier order. cara's own absolute ``cara.*``
  imports land in the framework tier and its relative imports in the local
  tier, which is exactly what the tiers mean when the tree IS the framework.
* ``inline_imports`` — the ``# local:`` reason-tag law. The single largest
  debt below, and the one that most directly rots into superstition: 582
  lazy imports whose reason nobody wrote down.
* ``import_form`` — barrel-for-consumers / direct-for-siblings, with cara's
  top-level packages as the layers. The sibling arm is load-bearing here:
  a middleware reaching ``from cara.middleware import Middleware`` from
  inside ``cara/middleware/`` is a boot-order crash that survives only on
  hand-tuned barrel line order (§5.1).
* ``source_shape`` — file/class budgets. Note the EDGE-method arm scores
  zero and is not vacuous: ``_is_edge_path`` requires an edge layer directly
  under the app root, and cara nests its base classes at
  ``cara/http/controllers/`` and ``cara/events/jobs/``. Those are base
  classes, not flow edges. The rule stays armed for a future top-level
  ``cara/controllers/``.
* ``barrel_completeness`` — cara's barrels are §5.1 barrels like any other.

Excluded, each because the rule is PRODUCT-SHAPED — it asks a question the
framework has no answer to, and a scanner asked an inapplicable question
returns the vacuous PASS this module exists to abolish:

* ``domain_registry``, ``domain_ownership`` — §3 domains/flows. cara has no
  business domains; its packages are capabilities, and ``domains={}`` would
  make both scanners assert over an empty registry.
* ``write_ownership``, ``transaction_ownership``, ``raw_sql_home``,
  ``model_query_discipline`` — §7/§8 laws about a product's TABLES. cara
  owns no tables; it ships the ORM those laws are enforced with.
* ``vertical_slice_seams`` — §4 plugin/brand tokens. cara is brand-blind by
  construction and declares no ``plugin_tokens``.
* ``job_idempotency`` — every ``BaseJob`` subclass declares idempotency
  params. cara defines ``BaseJob``; it dispatches no product jobs.
* ``kernel_membership``, ``vendor_barrel_parity`` — §2 questions about
  ``commons/`` and the vendored production image. cara has no kernel and is
  never vendored INTO itself; it is the framework the vendor step
  materializes alongside one.
* ``flow_law``, ``port_membership`` — §5 controller/job edge and ``ports``
  layer shapes. cara has neither layer; see the edge note above.
* ``http_in_business_logic`` — "transport types stop at the edge". cara IS
  the transport: ``cara.http`` is the module the rule names.
* ``env_read_discipline`` — "the environment is read in ``config/`` only".
  cara has no ``config/``; ``cara/environment/`` is the reader that rule
  points products AT.
Nothing is deferred any more. ``collaborator_calls``, ``barrel_mid_load`` and
``silent_except_swallow`` were once excluded with "needs its own census pass";
all three have since had it, and all three came back EMPTY — so they are
scanned with a ``{}`` pin rather than carried as debt:

* ``collaborator_calls`` — ``ResponseFactory`` stopped declaring a
  collaborator type it does not call against. The five findings were that one
  mismatch.
* ``barrel_mid_load`` — ``ConnectionResolver`` and ``AMQPDriver`` stopped
  reaching through their own package barrels (§5.1).
* ``silent_except_swallow`` — every one of the 23 handlers turned out to be a
  deliberate best-effort path (telemetry that must not break the work it
  measures, logging during bootstrap before the Log facade is bound, optional
  dependencies, advisory headers). None was a hidden failure, so each carries
  a ``# allow-silent-except:`` tag stating WHY rather than a pinned debt
  implying it needs fixing. Pinning a correct handler as debt is a lie in the
  other direction.

An empty pin is stronger than an exclusion: the scanner runs on every commit,
so the FIRST new violation fails the build instead of quietly joining a
census.

THE DEBT
--------
Per §11 every finding above is a dated, exact, shrink-only debt: a
``{path: count}`` pin per scanner, compared with the framework's own
``ratchet``. Exact counts mean a NEW violation fails, GROWTH in a pinned
file fails, and a STALE pin — the file got fixed and nobody deleted the
line — fails too. A boolean "cara is exempt" flag would catch only the
first and would never expire, which is why §11 forbids it.

The pins live in ``_cara_census.py``, not in the scanners'
``seam_allowlists``: the manifest below passes no allowlists at all, so each
scanner reports cara's RAW counts and the census is the whole truth rather
than what survived suppression. It also keeps the framework's own adoption
debt out of the product-facing manifest surface.

After a shrink, regenerate the literal and paste it into ``_cara_census.py``
(deliberately a paste, not an auto-write: a guard that rewrites its own pin
is not a guard):

    ./.venv/bin/python -c "import tests.architecture._cara_self_scan as s; \
        print(s.render_census())"
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest, ManifestRoots
from cara.architecture.scanners import REGISTRY

#: Repository root — the "deployable" for a cara-rooted scan. ``tests/`` is
#: two levels down from it, and resolving keeps the path stable no matter
#: which directory pytest was invoked from.
CARA_ROOT: Path = Path(__file__).resolve().parents[2]

#: The framework's importable package: this scan's ``app`` root. Everything
#: that used to be spelled ``"app"`` inside a scanner is read from here.
CARA_PACKAGE: Path = CARA_ROOT / "cara"

#: Scanner ids run against cara itself, and the reason each is excluded.
#: Together they must exhaust ``REGISTRY`` (see ``test_cara_self_scan``).
SELF_SCANNED: tuple[str, ...] = (
    "import_tiers",
    "inline_imports",
    "import_form",
    "source_shape",
    "barrel_completeness",
    "collaborator_calls",
    "barrel_mid_load",
    "silent_except_swallow",
)

#: Every remaining scanner, and the doctrine reason it cannot apply to a
#: framework. No entry here is a deferral — see the module docstring.
NOT_APPLICABLE: dict[str, str] = {
    "domain_registry": "§3 domains/flows — cara has no business domains",
    "domain_ownership": "§3 cross-domain service door — no domains to cross",
    "write_ownership": "§7 table ownership — cara owns no tables",
    "transaction_ownership": "§8 who opens a transaction — no product services",
    "raw_sql_home": "§5 SQL lives in repositories — cara ships the ORM",
    "model_query_discipline": "§5 model reach — cara defines Model itself",
    "vertical_slice_seams": "§4 plugin tokens — cara is brand-blind by design",
    "job_idempotency": "§6 per-job idempotency params — cara defines BaseJob",
    "kernel_membership": "§2 commons/ membership — cara has no kernel",
    "vendor_barrel_parity": "§2 vendored image parity — cara is never vendored",
    "flow_law": "§5 controller/job edges — cara has no edge layer",
    "port_membership": "§5 ports layer — cara has no ports layer",
    "http_in_business_logic": "§5 transport stops at the edge — cara IS cara.http",
    "env_read_discipline": "§5 env read in config/ only — cara has no config/",
}


def cara_manifest() -> Manifest:
    """The Guard Pack manifest for cara's own tree.

    ``layers`` is every top-level package of ``cara/`` — the framework's
    equivalent of a product's layer set, and the unit both ``ImportForm``
    arms reason about (a consumer imports ``cara.queues``; a sibling inside
    ``cara/queues/`` must not).

    ``kernel`` is deliberately EMPTY and ``kernel_dev_root_name`` stays
    ``commons``: the framework must never import a product's kernel, so the
    kernel arm of ``ImportForm`` is a live rule here that currently finds
    nothing — the good kind of zero, from a rule that would fire.

    No ``seam_allowlists`` and no ``inline_import_exemptions`` are passed;
    the census below holds the debt instead (see the module docstring).
    """
    layers = tuple(
        sorted(
            child.name
            for child in CARA_PACKAGE.iterdir()
            if child.is_dir()
            and child.name != "__pycache__"
            and (child / "__init__.py").is_file()
        )
    )
    scanner_roots = {scanner: (CARA_PACKAGE,) for scanner in SELF_SCANNED}
    return Manifest(
        product="cara",
        deployable="cara",
        roots=ManifestRoots(
            deployable=CARA_ROOT,
            app=CARA_PACKAGE,
            scanner_roots=scanner_roots,
            kernel={},
            # cara has no app-local tier: an import is either the framework
            # itself (absolute ``cara.*``, tier 2) or relative (tier 3).
            local_root_names=(),
        ),
        layers=layers,
        domain_layers=(),
        domains={},
        scan_plugin_string_literals=False,
        kernel_packages=frozenset(),
        kernel_barrel_packages=frozenset(),
        seam_kernel_packages=frozenset(),
    )


def scan(scanner_id: str) -> list[Finding]:
    """Run ONE scanner against cara, by its ``REGISTRY`` id."""
    return REGISTRY[scanner_id].scan(cara_manifest())


def counts(findings: list[Finding]) -> dict[str, int]:
    """Findings collapsed to ``{path: count}`` — the census identity.

    Per PATH rather than per line: a line number moves every time an unrelated
    edit lands above it, and a pin that churns on unrelated edits is a pin
    people learn to regenerate blindly. A count still fails on growth, on a
    new file, and on a stale pin.
    """
    tally: dict[str, int] = {}
    for finding in findings:
        tally[finding.path] = tally.get(finding.path, 0) + 1
    return tally


def render_census() -> str:
    """The ``CENSUS`` literal for this working tree, to paste into
    ``_cara_census.py`` (see the module docstring for when and why)."""
    lines = ["CENSUS: dict[str, dict[str, int]] = {"]
    for scanner_id in SELF_SCANNED:
        tally = counts(scan(scanner_id))
        if not tally:
            lines.append(f'    "{scanner_id}": {{}},')
            continue
        lines.append(f'    "{scanner_id}": {{')
        lines.extend(f'        "{path}": {n},' for path, n in sorted(tally.items()))
        lines.append("    },")
    lines.append("}")
    return "\n".join(lines)
