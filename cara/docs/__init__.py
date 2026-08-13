"""cara.docs — the documentation engine products configure rather than copy.

Three passes over a checkout, all pure filesystem (AST, regex and git; no DB,
no broker, no app boot):

1. REFERENCE GENERATION — the route/queue/model/job/command/permission/env and
   package inventories, extracted straight from code. These pages are never
   hand-written, so they cannot go stale.
2. FRESHNESS — every hand-written page declares its ``sources:`` and the date a
   human last read it against them; sources newer than that date are STALE.
3. CLAIM VERIFICATION — the paths, pointers, commands and ports a page asserts,
   resolved against this checkout, plus every line that PRESCRIBES a practice
   the atlas forbids.

Product knowledge enters as a :class:`~cara.docs.DocsManifest.DocsManifest` and
nowhere else. This package never imports a product, never names one, and never
infers which product it is walking from what happens to be on disk.

``Claims``, ``ClaimSources``, ``Freshness``, ``Inventory``, ``Manifest`` and
``Support`` are regular submodules — import them directly. They are
deliberately not re-exported here: a name that is both an ``__all__`` entry and
a submodule is a shadowing footgun.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BAN_DECL_RE": (".Claims", "BAN_DECL_RE"),
    "CODE_EXT": (".ClaimSources", "CODE_EXT"),
    "CRAFT_RE": (".Claims", "CRAFT_RE"),
    "DocsManifest": (".DocsManifest", "DocsManifest"),
    "FM_RE": (".Freshness", "FM_RE"),
    "GENERATORS": (".Inventory", "GENERATORS"),
    "IGNORE_FILE": (".Claims", "IGNORE_FILE"),
    "IGNORE_LINE": (".Claims", "IGNORE_LINE"),
    "INLINE_CODE": (".Claims", "INLINE_CODE"),
    "ModelScope": (".ModelScope", "ModelScope"),
    "NEGATION_RE": (".Claims", "NEGATION_RE"),
    "PORT_RE": (".Claims", "PORT_RE"),
    "PRUNE": (".ClaimSources", "PRUNE"),
    "ROUTE_MODULES": (".Inventory", "ROUTE_MODULES"),
    "Say": (".Support", "Say"),
    "TILDE_RE": (".Claims", "TILDE_RE"),
    "VERBS": (".Inventory", "VERBS"),
    "atlas_bans": (".Claims", "atlas_bans"),
    "check_forbidden": (".Claims", "check_forbidden"),
    "check_path_claim": (".ClaimSources", "check_path_claim"),
    "claims_report": (".Claims", "claims_report"),
    "collect_routes": (".Inventory", "collect_routes"),
    "command_names": (".Claims", "command_names"),
    "command_rows": (".Inventory", "command_rows"),
    "declared_ports": (".ClaimSources", "declared_ports"),
    "dirty_paths": (".Support", "dirty_paths"),
    "doc_title": (".Freshness", "doc_title"),
    "forget_path_index": (".ClaimSources", "forget_path_index"),
    "freshness": (".Freshness", "freshness"),
    "front_matter": (".Freshness", "front_matter"),
    "gen_commands": (".Inventory", "gen_commands"),
    "gen_connectors": (".Inventory", "gen_connectors"),
    "gen_env": (".Inventory", "gen_env"),
    "gen_jobs": (".Inventory", "gen_jobs"),
    "gen_models": (".Inventory", "gen_models"),
    "gen_nav": (".Freshness", "gen_nav"),
    "gen_permissions": (".Inventory", "gen_permissions"),
    "gen_queues": (".Inventory", "gen_queues"),
    "gen_routes": (".Inventory", "gen_routes"),
    "generate_reference": (".Inventory", "generate_reference"),
    "git_root": (".Support", "git_root"),
    "header": (".Support", "header"),
    "invocation_re": (".Claims", "invocation_re"),
    "latest_committed_change": (".Support", "latest_committed_change"),
    "md_escape": (".Support", "md_escape"),
    "newest_change": (".Support", "newest_change"),
    "owned_markdowns": (".Claims", "owned_markdowns"),
    "parse_routes": (".Inventory", "parse_routes"),
    "path_index": (".ClaimSources", "path_index"),
    "read": (".Support", "read"),
    "scan_classes": (".Inventory", "scan_classes"),
    "sibling_roots": (".ClaimSources", "sibling_roots"),
    "strip_fences": (".ClaimSources", "strip_fences"),
    "verified_ts": (".Support", "verified_ts"),
    "verify_claims": (".Claims", "verify_claims"),
    "write_if_changed": (".Support", "write_if_changed"),
}

__all__ = [
    "BAN_DECL_RE",
    "CODE_EXT",
    "CRAFT_RE",
    "DocsManifest",
    "FM_RE",
    "GENERATORS",
    "IGNORE_FILE",
    "IGNORE_LINE",
    "INLINE_CODE",
    "ModelScope",
    "NEGATION_RE",
    "PORT_RE",
    "PRUNE",
    "ROUTE_MODULES",
    "Say",
    "TILDE_RE",
    "VERBS",
    "atlas_bans",
    "check_forbidden",
    "check_path_claim",
    "claims_report",
    "collect_routes",
    "command_names",
    "command_rows",
    "declared_ports",
    "dirty_paths",
    "doc_title",
    "forget_path_index",
    "freshness",
    "front_matter",
    "gen_commands",
    "gen_connectors",
    "gen_env",
    "gen_jobs",
    "gen_models",
    "gen_nav",
    "gen_permissions",
    "gen_queues",
    "gen_routes",
    "generate_reference",
    "git_root",
    "header",
    "invocation_re",
    "latest_committed_change",
    "md_escape",
    "newest_change",
    "owned_markdowns",
    "parse_routes",
    "path_index",
    "read",
    "scan_classes",
    "sibling_roots",
    "strip_fences",
    "verified_ts",
    "verify_claims",
    "write_if_changed",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
