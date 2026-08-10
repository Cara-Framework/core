"""SilentExceptSwallow: a broad ``except`` never disappears quietly.

``except Exception: pass`` converts every unforeseen failure into a
successful-looking no-op. The next incident has no log line, no metric and no
stack — the failure mode this rule exists to prevent is *silence*, not
catching.

A handler is a finding when ALL of these hold:

* it is bare (``except:``) or BROAD — catching ``Exception`` /
  ``BaseException``, alone or inside a tuple of types;
* nothing in the handler logs, re-raises or captures the error;
* the body only passes, ellipses, states a constant, or exits the block
  (``continue`` / ``break`` / bare ``return``).

A NARROW handler (``except (ValueError, TypeError): return None``) is a typed
fallback, not a swallow, and is left alone.

The escape hatch is a comment tag (``manifest.silent_except_allow_tag``, by
default ``# allow-silent-except``) anywhere in the handler's line span — for
the collect-and-log-later loop, where the log happens after the loop rather
than never.
"""

from __future__ import annotations

import ast

from cara.architecture._ast_utils import iter_modules, read_source
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

BROAD_EXCEPTIONS = frozenset({"Exception", "BaseException"})
#: Method/function names that prove the handler reported the failure.
REPORTING_CALLS = frozenset(
    {
        "error",
        "warning",
        "exception",
        "critical",
        "info",
        "debug",
        "log",
        "capture_exception",
    }
)


def _is_broad(handler: ast.ExceptHandler) -> bool:
    caught = handler.type
    if isinstance(caught, ast.Name):
        return caught.id in BROAD_EXCEPTIONS
    if isinstance(caught, ast.Tuple):
        return any(
            isinstance(element, ast.Name) and element.id in BROAD_EXCEPTIONS
            for element in caught.elts
        )
    return False


def _reports(handler: ast.ExceptHandler) -> bool:
    for node in ast.walk(handler):
        if isinstance(node, ast.Raise):
            return True
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            )
            if name in REPORTING_CALLS:
                return True
    return False


def _body_is_silent(handler: ast.ExceptHandler) -> bool:
    return all(
        isinstance(statement, (ast.Pass, ast.Continue, ast.Break))
        or (isinstance(statement, ast.Return) and statement.value is None)
        or (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant))
        for statement in handler.body
    )


class SilentExceptSwallow:
    """No broad ``except`` swallows a failure without a trace (DOCTRINE §5)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        tag = manifest.silent_except_allow_tag
        findings: list[Finding] = []
        for path, rel, tree in iter_modules(
            manifest.roots.scan_dirs("silent_except_swallow"), manifest.roots.deployable
        ):
            # ``iter_modules`` already proved the file parses, so this cannot
            # normally fail; going through ``read_source`` keeps a
            # mid-change tree (a file deleted between glob and read) a
            # skipped file rather than a crashed pack.
            lines = (read_source(path) or "").splitlines()
            for node in ast.walk(tree):
                if not isinstance(node, ast.ExceptHandler):
                    continue
                span = lines[node.lineno - 1 : (node.end_lineno or node.lineno)]
                if any(tag in line for line in span):
                    continue
                if node.type is None:
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            "bare `except:` — name the exceptions you can handle",
                        )
                    )
                    continue
                if not _is_broad(node) or _reports(node) or not _body_is_silent(node):
                    continue
                findings.append(
                    Finding(
                        rel,
                        node.lineno,
                        f"broad `except` swallows the failure with no log or re-raise "
                        f"— log it, re-raise, or annotate `# {tag}: <why>`",
                    )
                )
        return findings
