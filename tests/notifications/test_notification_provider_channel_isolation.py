"""Every channel-registration helper in ``NotificationProvider`` must
fail-soft: a broken / missing dependency in ONE channel must not
abort the others.

The bug shape pre-fix
~~~~~~~~~~~~~~~~~~~~~
``_add_mail_channel`` already wrapped its
``self.application.make("mail")`` call in a try/except + Log.warning
("Mail channel registration failed: …"). ``_add_slack_channel`` no-ops
gracefully on missing webhook config. ``_add_log_channel`` only reads
config values. But ``_add_database_channel`` called
``self.application.make("DB").query()`` BARE — when the DB binding
hadn't been registered (boot-time recovery, test harness with custom
provider order, ConfigurationProvider not yet loaded so the DB driver
defaults haven't been resolved), it raised
``MissingContainerBindingException`` and the unhandled exception
aborted the rest of ``register()``.

User-visible symptom
~~~~~~~~~~~~~~~~~~~~
The single missing dependency turned into a TOTAL notification
blackout: mail (registered before DB) survived, but slack + log
(registered after DB on lines 38-39 of the provider) never ran
because the raise short-circuited the method. The app booted clean
in every observability dashboard (no startup error), but every
later ``Notification::send`` for slack/log went into the void.

What this file pins
~~~~~~~~~~~~~~~~~~~
Static source-shape check: every ``_add_*_channel`` helper in the
provider must wrap its body in try/except. The audit doesn't try to
instantiate the provider (would need a full bootstrap with config +
DB + mail providers wired) — it parses the file and walks each
helper method's AST, checking that the outermost statement under
the function body is a Try node OR the function body contains at
least one statement-level Try that covers the application.make()
call.

If a future contributor adds e.g. ``_add_sms_channel`` they'll see
this test fail loudly the moment they forget the wrap.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


PROVIDER_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "cara"
    / "notifications"
    / "NotificationProvider.py"
)


def _provider_class() -> ast.ClassDef:
    tree = ast.parse(PROVIDER_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "NotificationProvider":
            return node
    raise AssertionError("NotificationProvider class not found in expected file")


def _add_channel_helpers() -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    cls = _provider_class()
    return [
        m
        for m in cls.body
        if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))
        and m.name.startswith("_add_")
        and m.name.endswith("_channel")
    ]


def _body_contains_try(body: list[ast.stmt]) -> bool:
    """True if ``body`` (a function body) contains a Try statement at
    any nesting level whose handlers include ``Exception`` (or bare
    except). We accept any handler that catches Exception or above —
    a helper catching a narrower class still violates the fail-soft
    contract."""
    for node in body:
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Try):
                continue
            for handler in sub.handlers:
                if handler.type is None:
                    return True
                # except Exception / except BaseException — accept.
                if isinstance(handler.type, ast.Name) and handler.type.id in {
                    "Exception",
                    "BaseException",
                }:
                    return True
                # except (Exception, ...): tuple form — accept if
                # Exception is one of the named types.
                if isinstance(handler.type, ast.Tuple):
                    for elt in handler.type.elts:
                        if isinstance(elt, ast.Name) and elt.id in {
                            "Exception",
                            "BaseException",
                        }:
                            return True
    return False


def test_provider_file_is_readable():
    """Smoke check — pin the discovery path."""
    assert PROVIDER_PATH.is_file(), (
        f"NotificationProvider.py missing at {PROVIDER_PATH}; did the "
        f"notifications/ layout change?"
    )


def test_provider_declares_helpers():
    """Pin that we found something to audit. An empty list would let
    the parametrized test below pass vacuously."""
    helpers = _add_channel_helpers()
    assert len(helpers) >= 3, (
        f"Expected at least 3 ``_add_*_channel`` helpers on "
        f"NotificationProvider, found {[h.name for h in helpers]}. "
        f"Has the channel-registration shape changed? Update this "
        f"test to match the new contract."
    )


@pytest.mark.parametrize(
    "helper",
    _add_channel_helpers(),
    ids=lambda h: h.name,
)
def test_every_channel_helper_is_fail_soft(
    helper: ast.FunctionDef | ast.AsyncFunctionDef,
):
    """The contract: every ``_add_*_channel`` MUST wrap its body in
    try/except Exception.

    Pre-fix ``_add_database_channel`` violated this — its
    ``self.application.make("DB").query()`` raised
    MissingContainerBindingException and the unhandled raise aborted
    the rest of ``register()``, silently dropping slack + log
    channels because they registered AFTER database in the provider's
    register() body.

    Fail-soft channels keep the rest of the notification subsystem
    online when one dependency is missing. The fix is to wrap the
    body in try/except + Log.warning, matching the canonical pattern
    already used by ``_add_mail_channel``.
    """
    assert _body_contains_try(helper.body), (
        f"{helper.name} does NOT wrap its body in try/except Exception. "
        f"A missing dependency here aborts the rest of "
        f"NotificationProvider.register(), causing siblings registered "
        f"after this helper to silently be skipped. Wrap the body like "
        f"_add_mail_channel does:\n\n"
        f"    try:\n"
        f"        ...existing body...\n"
        f"    except Exception as e:\n"
        f'        Log.warning(f"[NotificationProvider] X channel "\n'
        f'                    f"registration failed: {{e}}")\n'
    )
