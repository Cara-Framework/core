"""CollaboratorCalls: a call into an injected collaborator matches its shape."""

from __future__ import annotations

from dataclasses import replace

from cara.architecture.scanners.CollaboratorCalls import CollaboratorCalls

from ._fixtures import make_manifest, write

# ── real defects ────────────────────────────────────────────────────────


def test_missing_method_is_a_finding(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1)
""",
    )
    findings = CollaboratorCalls.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert findings[0].path == "app/controllers/FooController.py"
    assert "has no such method" in findings[0].message
    assert "self.foo_service.missing_method" in findings[0].message


def test_too_many_positional_args_is_a_finding(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.bar(1, 2)
""",
    )
    findings = CollaboratorCalls.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert "accepts 1 positional argument(s) but 2 were passed" in findings[0].message


def test_missing_required_kwonly_is_a_finding(tmp_path):
    write(
        tmp_path / "app/repositories/FooRepository.py",
        """
class FooRepository:
    def bar(self, x, *, channel_ids):
        return x
""",
    )
    write(
        tmp_path / "app/services/FooService.py",
        """
from app.repositories.FooRepository import FooRepository


class FooService:
    def __init__(self, foo_repo: FooRepository) -> None:
        self.foo_repo = foo_repo

    def handle(self, x):
        return self.foo_repo.bar(x)
""",
    )
    findings = CollaboratorCalls.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert (
        "requires keyword-only 'channel_ids' but the call omits it" in findings[0].message
    )


def test_missing_required_positional_or_keyword_is_a_finding(tmp_path):
    """The `team_members_page(team)` vs `(tenant_id, team_id)` shape — the
    caller supplies fewer positional-or-keyword arguments than the target
    requires, and none of the missing names are covered by keyword either."""
    write(
        tmp_path / "app/repositories/FooRepository.py",
        """
class FooRepository:
    def bar(self, tenant_id, team_id):
        return tenant_id
""",
    )
    write(
        tmp_path / "app/services/FooService.py",
        """
from app.repositories.FooRepository import FooRepository


class FooService:
    def __init__(self, foo_repo: FooRepository) -> None:
        self.foo_repo = foo_repo

    def handle(self, team):
        return self.foo_repo.bar(team)
""",
    )
    findings = CollaboratorCalls.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert "requires 'team_id'" in findings[0].message


def test_thread_offload_forwarder_shape_is_checked(tmp_path):
    """`ExecutionContext.run_in_thread(self.attr.method, *args, **kwargs)` —
    the codebase's dominant async-controller-to-sync-service shape — must be
    checked exactly like a direct call."""
    write(
        tmp_path / "app/repositories/FooRepository.py",
        """
class FooRepository:
    def bar(self, x, *, channel_ids):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from cara.context import ExecutionContext

from app.repositories.FooRepository import FooRepository


class FooController:
    def __init__(self, foo_repo: FooRepository) -> None:
        self.foo_repo = foo_repo

    async def handle(self):
        return await ExecutionContext.run_in_thread(self.foo_repo.bar, 1)
""",
    )
    findings = CollaboratorCalls.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert "requires keyword-only 'channel_ids'" in findings[0].message


def test_mixin_call_site_is_checked_against_the_composing_class(tmp_path):
    """A private edge mixin has no `__init__` of its own — its `self.attr`
    calls must still be validated against the concrete controller's
    constructor-declared attribute map (the `_FooControllerMixin` shape)."""
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/_FooControllerEdgeMixin.py",
        """
class _FooControllerEdgeMixin:
    def edge_handler(self):
        return self.foo_service.missing_method(1)
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.controllers._FooControllerEdgeMixin import _FooControllerEdgeMixin
from app.services.FooService import FooService


class FooController(_FooControllerEdgeMixin):
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service
""",
    )
    findings = CollaboratorCalls.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert findings[0].path == "app/controllers/_FooControllerEdgeMixin.py"
    assert "has no such method" in findings[0].message


def test_matching_call_is_clean(tmp_path):
    write(
        tmp_path / "app/repositories/FooRepository.py",
        """
class FooRepository:
    def bar(self, x, *, channel_ids=None):
        return x
""",
    )
    write(
        tmp_path / "app/services/FooService.py",
        """
from app.repositories.FooRepository import FooRepository


class FooService:
    def __init__(self, foo_repo: FooRepository) -> None:
        self.foo_repo = foo_repo

    def handle(self, x):
        return self.foo_repo.bar(x, channel_ids=None)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


# ── SKIP: zero false positives ──────────────────────────────────────────


def test_unannotated_attribute_is_skipped(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
class FooController:
    def __init__(self, foo_service) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_target_class_with_unresolvable_base_is_skipped(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
from some.external import ExternalBase


class FooService(ExternalBase):
    pass
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_decorated_target_method_is_skipped(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    @property
    def bar(self):
        return 1
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.bar(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_type_not_defined_in_scanned_trees_is_skipped(tmp_path):
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from cara.http import Controller


class FooController:
    def __init__(self, controller: Controller) -> None:
        self.controller = controller

    def handle(self):
        return self.controller.nonexistent_method(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_union_annotation_is_skipped(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService | None) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_protocol_annotation_is_skipped(tmp_path):
    write(
        tmp_path / "app/ports/FooContract.py",
        """
from typing import Protocol


class FooContract(Protocol):
    def bar(self, x: int) -> int: ...
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.ports.FooContract import FooContract


class FooController:
    def __init__(self, foo_service: FooContract) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_reassigned_attribute_is_skipped(tmp_path):
    """A later `self.foo_service = ...` rebind (a lazy-load or reset
    pattern) makes the constructor annotation untrustworthy for the rest of
    the class's lifetime."""
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def reset(self, other) -> None:
        self.foo_service = other

    def handle(self):
        return self.foo_service.missing_method(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_star_args_unpacking_at_call_site_is_skipped(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self, args, kwargs):
        return self.foo_service.bar(*args, **kwargs)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


def test_ambiguous_collaborator_type_name_is_skipped(tmp_path):
    write(
        tmp_path / "app/services/one/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/services/two/FooService.py",
        """
class FooService:
    def bar(self, x, y):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.one.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1, 2, 3)
""",
    )
    assert CollaboratorCalls.scan(make_manifest(tmp_path)) == []


# ── exemptions: shrink-only sunset debt ─────────────────────────────────


def test_exemption_pin_suppresses_the_finding(tmp_path):
    write(
        tmp_path / "app/services/FooService.py",
        """
class FooService:
    def bar(self, x):
        return x
""",
    )
    write(
        tmp_path / "app/controllers/FooController.py",
        """
from app.services.FooService import FooService


class FooController:
    def __init__(self, foo_service: FooService) -> None:
        self.foo_service = foo_service

    def handle(self):
        return self.foo_service.missing_method(1)
""",
    )
    manifest = replace(
        make_manifest(tmp_path),
        collaborator_call_exemptions=frozenset(
            {"app/controllers/FooController.py:10:self.foo_service.missing_method"}
        ),
    )
    assert CollaboratorCalls.scan(manifest) == []


def test_stale_exemption_pin_is_a_finding(tmp_path):
    manifest = replace(
        make_manifest(tmp_path),
        collaborator_call_exemptions=frozenset(
            {"app/controllers/Ghost.py:1:self.ghost.vanished"}
        ),
    )
    findings = CollaboratorCalls.scan(manifest)
    assert len(findings) == 1
    assert "no matching finding remains" in findings[0].message
