"""Authorization Gate + Policy behavioural contract.

Covers ability/policy resolution, the root-bypass / blanket-allow patterns the
app relies on, rich response messages, the before/after hooks, fail-closed
evaluation, fail-loud registration, and the policy-instance cache.
"""

from __future__ import annotations

import pytest

from cara.authorization import AuthorizationResponse, Gate, Policy
from cara.exceptions import AuthorizationFailedException

# -- fixtures ------------------------------------------------------------- #


class User:
    def __init__(self, uid, is_admin=False, is_root=False):
        self.id = uid
        self.is_admin = is_admin
        self.is_root = is_root


class Product:
    pass


_INSTANTIATIONS = {"n": 0}


class ProductPolicy(Policy):
    def __init__(self):
        super().__init__()
        _INSTANTIATIONS["n"] += 1

    def update(self, user, model=None):
        return bool(getattr(user, "is_admin", False))

    def delete(self, user, model=None):
        if getattr(user, "is_admin", False):
            return True
        return AuthorizationResponse(False, "Only admins may delete products.")

    def explode(self, user, model=None):
        raise RuntimeError("boom")


@pytest.fixture
def admin():
    return User(1, is_admin=True)


@pytest.fixture
def plain():
    return User(2)


@pytest.fixture
def root():
    return User(3, is_root=True)


@pytest.fixture
def product():
    return Product()


@pytest.fixture
def gate():
    """A gate mirroring the app's GuardProvider wiring."""
    g = Gate(user_resolver=lambda: None)
    g.define("admin", lambda user, *_a: bool(user and getattr(user, "is_admin", False)))
    g.before(
        lambda user, _a, *_args: (
            True if user and getattr(user, "is_root", False) else None
        )
    )
    g.register_policies([(Product, ProductPolicy)])
    return g


# -- abilities ------------------------------------------------------------ #


def test_gate_satisfies_contract(gate):
    assert isinstance(gate, Gate)


def test_guest_is_denied(gate):
    assert gate.for_user(None).allows("admin") is False
    assert gate.for_user(None).inspect("admin").message() == "Unauthenticated."


def test_admin_ability(gate, admin, plain):
    assert gate.for_user(admin).allows("admin") is True
    assert gate.for_user(plain).allows("admin") is False


def test_root_bypasses_every_check(gate, root, product):
    assert gate.for_user(root).allows("literally-anything") is True
    assert gate.for_user(root).allows("update", product) is True


# -- policies ------------------------------------------------------------- #


def test_model_policy_resolution(gate, admin, plain, product):
    assert gate.for_user(admin).allows("update", product) is True
    assert gate.for_user(plain).allows("update", product) is False
    assert gate.for_user(plain).denies("update", product) is True


def test_any_and_none(gate, admin, plain):
    assert gate.for_user(admin).any(["nope", "admin"]) is True
    assert gate.for_user(plain).any(["nope", "admin"]) is False
    assert gate.for_user(plain).none(["admin", "update"]) is True
    assert gate.for_user(admin).none(["admin"]) is False


def test_unknown_ability_is_denied_with_message():
    response = Gate().for_user(User(9)).inspect("mystery")
    assert response.denied()
    assert "mystery" in response.message()


# -- authorize / inspect / rich messages ---------------------------------- #


def test_authorize_raises_on_denial(gate, plain):
    with pytest.raises(AuthorizationFailedException) as excinfo:
        gate.for_user(plain).authorize("admin")
    assert excinfo.value.ability == "admin"


def test_authorize_returns_response_on_allow(gate, admin):
    assert gate.for_user(admin).authorize("admin").allowed() is True


def test_policy_response_message_surfaces(gate, plain, product):
    inspected = gate.for_user(plain).inspect("delete", product)
    assert inspected.denied()
    assert inspected.message() == "Only admins may delete products."

    with pytest.raises(AuthorizationFailedException) as excinfo:
        gate.for_user(plain).authorize("delete", product)
    assert excinfo.value.message == "Only admins may delete products."


# -- hooks ---------------------------------------------------------------- #


def test_policy_before_hook_short_circuits(plain, product):
    class BeforePolicy(Policy):
        def before(self, user, ability, *args):
            return True

        def update(self, user, model=None):
            return False

    g = Gate()
    g.policy(Product, BeforePolicy)
    assert g.for_user(plain).allows("update", product) is True


def test_policy_after_hook_overrides(admin, product):
    class AfterPolicy(Policy):
        def after(self, user, ability, result, *args):
            return False

        def update(self, user, model=None):
            return True

    g = Gate()
    g.policy(Product, AfterPolicy)
    assert g.for_user(admin).allows("update", product) is False


def test_gate_after_callback_overrides(gate, admin):
    gate.after(lambda user, ability, result, *a: False)
    assert gate.for_user(admin).allows("admin") is False


# -- failure modes -------------------------------------------------------- #


@pytest.mark.parametrize("bad", [[(Product,)], [42], [("a", "b", "c")]])
def test_register_policies_fails_loud(bad):
    with pytest.raises(TypeError):
        Gate().register_policies(bad)


def test_policy_with_bad_model_fails_loud():
    with pytest.raises(TypeError):
        Gate().policy(None, ProductPolicy)


def test_raising_policy_fails_closed(admin, product):
    g = Gate()
    g.policy(Product, ProductPolicy)
    assert g.for_user(admin).allows("explode", product) is False


# -- performance contract ------------------------------------------------- #


def test_policy_instance_is_cached(admin, product):
    _INSTANTIATIONS["n"] = 0
    g = Gate()
    g.policy(Product, ProductPolicy)
    for _ in range(5):
        g.for_user(admin).allows("update", product)
    assert _INSTANTIATIONS["n"] == 1


def test_for_user_shares_registries_but_isolates_user(gate, admin, plain):
    v1 = gate.for_user(admin)
    v2 = gate.for_user(plain)
    assert v1._policies is v2._policies is gate._policies
    assert v1._current_user is admin
    assert v2._current_user is plain
