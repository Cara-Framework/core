"""Password strength validation — generic policy primitives.

Like Laravel's ``Password::min(8)->mixedCase()->numbers()`` validators,
these helpers encode the universal "is this password strong enough"
rules so every cara app's auth flows (register, change, reset) agree on
a single policy without re-rolling their own checker.

Currently policy is fixed (5 distinct chars, mixed case, ≥1 digit,
common-prefix blacklist). A fluent builder analogous to Laravel's
``Password`` rule is intentionally not shipped yet — single helper
covers every existing call site cleanly; we'll grow the surface only
when an app actually needs configurable rules.
"""


# Common-password prefixes — lowercased. Match by prefix rather than
# exact equality so trivial decorations ("password123", "qwerty!")
# still get caught. Sourced from the SecLists "10k-most-common.txt"
# top-N + obvious culture-specific ones (sports/animals/games seen on
# breach lists). Keep this list short — the goal is to catch the
# laziest passwords, not to be a comprehensive denylist (entropy
# checks already gate the rest).
_COMMON_PASSWORD_PREFIXES = (
    "password", "qwerty", "123456", "111111", "abc123", "letmein", "admin",
    "welcome", "monkey", "dragon", "iloveyou", "sunshine", "princess",
    "football", "baseball", "master", "trustno1", "1q2w3e4r", "changeme",
)


def check_password_strength(password: str) -> str | None:
    """Return an error message if ``password`` is too weak, ``None`` otherwise.

    Unified policy used across every auth flow (register / reset /
    change). Centralising it here means a future tightening (e.g.
    minimum length bump from 5 to 8 distinct chars) propagates through
    every flow without per-flow edits.

    Rules:

    1. ≥ 5 distinct characters — defeats single-char repetition
       (``aaaaaa``, ``111111``).
    2. ≥ 1 uppercase letter — entropy floor.
    3. ≥ 1 lowercase letter — entropy floor.
    4. ≥ 1 digit — entropy floor.
    5. Doesn't start with a common breach-list prefix (case-insensitive).

    Args:
        password: Raw password string (whatever the user typed).
            Whitespace is significant — callers should NOT pre-strip.

    Returns:
        ``None`` if the password meets every rule. Otherwise a single
        end-user-friendly error message describing the FIRST failed
        rule. The string is meant to be surfaced to the user as-is —
        format with capitalisation + period to match.

    Examples:
        >>> check_password_strength("Password1!") is None  # passes? No — common prefix
        False
        >>> check_password_strength("Aa1bcde") is None
        True
    """
    if len(set(password)) < 5:
        return "Password must contain at least 5 distinct characters."
    if not any(c.isupper() for c in password):
        return "Password must contain at least one uppercase letter."
    if not any(c.islower() for c in password):
        return "Password must contain at least one lowercase letter."
    if not any(c.isdigit() for c in password):
        return "Password must contain at least one digit."

    lowered = password.lower()
    for prefix in _COMMON_PASSWORD_PREFIXES:
        if lowered.startswith(prefix):
            return "Password is too common. Choose a less predictable one."
    return None


__all__ = ["check_password_strength"]
