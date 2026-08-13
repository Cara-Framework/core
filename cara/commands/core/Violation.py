"""Violation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Violation:
    """One convention breach, with the file it lives in and its one-line remedy.

    ``blocks_fix`` marks a violation that ``--fix`` must not run THROUGH:
    regenerating would erase the evidence (a hand-added index) rather than
    repair it. ``human_only`` marks one regeneration simply cannot address.
    """

    rule: str
    path: str
    message: str
    remedy: str
    human_only: bool = False
    blocks_fix: bool = False
