from __future__ import annotations

class SubGroupExpression:
    """A helper class to manage subgroup expressions."""

    def __init__(self, builder, alias="group"):
        self.builder = builder
        self.alias = alias
