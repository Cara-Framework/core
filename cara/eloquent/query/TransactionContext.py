"""Context manager for QueryBuilder transactions."""

from __future__ import annotations


class TransactionContext:
    """Commit successful query work and roll failures back."""

    def __init__(self, builder):
        self.builder = builder

    def __enter__(self):
        self.builder.begin()
        return self.builder

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self.builder.rollback()
            except Exception as rollback_exc:
                raise rollback_exc from exc_val
            return False
        self.builder.commit()
        return True
