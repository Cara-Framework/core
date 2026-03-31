class HavingExpression:
    """A helper class to manage having expressions."""

    def __init__(self, column, equality=None, value=None, raw=False):
        self.column = column
        self.raw = raw

        if equality and not value:
            value = equality
            equality = "="

        self.equality = equality
        self.value = value
        self.value_type = "having"
