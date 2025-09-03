class GroupByExpression:
    """A helper class to manage select expressions."""

    def __init__(self, column=None, raw=False, bindings=()):
        self.column = column.strip()

        self.raw = raw
        self.bindings = bindings
