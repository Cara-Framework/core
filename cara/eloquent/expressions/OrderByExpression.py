class OrderByExpression:
    """A helper class to manage select expressions."""

    def __init__(
        self,
        column,
        direction="ASC",
        raw=False,
        bindings=(),
    ):
        self.column = column.strip()

        self.raw = raw

        self.direction = direction
        self.bindings = bindings

        if raw is False:
            if self.column.endswith(" desc"):
                self.column = self.column.split(" desc")[0].strip()
                self.direction = "DESC"

            if self.column.endswith(" asc"):
                self.column = self.column.split(" asc")[0].strip()
                self.direction = "ASC"
