class AggregateExpression:
    def __init__(self, aggregate=None, column=None, alias=False):
        self.aggregate = aggregate
        self.column = column.strip()
        self.alias = alias
        if " as " in self.column:
            self.column, self.alias = self.column.split(" as ")
