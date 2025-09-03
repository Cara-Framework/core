class ColumnFactory:
    """Single Responsibility: Creates different types of columns"""

    def __init__(self, table, default_string_length=255):
        self.table = table
        self.default_string_length = default_string_length

    def create_column(self, column_name, column_type, **kwargs):
        """Factory method for creating columns - Open/Closed principle"""
        return self.table.add_column(column_name, column_type, **kwargs)

    # === String/Text Columns ===

    def string(self, column, length=255, nullable=False):
        """Create string column"""
        return self.create_column(column, "string", length=length, nullable=nullable)

    def char(self, column, length=1, nullable=False):
        """Create char column"""
        return self.create_column(column, "char", length=length, nullable=nullable)

    def text(self, column, length=None, nullable=False):
        """Create text column"""
        return self.create_column(column, "text", length=length, nullable=nullable)

    def tiny_text(self, column, length=None, nullable=False):
        """Create tiny text column"""
        return self.create_column(column, "tiny_text", length=length, nullable=nullable)

    def long_text(self, column, length=None, nullable=False):
        """Create long text column"""
        return self.create_column(column, "long_text", length=length, nullable=nullable)

    # === Integer Columns ===

    def integer(self, column, length=11, nullable=False):
        """Create integer column"""
        return self.create_column(column, "integer", length=length, nullable=nullable)

    def tiny_integer(self, column, length=1, nullable=False):
        """Create tiny integer column"""
        return self.create_column(
            column, "tiny_integer", length=length, nullable=nullable
        )

    def small_integer(self, column, length=5, nullable=False):
        """Create small integer column"""
        return self.create_column(
            column, "small_integer", length=length, nullable=nullable
        )

    def medium_integer(self, column, length=7, nullable=False):
        """Create medium integer column"""
        return self.create_column(
            column, "medium_integer", length=length, nullable=nullable
        )

    def big_integer(self, column, length=32, nullable=False):
        """Create big integer column"""
        return self.create_column(column, "big_integer", length=length, nullable=nullable)

    def unsigned_integer(self, column, nullable=False):
        """Create unsigned integer column"""
        return self.create_column(column, "integer", nullable=nullable).unsigned()

    def unsigned_big_integer(self, column, length=32, nullable=False):
        """Create unsigned big integer column"""
        return self.create_column(
            column, "big_integer", length=length, nullable=nullable
        ).unsigned()

    # === Auto-increment Columns ===

    def increments(self, column, nullable=False):
        """Create auto-incrementing primary key"""
        return self.create_column(column, "increments", nullable=nullable, primary=True)

    def tiny_increments(self, column, nullable=False):
        """Create tiny auto-incrementing primary key"""
        return self.create_column(
            column, "tiny_increments", nullable=nullable, primary=True
        )

    def big_increments(self, column, nullable=False):
        """Create big auto-incrementing primary key"""
        return self.create_column(
            column, "big_increments", nullable=nullable, primary=True
        )

    def id(self, column="id"):
        """Create standard ID column"""
        return self.big_increments(column)

    # === Special Columns ===

    def uuid(self, column, nullable=False, length=36):
        """Create UUID column"""
        return self.create_column(column, "uuid", nullable=nullable, length=length)

    def boolean(self, column, nullable=False):
        """Create boolean column"""
        return self.create_column(column, "boolean", nullable=nullable)

    def binary(self, column, nullable=False):
        """Create binary column"""
        return self.create_column(column, "binary", nullable=nullable)

    # === Date/Time Columns ===

    def date(self, column, nullable=False):
        """Create date column"""
        return self.create_column(column, "date", nullable=nullable)

    def time(self, column, nullable=False):
        """Create time column"""
        return self.create_column(column, "time", nullable=nullable)

    def datetime(self, column, nullable=False, now=False):
        """Create datetime column"""
        col = self.create_column(column, "datetime", nullable=nullable)
        if now:
            col.use_current()
        return col

    def timestamp(self, column, nullable=False, now=False):
        """Create timestamp column"""
        col = self.create_column(column, "timestamp", nullable=nullable)
        if now:
            col.use_current()
        return col

    def timestamps(self):
        """Create created_at and updated_at columns"""
        self.timestamp("created_at", nullable=True, now=True)
        return self.timestamp("updated_at", nullable=True, now=True)

    # === Numeric Columns ===

    def decimal(self, column, length=17, precision=6, nullable=False):
        """Create decimal column"""
        return self.create_column(
            column, "decimal", length=f"{length}, {precision}", nullable=nullable
        )

    def float(self, column, length=19, precision=4, nullable=False):
        """Create float column"""
        return self.create_column(
            column, "float", length=f"{length}, {precision}", nullable=nullable
        )

    def double(self, column, nullable=False):
        """Create double column"""
        return self.create_column(column, "double", nullable=nullable)

    def unsigned_decimal(self, column, length=17, precision=6, nullable=False):
        """Create unsigned decimal column"""
        return self.create_column(
            column, "decimal", length=f"{length}, {precision}", nullable=nullable
        ).unsigned()

    # === JSON/Special Columns ===

    def json(self, column, nullable=False):
        """Create JSON column"""
        return self.create_column(column, "json", nullable=nullable)

    def jsonb(self, column, nullable=False):
        """Create JSONB column"""
        return self.create_column(column, "jsonb", nullable=nullable)

    def enum(self, column, options=None, nullable=False):
        """Create enum column"""
        options = options or []
        return self.create_column(
            column, "enum", length="255", values=options, nullable=nullable
        )

    # === Network/Geo Columns ===

    def inet(self, column, length=255, nullable=False):
        """Create inet column"""
        return self.create_column(column, "inet", length=255, nullable=nullable)

    def cidr(self, column, length=255, nullable=False):
        """Create cidr column"""
        return self.create_column(column, "cidr", length=255, nullable=nullable)

    def macaddr(self, column, length=255, nullable=False):
        """Create macaddr column"""
        return self.create_column(column, "macaddr", length=255, nullable=nullable)

    def point(self, column, nullable=False):
        """Create point column"""
        return self.create_column(column, "point", nullable=nullable)

    def geometry(self, column, nullable=False):
        """Create geometry column"""
        return self.create_column(column, "geometry", nullable=nullable)

    def year(self, column, length=4, default=None, nullable=False):
        """Create year column"""
        return self.create_column(
            column, "year", length=length, nullable=nullable, default=default
        )

    # === Polymorphic Columns ===

    def morphs(self, column, nullable=False, indexes=True):
        """Create polymorphic relationship columns"""
        columns = []
        columns.append(
            self.create_column(f"{column}_id", "integer", nullable=nullable).unsigned()
        )
        columns.append(
            self.create_column(
                f"{column}_type",
                "string",
                nullable=nullable,
                length=self.default_string_length,
            )
        )
        return columns
