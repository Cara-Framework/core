class ConnectionFactory:
    """
    Single Responsibility: Creates database connections ONLY
    Open/Closed: Can be extended with new connection types
    No configuration reading - gets config from outside
    """

    _connections = {}

    def __init__(self):
        """Initialize connection factory"""
        pass

    @classmethod
    def register(cls, key, connection):
        """
        Registers new connections.

        Arguments:
            key {key} -- The key or driver name you want assigned to this connection
            connection {eloquent.connections.BaseConnection} -- An instance of a BaseConnection class.

        Returns:
            cls
        """
        cls._connections.update({key: connection})
        return cls

    def make(self, driver_name):
        """
        Makes connection class by driver name.

        Arguments:
            driver_name {string} -- The driver name (sqlite, mysql, postgres, etc.)

        Raises:
            Exception: Raises exception if driver not found

        Returns:
            cara.eloquent.connection.BaseConnection -- Returns connection class.
        """
        if driver_name in self._connections:
            return self._connections[driver_name]

        raise Exception(
            f"The '{driver_name}' connection driver does not exist. Available drivers: {list(self._connections.keys())}"
        )
