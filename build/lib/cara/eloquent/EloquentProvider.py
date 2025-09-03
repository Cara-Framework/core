from cara.configuration import config
from cara.eloquent.DatabaseManager import DatabaseManager
from cara.foundation import Provider


class EloquentProvider(Provider):
    """Eloquent ORM Provider - Database Manager'ı uygulamaya bind eder"""

    def __init__(self, application):
        self.application = application

    def register(self):
        """Database Manager'ı kayıt eder ve yapılandırır"""
        # Konfigürasyondan database ayarlarını al
        default_connection = config("database.default", "app")
        connection_details = config("database.drivers")

        # DatabaseManager'ı oluştur ve yapılandır
        database_manager = DatabaseManager().set_database_config(
            default_connection=default_connection, connection_details=connection_details
        )

        # Global instance olarak ayarla
        DatabaseManager.set_instance(database_manager)

        # Uygulamaya DB olarak bind et (backward compatibility)
        self.application.bind("DB", database_manager)

    def boot(self):
        """Provider boot işlemleri (opsiyonel)"""
        pass
