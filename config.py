import os

# Configuración de la base de datos
SQLALCHEMY_DATABASE_URI = 'sqlite:///migration.db'  # Utiliza SQLite (se crea un archivo migration.db)
SQLALCHEMY_TRACK_MODIFICATIONS = False  # Desactivamos la monitorización para evitar alertas innecesarias


# Configuración de logging
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO')
LOGGING_FILE = os.getenv('LOGGING_FILE', 'migration.log')

# Carpeta de carga de archivos CSV
UPLOAD_FOLDER = 'uploads/'
